import logging
from datetime import date, timedelta
from sqlalchemy import and_, or_, select
from src.config.database import Session
from src.constants.enums import Status
from src.domain.analysis import analyze
from src.domain.selection import StaleAttempt
from src.domain.spec import SPEC
from src.models.jobs import Delivery, Run, RunCheckpoint, Schedule
from src.models.source import Dataset
from src.services.audit import record
from src.utils.exports import write_report
from src.utils.time import now

log = logging.getLogger(__name__)
LEASE = timedelta(minutes=3)


def materialize_due():
    if not SPEC.get('scheduled'):
        return
    stamp = now()
    with Session.begin() as db:
        schedules = db.scalars(select(Schedule).where(Schedule.enabled.is_(True), Schedule.next_at <= stamp)
                                .order_by(Schedule.next_at).limit(20).with_for_update(skip_locked=True))
        for item in schedules:
            due = item.next_at
            dataset_id = item.dataset_id
            params = dict(item.params)
            if item.repeat_daily:
                elapsed_days = (stamp - due).days
                due += timedelta(days=elapsed_days)
                period_key = SPEC['schedule_date_param']
                report_day = date.fromisoformat(params[period_key]) + timedelta(days=elapsed_days)
                params[period_key] = report_day.isoformat()
                item.params = {**params, period_key: (report_day + timedelta(days=1)).isoformat()}
                dataset_id = db.scalar(select(Dataset.id).order_by(Dataset.id.desc()).limit(1)) or dataset_id
                item.next_at = due + timedelta(days=1)
            else:
                item.enabled = False
            db.add(Run(dataset_id=dataset_id, schedule_id=item.id, scheduled_at=due,
                       name=item.name, params=params, author=item.author))


def claim():
    stamp = now()
    with Session.begin() as db:
        query = select(Run).where(or_(Run.status == Status.QUEUED,
                                      and_(Run.status == Status.RUNNING, Run.lease_until < stamp)))
        job = db.scalar(query.order_by(Run.id).limit(1).with_for_update(skip_locked=True))
        if job is None:
            return None
        if job.attempt >= 3:
            job.status = Status.FAILED
            job.error = '后台进程多次中断，请发起新的运行'
            job.finished_at = stamp
            record(db, 'worker', 'analysis.failed', job.id, reason=job.error)
            return None
        job.status = Status.RUNNING
        job.attempt += 1
        job.lease_until = stamp + LEASE
        records = db.get(Dataset, job.dataset_id).rows
        # 检查点按运行保存，续跑的上一次尝试也能接着算；payload 指纹再校验配对与限额。
        saved = db.get(RunCheckpoint, job.id)
        checkpoint = dict(saved.payload) if saved else None
        return job.id, job.attempt, records, job.params, checkpoint


def _owned(db, job_id, attempt):
    job = db.get(Run, job_id, with_for_update=True)
    if job is None or job.attempt != attempt or job.status != Status.RUNNING:
        raise StaleAttempt
    return job


def persist(job_id, attempt, checkpoint):
    """续跑检查点只在本次尝试仍然持有时落库，旧尝试晚到直接丢弃。"""
    with Session.begin() as db:
        try:
            _owned(db, job_id, attempt)
        except StaleAttempt:
            return False
        saved = db.get(RunCheckpoint, job_id, with_for_update=True)
        if saved is None:
            db.add(RunCheckpoint(run_id=job_id, attempt=attempt, payload=checkpoint))
        else:
            saved.attempt, saved.payload, saved.updated_at = attempt, checkpoint, now()
        return True


def execute(lease):
    job_id, attempt, rows, params, checkpoint = lease
    result = export = failure = None

    def progress(index, state):
        # 平均每 20 个批次落一次检查点，兼顾续跑粒度与写库开销；同时确认本次
        # 尝试仍持有运行，被新尝试接管就立刻停算，避免旧尝试继续消耗资源。
        if index % 20 and index:
            return
        if not persist(job_id, attempt, state):
            raise StaleAttempt

    try:
        result = analyze(rows, params, checkpoint=checkpoint, progress=progress)
        export = write_report(job_id, attempt, result)
    except StaleAttempt:
        log.info('Analysis %s attempt %s was superseded; discarding late result', job_id, attempt)
        return
    except ValueError as error:
        failure = str(error)[:500]
    except Exception:
        log.exception('Analysis %s failed', job_id)
        failure = '计算失败，请检查字段口径后重试'
    with Session.begin() as db:
        try:
            job = _owned(db, job_id, attempt)
        except StaleAttempt:
            return
        job.lease_until = None
        job.finished_at = now()
        if failure:
            # 发布失败时保留页面与导出的旧版，两者仍指向同一次成功的方案。
            job.status = Status.FAILED
            job.error = failure
        else:
            job.status = Status.COMPLETE
            job.result = result
            job.export_file = export
            job.error = None
        saved = db.get(RunCheckpoint, job_id)
        if saved is not None:
            db.delete(saved)
        record(db, 'worker', f'analysis.{job.status}', job.id, attempt=attempt, reason=failure)
        if job.schedule_id:
            plan = db.get(Schedule, job.schedule_id)
            ready = not failure and result.get('ready', True)
            detail = '已保存 CSV 到站内收件箱' if ready else (failure or result['headline'])
            receipt = Delivery(run_id=job.id, recipient=plan.recipient, state='delivered' if ready else 'blocked', detail=detail[:500])
            db.add(receipt)
            record(db, 'worker', f'delivery.{receipt.state}', job.id, recipient=plan.recipient)
