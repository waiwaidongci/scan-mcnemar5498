"""整批试用候选 B 的组合筛选。

每张票据按扫描批次整批入选或整批不入选。筛选同时受两个资源约束：

* 入选总票数不超过预算；
* 各票种中「仅 A 通过」的票数不超过该票种上限。

目标按业务口径依次比较：通过票数净增（仅 B 通过减仅 A 通过）最大、
入选总票数最少、入选批次编号序列字典序最小。最大净增不为正时返回空方案，
表示整批继续使用 A。

搜索按批次编号从大到小推进，状态以（入选票数, 各票种仅 A 通过用量）为键，
保存该资源下净增最大的组合；净增并列时保留批次编号序列字典序更小的组合。
由于后续批次编号都更小，共同后缀不会改变这一比较，因此去重是安全的。
每个批次处理完可把前沿写入检查点，中断重启后从已处理前缀继续。
"""

CHECKPOINT_VERSION = 2
MAX_STATES = 20_000


class StaleAttempt(Exception):
    """更晚的一次尝试已经接管该运行，当前尝试不得再写入。"""


def aggregate(documents, type_order):
    """按批次汇总票数、差异对和各票种仅 A 通过票数。"""
    batches = {}
    for row in documents:
        batch = batches.get(row["batch_id"])
        if batch is None:
            batch = batches[row["batch_id"]] = {
                "batch_id": row["batch_id"],
                "documents": 0,
                "both_pass": 0,
                "a_only": 0,
                "b_only": 0,
                "both_fail": 0,
                "a_only_by_type": {name: 0 for name in type_order},
            }
        batch["documents"] += 1
        if row["a_pass"] and row["b_pass"]:
            batch["both_pass"] += 1
        elif row["a_pass"]:
            batch["a_only"] += 1
            batch["a_only_by_type"][row["document_type"]] += 1
        elif row["b_pass"]:
            batch["b_only"] += 1
        else:
            batch["both_fail"] += 1
    for batch in batches.values():
        batch["net"] = batch["b_only"] - batch["a_only"]
        batch["usage"] = tuple(batch["a_only_by_type"][name] for name in type_order)
    return sorted(batches.values(), key=lambda item: item["batch_id"], reverse=True)


def _caps(type_order, limits):
    return tuple(int(limits.get(name, 0)) for name in type_order)


def _fingerprint(batches, budget, caps, type_order):
    return {
        "budget": budget,
        "types": list(type_order),
        "caps": list(caps),
        "batches": [[item["batch_id"], item["documents"], item["a_only"], item["b_only"]]
                    for item in sorted(batches, key=lambda row: row["batch_id"])],
    }


def _tie_key(state):
    """越小的键越优：净增最大、票数最少、批次编号序列字典序最小。

    批次按编号升序排列后再比较，与业务口径「按批次编号排序」一致。
    """
    net, documents, _usage, selected = state
    return (-net, documents, tuple(sorted(selected)))


def _dominates(left, right):
    """left 在任意相同后续选择下都不差于 right。

    批次按编号降序处理，后续批次编号都更小，因此两边共同加入的后缀不会
    改变「已选批次按编号升序」的字典序比较。只有目标与并列取舍都不劣时，
    才允许剪掉 right。
    """
    net, documents, usage, selected = left
    other_net, other_documents, other_usage, other_selected = right
    if documents > other_documents or any(a > b for a, b in zip(usage, other_usage)):
        return False
    if net != other_net:
        return net > other_net
    if documents != other_documents:
        return documents < other_documents
    return tuple(sorted(selected)) <= tuple(sorted(other_selected))


def _prune(states):
    """去掉被同一资源或更优资源状态支配的组合。"""
    unique = {}
    for state in states:
        key = (state[1], state[2])
        current = unique.get(key)
        if current is None or _tie_key(state) < _tie_key(current):
            unique[key] = state
    ordered = sorted(unique.values(), key=lambda item: (item[1], -item[0], item[3]))
    kept = []
    for state in ordered:
        if any(_dominates(other, state) for other in kept):
            continue
        kept = [other for other in kept if not _dominates(state, other)]
        kept.append(state)
    if len(kept) > MAX_STATES:
        raise ValueError('批次组合过多，请减少批次或收紧预算与上限后重试。')
    return kept


def _serialize(states):
    return [[state[0], state[1], list(state[2]), list(state[3])] for state in states]


def _restore(raw):
    return [(int(item[0]), int(item[1]), tuple(int(value) for value in item[2]),
             tuple(str(value) for value in item[3])) for item in raw]


def best_selection(batches, budget, limits, type_order, checkpoint=None, progress=None):
    """返回满足预算与票种上限的最优整批组合。

    checkpoint 与 progress 让调用方把前沿落盘；progress 收到 (已处理批次,
    可序列化前沿)。前沿指纹与当前配对或限额不一致时，检查点会被丢弃。
    """
    budget = int(budget)
    caps = _caps(type_order, limits)
    # 前沿去重依赖「按批次编号降序处理」：后续批次编号更小，共同后缀不改变并列取舍。
    batches = sorted(batches, key=lambda item: item["batch_id"], reverse=True)
    fingerprint = _fingerprint(batches, budget, caps, type_order)
    states = [(0, 0, tuple(0 for _ in type_order), ())]
    start = 0
    if isinstance(checkpoint, dict) and checkpoint.get("version") == CHECKPOINT_VERSION:
        if checkpoint.get("fingerprint") == fingerprint:
            index = checkpoint.get("index", 0)
            raw = checkpoint.get("states")
            if isinstance(index, int) and 0 <= index <= len(batches) and isinstance(raw, list):
                try:
                    restored = _restore(raw)
                except (TypeError, ValueError, IndexError):
                    restored = None
                if restored is not None and all(len(item[2]) == len(type_order) for item in restored):
                    start, states = index, restored

    for index in range(start, len(batches)):
        batch = batches[index]
        additions = []
        for net, documents, usage, selected in states:
            next_documents = documents + batch["documents"]
            if next_documents > budget:
                continue
            next_usage = tuple(value + extra for value, extra in zip(usage, batch["usage"]))
            if any(value > cap for value, cap in zip(next_usage, caps)):
                continue
            additions.append((net + batch["net"], next_documents, next_usage,
                              (batch["batch_id"],) + selected))
        states = _prune(states + additions)
        if progress:
            progress(index + 1, {
                "version": CHECKPOINT_VERSION,
                "fingerprint": fingerprint,
                "index": index + 1,
                "states": _serialize(states),
            })

    best = min(states, key=_tie_key)
    if best[0] <= 0:
        best = (0, 0, tuple(0 for _ in type_order), ())
    return _summarize(batches, best, budget, caps, type_order)


def _summarize(batches, best, budget, caps, type_order):
    net, documents, usage, selected = best
    chosen = set(selected)
    ordered = sorted(batches, key=lambda item: item["batch_id"])
    rows = []
    for batch in ordered:
        rows.append({
            "batch_id": batch["batch_id"],
            "documents": batch["documents"],
            "a_only": batch["a_only"],
            "b_only": batch["b_only"],
            "net": batch["net"],
            "selected": "是" if batch["batch_id"] in chosen else "否",
        })
    a_only = sum(batch["a_only"] for batch in batches if batch["batch_id"] in chosen)
    b_only = sum(batch["b_only"] for batch in batches if batch["batch_id"] in chosen)
    usage_rows = [{
        "item": "入选总票数",
        "used": documents,
        "limit": budget,
        "remaining": budget - documents,
    }]
    for name, cap, used in zip(type_order, caps, usage):
        usage_rows.append({
            "item": f"{name} · 仅 A 通过",
            "used": used,
            "limit": cap,
            "remaining": cap - used,
        })
    return {
        "batch_ids": sorted(selected),
        "documents": documents,
        "net": net,
        "a_only": a_only,
        "b_only": b_only,
        "usage": {name: used for name, used in zip(type_order, usage)},
        "limits": {name: cap for name, cap in zip(type_order, caps)},
        "budget": budget,
        "effective": net > 0,
        "batches": rows,
        "usage_rows": usage_rows,
    }
