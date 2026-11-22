import csv
from src.config.settings import ARTIFACTS


def safe_cell(value):
    if isinstance(value, str) and (value.lstrip().startswith(('=', '+', '-', '@'))
                                   or value.startswith(('\t', '\r', '\n'))):
        return "'" + value
    return value


def write_report(run_id, attempt, result):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    name = f'analysis-{run_id}-{attempt}.csv'
    path = ARTIFACTS / name
    staging = path.with_suffix('.tmp')
    with staging.open('w', encoding='utf-8-sig', newline='') as output:
        writer = csv.writer(output)
        writer.writerow([safe_cell(result['headline'])])
        writer.writerow([safe_cell(result.get('note', ''))])
        selection = result.get('selection')
        if selection:
            writer.writerow([])
            writer.writerow([safe_cell('整批试用 B 方案')])
            writer.writerow([safe_cell(selection['headline'])])
            writer.writerow([safe_cell('入选批次'), safe_cell('、'.join(selection['batch_ids']) or '无（继续使用 A）')])
            writer.writerow([safe_cell('入选票数 / 预算'),
                             safe_cell(f"{selection['documents']} / {selection['budget']}")])
            for metric in result['metrics']:
                writer.writerow([safe_cell(metric['label']), safe_cell(metric['value']),
                                 safe_cell(metric.get('detail', ''))])
        else:
            for metric in result['metrics']:
                writer.writerow([safe_cell(metric['label']), safe_cell(metric['value']),
                                 safe_cell(metric.get('detail', ''))])
        for table in result['tables']:
            writer.writerow([])
            writer.writerow([safe_cell(table['title'])])
            columns = table['columns']
            writer.writerow([safe_cell(column['label']) for column in columns])
            writer.writerows([safe_cell(row.get(column['key'])) for column in columns] for row in table['rows'])
    staging.replace(path)
    return name
