"""Frozen write plan shared by both crash rounds, plus expected-row helpers."""
import hashlib, json, os

HERE = os.path.dirname(os.path.abspath(__file__))
PLAN_PATH = os.path.join(HERE, 'write_plan.json')


def load_plan(path=PLAN_PATH):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def plan_sha256(plan):
    blob = json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(blob).hexdigest()


def batch_range(plan, batch):
    n = plan['batch_rows']
    lo = (batch - 1) * n + 1
    hi = batch * n
    return lo, hi


def expected_row(plan, batch, seq):
    n = plan['batch_rows']
    return {
        'id': (batch - 1) * n + seq,
        'sensor_id': plan['sensor_id'],
        'recorded_at': plan['recorded_at'],
        'device_type': plan['device_type'],
        'value': 30 + ((batch + seq) % 900) / 10,
    }


def insert_sql(plan, batch):
    """One multi-row INSERT = one batch; InnoDB makes the statement atomic."""
    n = plan['batch_rows']
    rows = []
    for seq in range(1, n + 1):
        r = expected_row(plan, batch, seq)
        rows.append("(%d,%d,'%s','%s',%.1f)" % (
            r['id'], r['sensor_id'], r['recorded_at'], r['device_type'], r['value']))
    return 'INSERT INTO telemetry(id,sensor_id,recorded_at,device_type,value) VALUES ' + ','.join(rows)
