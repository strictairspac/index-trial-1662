#!/usr/bin/env python3
"""Crash-durability comparison for two MySQL persistence settings (gb-4102).

Stages (each idempotent; safe to re-run):
  setup                 create isolated instances (trial + two crash rounds)
  trial                 run the unmodified application index trial in its own schema
  round --round fc1|fc0 write the frozen batch plan, SIGKILL mysqld at batch 240
  reconcile --round ...  classify every batch from an out-of-database receipt log;
                         checkpointed, so an interrupted run resumes without double counting
  report                emit the comparison; refuses until both rounds are fully reconciled
"""
import argparse, json, os, queue, signal, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)                        # test-artifacts/gb-4102
PROJECT = os.path.dirname(os.path.dirname(ROOT))    # repository root (holds backend/)
def runs_root():
    return os.environ.get('GB4102_RUNS') or os.path.join(ROOT, 'runs')
LAB = os.path.join(ROOT, 'lab')
sys.path.insert(0, HERE)

import plan as planmod
import mysql_lab
from mysql_lab import APP_DB, CRASH_DB, APP_TELEMETRY_DDL, TELEMETRY_DDL

RECEIPT_FSYNC_S = 0.05
ROUND_META = {
    'fc1': {'flush': 1, 'label': 'innodb_flush_log_at_trx_commit=1'},
    'fc0': {'flush': 0, 'label': 'innodb_flush_log_at_trx_commit=0'},
    'fc2': {'flush': 2, 'label': 'innodb_flush_log_at_trx_commit=2'},
}
COMPARED = ('fc1', 'fc0')   # the two settings the report compares


def rdir(name):
    d = os.path.join(runs_root(), name)
    os.makedirs(d, exist_ok=True)
    return d


def jdump(path, obj):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def jload(path, default=None):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


def jappend(path, obj):
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + '\n')
        f.flush()


def plan_hash(plan):
    return planmod.plan_sha256(plan)


# ------------------------------------------------------------------ setup
def cmd_setup(args):
    p = planmod.load_plan()
    for role in COMPARED:
        inst = mysql_lab.instance(role)
        inst.initialize(fresh=True)
        inst.start()
        mysql_lab.prepare_schema(inst, CRASH_DB, TELEMETRY_DDL)
        inst.stop_clean()
        print('%-4s ready flush=%d port=%d datadir=%s'
              % (role, inst.flush, inst.port, inst.datadir))
    inst = mysql_lab.instance('trial')
    inst.initialize(fresh=True)
    inst.start()
    mysql_lab.prepare_schema(inst, APP_DB, APP_TELEMETRY_DDL)
    inst.stop_clean()
    jdump(os.path.join(rdir('trial'), 'plan.json'), {'plan': p, 'plan_sha256': plan_hash(p)})
    print('trial ready port=%d datadir=%s' % (inst.port, inst.datadir))


# ------------------------------------------------------------------ trial
def cmd_trial(args):
    """The original application trial path, untouched, in the application schema."""
    inst = mysql_lab.instance('trial')
    if not inst.is_up():
        inst.start()
    try:
        conn = mysql_lab.root_conn(inst)
        cur = conn.cursor()
        cur.execute('USE `%s`' % APP_DB)
        cur.execute('SELECT COUNT(*) FROM telemetry')
        if cur.fetchone()[0] == 0:
            from datetime import datetime, timedelta
            base = datetime(2026, 11, 1)
            rows = []
            for i in range(5000):
                rows.append("(%d,%d,'%s','%s',%.2f)" % (
                    i + 1, i % 100 + 1, (base + timedelta(minutes=i * 8)).isoformat(),
                    ['pump', 'motor', 'valve'][i % 3], round(30 + (i * 17 % 900) / 10, 2)))
            for k in range(0, len(rows), 1000):
                cur.execute('INSERT INTO telemetry(id,sensor_id,recorded_at,device_type,value) '
                            'VALUES ' + ','.join(rows[k:k + 1000]))
        cur.execute('SELECT COUNT(*) FROM telemetry')
        fixture_rows = cur.fetchone()[0]
        conn.close()

        env = dict(os.environ)
        env['DATABASE_URL'] = 'mysql+pymysql://lab:labpass@127.0.0.1:%d/%s' % (inst.port, APP_DB)
        env['JWT_SECRET'] = 'gb-4102-lab'
        env['PYTHONPATH'] = os.path.join(PROJECT, 'backend')
        env['PYTHONUNBUFFERED'] = '1'
        code = ('import json,sys;from src.services.index_algorithm import run;'
                'print(json.dumps(run(json.loads(sys.argv[1])),ensure_ascii=False))')
        snap = {'workload': {'sensor_id': 17, 'start_at': '2026-11-01T00:00:00',
                             'end_at': '2026-11-30T23:59:59', 'device_type': None},
                'candidate': {'columns': ['sensor_id', 'recorded_at']}}
        t0 = time.time()
        r = subprocess.run([sys.executable, '-c', code, json.dumps(snap)],
                           env=env, capture_output=True, text=True, cwd=PROJECT)
        out = {'fixture_rows': fixture_rows, 'returncode': r.returncode,
               'duration_s': round(time.time() - t0, 3), 'stderr': r.stderr[-4000:]}
        if r.returncode == 0 and r.stdout.strip():
            out['result'] = json.loads(r.stdout.strip().splitlines()[-1])
        else:
            out['stdout_tail'] = r.stdout[-2000:]
        jdump(os.path.join(rdir('trial'), 'trial_result.json'), out)
        print('index trial rc=%s duration=%ss rows=%s checksum=%s equal=%s' % (
            out['returncode'], out['duration_s'],
            out.get('result', {}).get('rows'), out.get('result', {}).get('checksum'),
            out.get('result', {}).get('equal_results')))
    finally:
        inst.stop_clean()


# ------------------------------------------------------------------ receipts
class ReceiptRecorder:
    """Out-of-database receipts. append() only enqueues, so recording a receipt
    never blocks the next batch; a separate thread writes and fsyncs on a timer."""

    def __init__(self, path):
        self.path = path
        if os.path.exists(path):
            os.unlink(path)
        self.q = queue.Queue()
        self.fh = open(path, 'a', encoding='utf-8')
        self.stop = threading.Event()
        self.fsyncs = 0
        self.th = threading.Thread(target=self._loop, daemon=True)
        self.th.start()

    def append(self, batch, acked_at, rows):
        self.q.put({'batch': batch, 'acked_at': round(acked_at, 6), 'rows': rows})

    def _loop(self):
        next_flush = time.monotonic() + RECEIPT_FSYNC_S
        while not self.stop.is_set():
            timeout = max(0.0, next_flush - time.monotonic())
            try:
                item = self.q.get(timeout=timeout)
            except queue.Empty:
                item = None
            if item is not None:
                item['recorded_at'] = round(time.time(), 6)
                self.fh.write(json.dumps(item, sort_keys=True) + '\n')
                self.fh.flush()
            if time.monotonic() >= next_flush:
                # timer-driven durability flush; the writer never waits on this
                os.fsync(self.fh.fileno())
                self.fsyncs += 1
                next_flush = time.monotonic() + RECEIPT_FSYNC_S
        # drain whatever was queued before the kill
        while True:
            try:
                item = self.q.get_nowait()
            except queue.Empty:
                break
            item['recorded_at'] = round(time.time(), 6)
            self.fh.write(json.dumps(item, sort_keys=True) + '\n')
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fsyncs += 1

    def close(self):
        self.stop.set()
        self.th.join(timeout=30)
        self.fh.close()


def read_receipts(path):
    out, torn = {}, []
    if not os.path.exists(path):
        return out, torn
    with open(path, 'r', encoding='utf-8') as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                out[int(obj['batch'])] = obj
            except (json.JSONDecodeError, KeyError, ValueError):
                torn.append(raw)
    return out, torn


# ------------------------------------------------------------------ round
def cmd_round(args):
    role = args.round
    p = planmod.load_plan()
    rd = rdir(role)
    meta_path = os.path.join(rd, 'round.json')
    meta = jload(meta_path)
    if meta and meta.get('status') == 'killed' and not args.force:
        print('%s already ran and was killed at batch %d; use --force to redo'
              % (role, meta['kill_at_batch']))
        return

    inst = mysql_lab.instance(role)
    inst.initialize(fresh=True)
    inst.start()
    mysql_lab.prepare_schema(inst, CRASH_DB, TELEMETRY_DDL)

    attempts_path = os.path.join(rd, 'attempts.jsonl')
    receipts_path = os.path.join(rd, 'receipts.jsonl')
    for f in (attempts_path, receipts_path):
        if os.path.exists(f):
            os.unlink(f)

    meta = {'round': role, 'label': ROUND_META[role]['label'],
            'flush_log_at_trx_commit': inst.flush,
            'log_writer_threads': mysql_lab.LOG_WRITER_THREADS,
            'plan_sha256': plan_hash(p), 'plan_version': p['plan_version'],
            'batch_rows': p['batch_rows'], 'total_batches': p['total_batches'],
            'kill_at_batch': p['kill_at_batch'], 'pid': inst.proc.pid,
            'datadir': inst.datadir, 'port': inst.port,
            'started_at': round(time.time(), 6), 'status': 'writing'}
    jdump(meta_path, meta)

    rec = ReceiptRecorder(receipts_path)
    writer_conn = mysql_lab.tcp_conn(inst, CRASH_DB, autocommit=False)
    inflight = []          # connections whose COMMIT OK was deliberately not read
    t_start = time.time()
    kill_at = p['kill_at_batch']
    paced_end = p['phases'][0].get('batches', '1-180')
    paced_last = int(paced_end.split('-')[-1])
    inflight_first = int(p['phases'][2]['batches'].split('-')[0])

    try:
        for batch in range(1, kill_at + 1):
            sql = planmod.insert_sql(p, batch)
            if batch >= inflight_first:
                # Unacknowledged batch: fresh connection, COMMIT sent but OK not
                # read, so no receipt even though InnoDB may make it durable.
                c = mysql_lab.tcp_conn(inst, CRASH_DB, autocommit=False)
                c.cursor().execute(sql)
                from pymysql.constants import COMMAND
                t_commit = time.time()
                c._execute_command(COMMAND.COM_QUERY, 'COMMIT')   # deliberately unread
                jappend(attempts_path, {'batch': batch, 'phase': 'inflight',
                                        'commit_sent_at': round(t_commit, 6),
                                        'ack': None, 'rows': p['batch_rows']})
                inflight.append(c)
                if batch == kill_at:
                    time.sleep(p['kill_settle_ms'] / 1000.0)
            else:
                cur = writer_conn.cursor()
                cur.execute(sql)
                t_commit = time.time()
                writer_conn.commit()
                acked_at = time.time()
                rec.append(batch, acked_at, p['batch_rows'])
                jappend(attempts_path, {'batch': batch,
                                        'phase': 'paced' if batch <= paced_last else 'burst',
                                        'commit_sent_at': round(t_commit, 6),
                                        'acked_at': round(acked_at, 6),
                                        'ack': True, 'rows': p['batch_rows']})
                if batch <= paced_last:
                    due = t_start + batch * (p['phases'][0]['pace_ms'] / 1000.0)
                    if due > time.time():
                        time.sleep(due - time.time())
        # Kill the database process only; the writer host keeps running.
        kill_wall = time.time()
        pid = inst.proc.pid
        inst.kill9()
        killed_at = time.time()
        meta.update({'status': 'killed', 'killed_at': round(killed_at, 6),
                     'kill_wall': round(kill_wall, 6), 'killed_pid': pid,
                     'write_elapsed_s': round(killed_at - t_start, 3),
                     'receipt_fsyncs_before_close': rec.fsyncs,
                     'inflight_batches': list(range(inflight_first, kill_at + 1))})
        jdump(meta_path, meta)
        print('%s: committed %d batches, SIGKILL at %.3fs, receipt fsyncs=%d'
              % (role, kill_at, killed_at - t_start, rec.fsyncs))
    finally:
        for c in inflight:
            try:
                c.close()
            except Exception:
                pass
        try:
            writer_conn.close()
        except Exception:
            pass
        rec.close()
        if jload(meta_path, {}).get('status') != 'killed':
            meta['status'] = 'aborted'
            meta['aborted_at'] = round(time.time(), 6)
            jdump(meta_path, meta)


# ------------------------------------------------------------------ reconcile
def _ledger_batches(ledger_path):
    seen = {}
    if not os.path.exists(ledger_path):
        return seen
    with open(ledger_path, 'r', encoding='utf-8') as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                seen[int(obj['batch'])] = obj
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
    return seen


def classify(p, batch, receipts, rowcount, rows):
    expected = [planmod.expected_row(p, batch, s) for s in range(1, p['batch_rows'] + 1)]
    exact = (len(rows) == len(expected) and all(
        int(r[0]) == e['id'] and int(r[1]) == e['sensor_id'] and
        r[2] == e['recorded_at'] and r[3] == e['device_type'] and
        abs(float(r[4]) - e['value']) < 1e-9 for r, e in zip(rows, expected)))
    acked = batch in receipts
    if acked:
        verdict = 'acked_readable' if rowcount == p['batch_rows'] and exact else 'acked_lost'
    else:
        if rowcount == p['batch_rows'] and exact:
            verdict = 'unacked_readable'
        elif rowcount == 0:
            verdict = 'unacked_absent'
        else:
            verdict = 'unacked_partial'
    return acked, exact, verdict


def cmd_reconcile(args):
    role = args.round
    p = planmod.load_plan()
    rd = rdir(role)
    meta = jload(os.path.join(rd, 'round.json'))
    if not meta or meta.get('status') != 'killed':
        raise SystemExit('%s has no killed round to reconcile' % role)
    if meta['plan_sha256'] != plan_hash(p):
        raise SystemExit('write plan changed since %s ran; refusing to reconcile' % role)

    inst = mysql_lab.instance(role)
    if not inst.is_up():
        inst.start()

    receipts, torn = read_receipts(os.path.join(rd, 'receipts.jsonl'))
    ledger_path = os.path.join(rd, 'ledger.jsonl')
    ckpt_path = os.path.join(rd, 'checkpoint.json')
    done = _ledger_batches(ledger_path)
    ckpt = jload(ckpt_path, {}) or {}
    if ckpt.get('plan_sha256') and ckpt['plan_sha256'] != plan_hash(p):
        raise SystemExit('checkpoint belongs to a different plan; refusing to resume')

    conn = mysql_lab.tcp_conn(inst, CRASH_DB, autocommit=True)
    cur = conn.cursor()
    recovered_at = round(time.time(), 6)
    processed_now = 0
    try:
        for batch in range(1, p['total_batches'] + 1):
            if batch in done:
                continue
            lo, hi = planmod.batch_range(p, batch)
            cur.execute('SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry '
                        'WHERE id BETWEEN %s AND %s ORDER BY id', (lo, hi))
            rows = cur.fetchall()
            acked, exact, verdict = classify(p, batch, receipts, len(rows), rows)
            entry = {'batch': batch, 'acked': acked, 'rows_found': len(rows),
                     'rows_expected': p['batch_rows'], 'exact_match': exact,
                     'first_id': int(rows[0][0]) if rows else None,
                     'last_id': int(rows[-1][0]) if rows else None,
                     'verdict': verdict,
                     'acked_at': receipts[batch]['acked_at'] if acked else None,
                     'reconciled_at': round(time.time(), 6)}
            jappend(ledger_path, entry)
            done[batch] = entry
            jdump(ckpt_path, {'round': role, 'plan_sha256': plan_hash(p),
                              'last_batch': batch, 'total_batches': p['total_batches'],
                              'classified': len(done), 'updated_at': round(time.time(), 6)})
            processed_now += 1
            if args.interrupt_after is not None and processed_now >= args.interrupt_after:
                print('reconcile %s interrupted after %d new batches (checkpoint last=%d)'
                      % (role, processed_now, batch))
                raise SystemExit(75)
    finally:
        conn.close()

    summary = summarize(done, p)
    jdump(os.path.join(rd, 'reconcile.json'),
          {'round': role, 'label': meta['label'], 'plan_sha256': plan_hash(p),
           'total_batches': p['total_batches'], 'batches_classified': len(done),
           'torn_receipt_lines': len(torn), 'receipts_read': len(receipts),
           'recovered_at': recovered_at, 'summary': summary,
           'complete': len(done) == p['total_batches'],
           'finished_at': round(time.time(), 6)})
    print('reconcile %s complete: %s' % (role, json.dumps(summary['counts'], ensure_ascii=False)))


def summarize(done, p):
    counts = {'acked_readable': 0, 'acked_lost': 0, 'unacked_readable': 0,
              'unacked_absent': 0, 'unacked_partial': 0}
    rows_present = 0
    for batch in sorted(done):
        e = done[batch]
        counts[e['verdict']] += 1
        rows_present += e['rows_found']
    return {'counts': counts, 'rows_present': rows_present,
            'rows_expected_total': p['total_batches'] * p['batch_rows'],
            'acked_batches': sum(1 for e in done.values() if e['acked']),
            'unacked_batches': sum(1 for e in done.values() if not e['acked']),
            'acked_lost_batches': [b for b in sorted(done) if done[b]['verdict'] == 'acked_lost'],
            'unacked_readable_batches': [b for b in sorted(done)
                                         if done[b]['verdict'] in ('unacked_readable', 'unacked_partial')]}


# ------------------------------------------------------------------ report
def cmd_report(args):
    p = planmod.load_plan()
    rounds = {}
    for role in COMPARED:
        rec = jload(os.path.join(rdir(role), 'reconcile.json'))
        if not rec:
            raise SystemExit('%s not reconciled yet; run reconcile first' % role)
        if not rec.get('complete') or rec['batches_classified'] != p['total_batches']:
            raise SystemExit('%s reconciliation incomplete (%s/%s)'
                             % (role, rec['batches_classified'], p['total_batches']))
        if rec['plan_sha256'] != plan_hash(p):
            raise SystemExit('%s reconciled against a different plan' % role)
        rounds[role] = rec
    report = {'generated_at': round(time.time(), 6), 'plan_sha256': plan_hash(p),
              'plan': p, 'rounds': rounds,
              'trial': jload(os.path.join(rdir('trial'), 'trial_result.json'))}
    jdump(os.path.join(ROOT, 'comparison.json'), report)
    md = render_markdown(report)
    with open(os.path.join(ROOT, 'comparison.md'), 'w', encoding='utf-8') as f:
        f.write(md)
        f.flush()
        os.fsync(f.fileno())
    print(md)


def render_markdown(report):
    p = report['plan']
    L = ['# 崩溃持久化对比结果 · gb-4102', '']
    L.append('写入计划 `%s`（%d 批 × %d 行，冻结 v%d），两轮共用同一计划与同一批次数。'
             % (report['plan_sha256'][:16], p['total_batches'], p['batch_rows'], p['plan_version']))
    L.append('')
    L.append('| 指标 | fc=1 | fc=0 |')
    L.append('|---|---|---|')
    a = report['rounds']['fc1']['summary']['counts']
    b = report['rounds']['fc0']['summary']['counts']
    for label, key in [('收到回执且读得回（已确认）', 'acked_readable'),
                       ('**收到回执却读不到（丢失）**', 'acked_lost'),
                       ('未收到回执但读得回', 'unacked_readable'),
                       ('未收到回执且读不到', 'unacked_absent'),
                       ('未收到回执、仅读回部分行', 'unacked_partial')]:
        L.append('| %s | %d | %d |' % (label, a[key], b[key]))
    L.append('')
    L.append('| 轮次 | 确认批次 | 已确认却丢失的批次数 | 未收到回执但读得回的批次 |')
    L.append('|---|---|---|---|')
    for role in COMPARED:
        r = report['rounds'][role]
        s = r['summary']
        L.append('| %s (%s) | %d | %d | %s |' % (
            role, r['label'], s['acked_batches'], len(s['acked_lost_batches']),
            ', '.join(map(str, s['unacked_readable_batches'])) or '无'))
    L.append('')
    lost_map = {role: report['rounds'][role]['summary']['acked_lost_batches'] for role in COMPARED}
    if any(lost_map.values()):
        L.append('已确认却丢失的具体批次：')
        for role in COMPARED:
            bs = lost_map[role]
            L.append('- %s：%s' % (role, (('%d–%d（共 %d 批）' % (bs[0], bs[-1], len(bs)))
                                         if bs else '无')))
    L.append('')
    t = report.get('trial') or {}
    res = t.get('result') or {}
    L.append('原索引试验（独立库、照常运行）：rc=%s，返回 %s 行，SHA-256 `%s`，前后结果一致=%s。'
             % (t.get('returncode'), res.get('rows'), res.get('checksum'), res.get('equal_results')))
    return '\n'.join(L) + '\n'


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    sub.add_parser('setup')
    sub.add_parser('trial')
    r = sub.add_parser('round')
    r.add_argument('--round', required=True, choices=sorted(ROUND_META))
    r.add_argument('--force', action='store_true')
    c = sub.add_parser('reconcile')
    c.add_argument('--round', required=True, choices=sorted(ROUND_META))
    c.add_argument('--interrupt-after', type=int, default=None,
                   help='simulate an interrupted reconciliation after N new batches')
    sub.add_parser('report')
    args = ap.parse_args()
    {'setup': cmd_setup, 'trial': cmd_trial, 'round': cmd_round,
     'reconcile': cmd_reconcile, 'report': cmd_report}[args.cmd](args)


if __name__ == '__main__':
    main()
