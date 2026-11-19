#!/usr/bin/env python3
"""Evidence collection runs for gb-4102, kept separate from the main rounds.

  trial-concurrent  run the unmodified application index trial against its own
                    schema and database, while both crash rounds are writing
                    their plan and being killed. Proves the original trial kept
                    working normally and is unaffected by the crash rounds.

  receipt-latency   measure that recording a receipt does not hold up the next
                    batch write: compare commit-to-commit gap with receipts on
                    vs. a no-receipt control, same plan, same settings.

  repeat            repeat one crash round N times to show the loss pattern is a
                    property of the setting, not a one-off timing accident.
"""
import json, os, queue, statistics, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import plan as planmod
import mysql_lab
import stage


def _trial_cmd(inst):
    env = dict(os.environ)
    env['DATABASE_URL'] = 'mysql+pymysql://lab:labpass@127.0.0.1:%d/%s' % (inst.port, mysql_lab.APP_DB)
    env['JWT_SECRET'] = 'gb-4102-lab'
    env['PYTHONPATH'] = os.path.join(stage.PROJECT, 'backend')
    env['PYTHONUNBUFFERED'] = '1'
    code = ('import json,sys;from src.services.index_algorithm import run;'
            'print(json.dumps(run(json.loads(sys.argv[1])),ensure_ascii=False))')
    snap = {'workload': {'sensor_id': 17, 'start_at': '2026-11-01T00:00:00',
                         'end_at': '2026-11-30T23:59:59', 'device_type': None},
            'candidate': {'columns': ['sensor_id', 'recorded_at']}}
    return env, [sys.executable, '-c', code, json.dumps(snap)]


def cmd_trial_concurrent(args):
    """The original trial, repeatedly, in its own instance while rounds run."""
    import stage as st
    os.makedirs(stage.rdir('concurrent'), exist_ok=True)
    inst = mysql_lab.instance('trial')
    if not inst.is_up():
        inst.start()
    procs = {}
    # launch both crash rounds in parallel; each kills its own mysqld
    env_round = dict(os.environ, GB4102_RUNS=stage.rdir('concurrent'))
    for role in ('fc1', 'fc0'):
        procs[role] = subprocess.Popen(
            [sys.executable, os.path.join(HERE, 'stage.py'), 'round', '--round', role, '--force'],
            env=env_round, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    # keep running the original trial in its own instance until both rounds end
    env, cmd = _trial_cmd(inst)
    runs = []
    while any(pr.poll() is None for pr in procs.values()):
        t0 = time.time()
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=stage.PROJECT)
        rec = {'returncode': r.returncode, 'duration_s': round(time.time() - t0, 3),
               'started_at': round(t0, 6)}
        if r.returncode == 0 and r.stdout.strip():
            out = json.loads(r.stdout.strip().splitlines()[-1])
            rec.update({'rows': out.get('rows'), 'checksum': out.get('checksum'),
                        'equal_results': out.get('equal_results'),
                        'used_index': out.get('after_plan', [{}])[0].get('key')})
        else:
            rec['stderr'] = r.stderr[-1500:]
        runs.append(rec)
        # make sure the app schema is intact for the next trial
        c = mysql_lab.root_conn(inst)
        cur = c.cursor()
        cur.execute('USE `%s`' % mysql_lab.APP_DB)
        cur.execute('SELECT COUNT(*) FROM telemetry')
        rec['app_rows'] = cur.fetchone()[0]
        c.close()
    round_out = {}
    for role, pr in procs.items():
        out, _ = pr.communicate()
        round_out[role] = {'returncode': pr.returncode, 'output': out[-2000:]}
    doc = {'generated_at': round(time.time(), 6),
           'trial_runs': runs, 'round_processes': round_out,
           'all_trial_runs_ok': all(x['returncode'] == 0 and x.get('equal_results') for x in runs),
           'checksums': sorted(set(x.get('checksum') for x in runs if x.get('checksum')))}
    st.jdump(os.path.join(stage.rdir('trial'), 'trial_concurrent.json'), doc)
    print('trial runs while rounds ran: %d, all ok=%s, distinct checksums=%d'
          % (len(runs), doc['all_trial_runs_ok'], len(doc['checksums'])))
    inst.stop_clean()


def cmd_receipt_latency(args):
    """Compare commit-to-commit gap with receipts on vs a no-receipt control.

    Run twice: paced at the plan's 40 ms cadence, and unpaced (back-to-back) so
    that any per-batch cost the receipt path adds would be visible.
    """
    p = planmod.load_plan()
    out = {}
    for mode, pace_ms in (('paced', p['phases'][0]['pace_ms']), ('unpaced', 0)):
        for role in ('fc1', 'fc0'):
            inst = mysql_lab.instance(role)
            for with_receipts in (False, True):
                inst.initialize(fresh=True)
                inst.start()
                mysql_lab.prepare_schema(inst, mysql_lab.CRASH_DB, mysql_lab.TELEMETRY_DDL)
                conn = mysql_lab.tcp_conn(inst, mysql_lab.CRASH_DB, autocommit=False)
                rec = stage.ReceiptRecorder(os.path.join('/tmp', 'lat-%s-%s.jsonl'
                                                         % (role, with_receipts))) if with_receipts else None
                gaps, total0 = [], time.time()
                prev = None
                for batch in range(1, 181):
                    due = total0 + batch * (pace_ms / 1000.0)
                    sql = planmod.insert_sql(p, batch)
                    cur = conn.cursor()
                    cur.execute(sql)
                    conn.commit()
                    now = time.time()
                    if rec:
                        rec.append(batch, now, p['batch_rows'])
                    if prev is not None:
                        gaps.append((now - prev) * 1000)
                    prev = now
                    if due > time.time():
                        time.sleep(due - time.time())
                if rec:
                    rec.close()
                conn.close()
                out.setdefault(mode, {}).setdefault(role, {})[
                    'with_receipts' if with_receipts else 'control'] = {
                    'median_gap_ms': round(statistics.median(gaps), 4),
                    'p95_gap_ms': round(sorted(gaps)[int(len(gaps) * 0.95)], 4),
                    'max_gap_ms': round(max(gaps), 4),
                    'samples': len(gaps)}
                inst.stop_clean()
    stage.jdump(os.path.join(stage.ROOT, 'evidence', 'receipt_latency.json'), out)
    print(json.dumps(out, indent=2))


def cmd_repeat(args):
    """Repeat a crash round a few times; report verdict distributions."""
    role = args.round
    n = args.times
    results = []
    root = os.path.join(stage.ROOT, 'evidence', 'repeat-runs')
    for i in range(n):
        tag = 'repeat-%s-%d' % (role, i + 1)
        subprocess.run([sys.executable, os.path.join(HERE, 'stage.py'), 'round',
                        '--round', role, '--force'], check=True,
                       env=dict(os.environ, GB4102_RUNS=root),
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        rd = os.path.join(root, role)
        receipts, torn = stage.read_receipts(os.path.join(rd, 'receipts.jsonl'))
        inst = mysql_lab.instance(role)
        if not inst.is_up():
            inst.start()
        conn = mysql_lab.tcp_conn(inst, mysql_lab.CRASH_DB, autocommit=True)
        cur = conn.cursor()
        p = planmod.load_plan()
        counts = {'acked_readable': 0, 'acked_lost': 0, 'unacked_readable': 0,
                  'unacked_absent': 0, 'unacked_partial': 0}
        for batch in range(1, p['total_batches'] + 1):
            lo, hi = planmod.batch_range(p, batch)
            cur.execute('SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry '
                        'WHERE id BETWEEN %s AND %s ORDER BY id', (lo, hi))
            rows = cur.fetchall()
            _, _, v = stage.classify(p, batch, receipts, len(rows), rows)
            counts[v] += 1
        conn.close()
        inst.stop_clean()
        meta = stage.jload(os.path.join(rd, 'round.json'))
        results.append({'repeat': i + 1, 'counts': counts,
                        'receipts': len(receipts), 'torn': len(torn),
                        'write_elapsed_s': meta.get('write_elapsed_s')})
        print(tag, counts)
    stage.jdump(os.path.join(stage.ROOT, 'evidence', 'repeat_%s.json' % role), results)


def main():
    ap = __import__('argparse').ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    sub.add_parser('trial-concurrent')
    sub.add_parser('receipt-latency')
    r = sub.add_parser('repeat')
    r.add_argument('--round', required=True, choices=['fc1', 'fc0'])
    r.add_argument('--times', type=int, default=3)
    a = ap.parse_args()
    {'trial-concurrent': cmd_trial_concurrent, 'receipt-latency': cmd_receipt_latency,
     'repeat': cmd_repeat}[a.cmd](a)


if __name__ == '__main__':
    main()
