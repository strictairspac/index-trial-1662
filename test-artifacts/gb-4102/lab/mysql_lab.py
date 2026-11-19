"""Isolated MySQL instance control for the crash-durability comparison.

This environment has no Docker daemon and no privilege escalation, so the
official MySQL 8.0.40 linux-glibc2.28-aarch64 tarball is used directly.
Point MYSQL_LAB_BASE at the extracted tarball and MYSQL_LAB_LIBDIR at a
directory holding libaio.so.1.

Lab state (datadirs, error logs) lives under MYSQL_LAB_ROOT, deliberately
outside the artifact tree and outside the repository: it is disposable
machine state, while receipts and ledgers are kept as evidence.

Both crash rounds run with innodb_log_writer_threads=OFF held constant.
On MySQL 8.0 the default dedicated log-writer threads write the redo log to
the OS page cache on every commit even when flush_log_at_trx_commit=0, so a
process kill loses nothing and the two settings would be indistinguishable.
With the dedicated threads off, flush_log_at_trx_commit is the knob that
decides whether a commit is on disk when the process dies.
"""
import os, signal, subprocess, time, shutil

BASE = os.environ.get('MYSQL_LAB_BASE', '/tmp/mysql80-arm')
LIBDIR = os.environ.get('MYSQL_LAB_LIBDIR', '/tmp/libaio-root/usr/lib/aarch64-linux-gnu')
LAB_ROOT = os.environ.get('MYSQL_LAB_ROOT', '/tmp/gb4102-lab')
MYSQLD = os.path.join(BASE, 'bin', 'mysqld')

LOG_WRITER_THREADS = 'OFF'
APP_DB = 'indextrial'
CRASH_DB = 'crashlab'
BATCH_ROWS = 25

TELEMETRY_DDL = """CREATE TABLE IF NOT EXISTS telemetry(
    id INT PRIMARY KEY, sensor_id INT NOT NULL, recorded_at VARCHAR(30) NOT NULL,
    device_type VARCHAR(40) NOT NULL, value DOUBLE NOT NULL) ENGINE=InnoDB"""
# The application model declares an auto-increment id, so its schema must too.
APP_TELEMETRY_DDL = """CREATE TABLE IF NOT EXISTS telemetry(
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, sensor_id INT NOT NULL,
    recorded_at VARCHAR(30) NOT NULL, device_type VARCHAR(40) NOT NULL,
    value DOUBLE NOT NULL) ENGINE=InnoDB"""

PORTS = {'trial': 19300, 'fc1': 19301, 'fc0': 19302, 'fc2': 19303}
FLUSH = {'fc1': 1, 'fc0': 0, 'fc2': 2}


def _env():
    e = dict(os.environ)
    prior = e.get('LD_LIBRARY_PATH')
    e['LD_LIBRARY_PATH'] = LIBDIR + (':' + prior if prior else '')
    return e


class MysqlInstance:
    def __init__(self, role, port, flush):
        self.role = role
        self.port = port
        self.flush = int(flush)
        self.dir = os.path.join(LAB_ROOT, role)
        self.datadir = os.path.join(self.dir, 'data')
        self.errlog = os.path.join(self.dir, 'mysqld.err')
        self.proc = None

    def socket(self):
        return os.path.join(self.datadir, 'mysqld.sock')

    def pidfile(self):
        return os.path.join(self.datadir, 'mysqld.pid')

    def args(self):
        return [MYSQLD, '--no-defaults', '--basedir=' + BASE, '--datadir=' + self.datadir,
                '--port=%d' % self.port, '--bind-address=127.0.0.1',
                '--socket=' + self.socket(), '--pid-file=' + self.pidfile(),
                '--log-error=' + self.errlog, '--mysqlx=0', '--skip-name-resolve',
                '--skip-log-bin',
                '--innodb-flush-log-at-trx-commit=%d' % self.flush,
                '--innodb-log-writer-threads=' + LOG_WRITER_THREADS,
                '--innodb-buffer-pool-size=512M']

    def initialize(self, fresh=True):
        os.makedirs(self.dir, exist_ok=True)
        if fresh and os.path.isdir(self.datadir):
            shutil.rmtree(self.datadir)
        os.makedirs(self.datadir)
        r = subprocess.run([MYSQLD, '--no-defaults', '--initialize-insecure',
                            '--basedir=' + BASE, '--datadir=' + self.datadir,
                            '--log-error=' + self.errlog + '.init'],
                           env=_env(), capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError('initialize failed: ' + (r.stderr or '')[-2000:])

    def start(self, timeout=180):
        if os.path.exists(self.pidfile()):
            try:
                os.unlink(self.pidfile())
            except OSError:
                pass
        self.proc = subprocess.Popen(self.args(), env=_env(),
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import pymysql
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError('mysqld (%s) exited during startup, see %s'
                                   % (self.role, self.errlog))
            try:
                pymysql.connect(unix_socket=self.socket(), user='root', password='',
                               connect_timeout=1).close()
                return self
            except Exception:
                time.sleep(0.05)
        raise RuntimeError('mysqld (%s) not ready after %ss' % (self.role, timeout))

    def is_up(self):
        import pymysql
        try:
            pymysql.connect(unix_socket=self.socket(), user='root', password='',
                           connect_timeout=1).close()
            return True
        except Exception:
            return False

    def kill9(self):
        pid = None
        try:
            pid = int(open(self.pidfile()).read().strip())
        except Exception:
            pid = self.proc.pid if self.proc else None
        if pid:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if self.proc:
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                pass

    def stop_clean(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.kill9()

    def err_tail(self, n=4000):
        try:
            with open(self.errlog, 'r', errors='replace') as f:
                return f.read()[-n:]
        except OSError:
            return ''


def instance(role):
    return MysqlInstance(role, PORTS[role], FLUSH.get(role, 1))


def root_conn(inst):
    import pymysql
    return pymysql.connect(unix_socket=inst.socket(), user='root', password='', autocommit=True)


def prepare_schema(inst, schema, ddl):
    c = root_conn(inst)
    cur = c.cursor()
    cur.execute("CREATE USER IF NOT EXISTS 'lab'@'%' IDENTIFIED WITH mysql_native_password BY 'labpass'")
    cur.execute("GRANT ALL PRIVILEGES ON *.* TO 'lab'@'%'")
    cur.execute("FLUSH PRIVILEGES")
    cur.execute("CREATE DATABASE IF NOT EXISTS `%s`" % schema)
    cur.execute("USE `%s`" % schema)
    cur.execute(ddl)
    c.close()


def tcp_conn(inst, schema, autocommit=True):
    import pymysql
    return pymysql.connect(host='127.0.0.1', port=inst.port, user='lab', password='labpass',
                           database=schema, autocommit=autocommit, charset='utf8mb4')
