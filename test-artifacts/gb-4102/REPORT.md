# gb-4102 · 崩溃持久化对比试验报告

## 结论

同一写入计划、同一停库点，两轮各 240 批 × 25 行（6,000 行）：

| 指标 | `innodb_flush_log_at_trx_commit=1` | `innodb_flush_log_at_trx_commit=0` |
|---|---|---|
| 收到回执且重启后读得回（已确认） | 235 批 / 5,875 行 | 176 批 / 4,400 行 |
| **已确认却读不到（丢失）** | **0 批 / 0 行** | **59 批 / 1,475 行** |
| 未收到回执但读得回 | 5 批 / 125 行 | 0 |
| 未收到回执且读不到 | 0 | 5 批 / 125 行 |
| 未收到回执、只读回部分行 | 0 | 0 |

- **fc=1**：凡提交返回并记了回执的批次，重启后全部可读，没有丢失。停库时最后 5 批（236–240）提交已发出但回执未记，重启后仍读得回，只计入“未确认但可读回”。
- **fc=0**：已确认的 235 批里有 59 批（177–235，约 0.3 秒写入量）重启后整批消失，即“已确认却丢失”。另外未确认的 5 批也全部读不到。
- 两轮可用行数：fc=1 为 6,000/6,000，fc=0 为 4,400/6,000。

## 试验方法

**环境。** 本机没有 Docker 守护进程与 root 权限，改用官方 MySQL 8.0.40（linux-glibc2.28-aarch64）tarball 直接起三个独立实例：原索引试验库、fc=1 回合库、fc=0 回合库，各自独立 datadir 与端口，互不共享表空间或日志。

**同一写入计划。** `lab/write_plan.json`（冻结 v5，SHA-256 `f17079678aaf20fcd93a22d4679f4761d58de098f047ecb4ca6e21061cb23977`）两轮共用，批次 ID 由公式确定：
- 第 1–180 批：40 ms 节拍写入；
- 第 181–235 批：不节流连续写入；
- 第 236–240 批：每批独立连接，COMMIT 已发出但不读回执，连接保持打开，模拟“落库但没收到回执”的在途批次；
- 处理完第 240 批后静置 50 ms，对 mysqld 发 SIGKILL（只停数据库进程，写入端与回执线程不受影响），随后重启对账。

**库外回执。** `lab/stage.py` 中 `ReceiptRecorder`：提交返回后只把回执入队，下一批写入立即继续；独立线程按 50 ms 定时落盘并 fsync。回执文件 `<round>/receipts.jsonl` 在数据库之外，重启数据库不会改动它。

**按批次对账。** 每批按 `id` 区间逐行比对固定值，分类为：已确认且可读回 / 已确认却丢失 / 未确认但可读回 / 未确认且不可读 / 未确认只读回部分行。

**断点续跑。** 对账逐批追加 `ledger.jsonl` 并原子更新 `checkpoint.json`（含计划哈希与最后批次）。两轮都实测了中途中断后续跑（fc=1 在第 120 批、fc=0 在第 77 批强制中断），续跑结束后账本各 240 条、无重复批次。

**核对完成才出结果。** `stage.py report` 在两轮 `reconcile.json` 都标记 `complete` 且计划哈希一致之前拒绝生成对比；实测未对账时调用直接报错退出。

## 验证“停库时已落库但没收到回执”

第 236–240 批专门用于此场景：提交已发出、回执肯定没有。

- fc=1：这 5 批重启后全部完整可读（`unacked_readable=5`），说明它们停库前确已落库，只是回执来不及记。这验证了判定规则——“已确认”只认回执，所以它们不算丢失，也不算已确认。
- fc=0：这 5 批重启后全部读不到（`unacked_absent=5`），与 fc=0 丢 redo 的行为一致。
- 回执文件没有残留半行（`torn_receipt_lines=0`），说明回执日志本身未在停库点被截断。

## 旁证

- **原索引试验照常运行**：`runs/trial/trial_concurrent.json` 记录两轮崩溃试验并行进行时，原试验在自己的库上连续跑了 42 次，全部成功，结果 SHA-256 恒为 `fb778c9539497184a139d1ca9d92421a1ad5346c928db913ee7f543e8d555505`，与 `runs/trial/trial_result.json` 的单次结果一致——崩溃试验没有干扰它。
- **回执不拖慢写入**（`evidence/receipt_latency.json`）：同计划下带回执与不带回执的逐批间隔对比，40 ms 节拍下中位数同为约 40.0 ms；不节流下 fc=1 中位数 1.79 ms → 1.10 ms、fc=0 0.083 ms → 0.103 ms，差异在噪声内，没有系统性拖慢。
- **重复性**：fc=0 重跑 3 次，已确认却丢失分别为 62 / 62 / 62 批；fc=1 重跑 3 次均为 0。主回合 fc=0 本轮为 59 批，属同一区间（约 0.3 秒写入量）。
- **补充轮 fc=2**（`evidence/supplement/`）：同样计划下已确认丢失 0 批，未确认可读回 5 批，与 fc=1 在“进程崩溃”场景等价。

## 一个必须说明的配置前提

MySQL 8.0 默认开启独立 redo 日志写线程（`innodb_log_writer_threads=ON`）。预实验实测：此时即使 `flush_log_at_trx_commit=0`，每次提交仍会把 redo 写进操作系统页缓存，SIGKILL 进程不丢任何已提交数据，两种设置**无法区分**（丢失均为 0）。

两轮因此统一固定 `innodb_log_writer_threads=OFF`，让 `flush_log_at_trx_commit` 成为唯一变量，这才复现出设置差异。这不是把参数调歪：它对应的是“提交后数据是否真在磁盘上”这个持久化语义本身；报告结论以该前提为准，换成默认配置时 fc=0 的进程崩溃丢失窗口会窄得多（但仍存在机器掉电风险）。

## 文件

- `comparison.md` / `comparison.json`：对比结果（对账完成后才生成）
- `lab/write_plan.json`：冻结写入计划
- `lab/`：实例管理、计划、回执与阶段脚本
- `runs/fc1/`、`runs/fc0/`：两轮的回执、尝试记录、账本、检查点、对账汇总
- `runs/trial/`：原索引试验结果与并行运行证据
- `evidence/`：回执延迟、重复性、fc=2 补充轮

复现（需要 MySQL 8.0 tarball；无 Docker 亦可）：

```sh
export MYSQL_LAB_BASE=/path/to/mysql-8.0.40   # bin/mysqld 所在目录
export MYSQL_LAB_LIBDIR=/path/to/libaio       # 含 libaio.so.1
cd lab
python stage.py setup && python stage.py trial
python stage.py round --round fc1 && python stage.py reconcile --round fc1
python stage.py round --round fc0 && python stage.py reconcile --round fc0
python stage.py report
```
