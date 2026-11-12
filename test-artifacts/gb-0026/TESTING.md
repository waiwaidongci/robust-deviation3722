# GB-0026 中断恢复与一次性结果验证

## 结论

- 服务重启后自动恢复未完成研究：worker 启动与运行中扫描数据库，补投丢失的排队通知，
  并按创建时冻结的 `input_snapshot / alpha / seed` 重算；不重读来源窗口或后来批注。
- 重复投递、多个执行进程争抢同一研究时，条件 UPDATE 领取产生唯一 `claim_token`，
  只有当前租约持有者能提交结果；旧执行晚到的结果/错误均不落库、不写档案。
- 已完成研究是终态；获批补测建议不被恢复流程触碰。
- 状态、结果档案（`/artifacts/run-<id>.json`）与 `deviation.completed` 留痕一致：
  档案后写且原子替换，缺失/错配由恢复例程用库内 `result` 补齐。

## 运行

```sh
cd backend
python3 -m pip install -r requirements.txt pytest
PYTHONPATH=. python3 -m pytest tests/ -q
```

本地无 Docker 时，测试用 SQLite + 进程内假队列（`tests/fake_redis.py`、`tests/proc_worker_boot.py`）
跑同一套领取/提交/租约代码；生产 Compose 仍是 PostgreSQL16 + Redis7。
结果：`15 passed`（2026-11-12）。

## 用例与覆盖的中断点

| 用例 | 场景 | 断言 |
|---|---|---|
| `test_notification_lost_is_recovered_from_database` | 通知丢失后研究停在 queued | 恢复扫描补投，领取并完成 |
| `test_queue_outage_keeps_run_queued_for_recovery` | 入队时 Redis 不可用 | 研究保持 queued（不再标 failed 要用户重提），记 `deviation.enqueue.deferred`，随后恢复 |
| `test_crash_before_claim_resumes_frozen_inputs` | 领走前进程中断 | 重启领取后 fit/input 指纹与冻结输入直接计算结果一致 |
| `test_crash_after_claim_is_reclaimed_when_lease_expires` | 领走后、拟合中断（停在 running，原实现永久挂起） | 租约未过期不抢；过期后重新领取，attempt=2，有 `deviation.resumed`；旧令牌提交被拒 |
| `test_interrupt_before_result_commit_does_not_strand_or_duplicate` | 写结果前中断 | 无半份 result；恢复后 completed，档案与库一致，`finished_at` 有值 |
| `test_interrupt_after_result_computed_before_artifact` | 数据库已提交完成、档案未写时进程中断 | 档案缺失；`reconcile_artifact` 用库内 result 补齐 |
| `test_duplicate_delivery_executes_only_once` | 同一研究重复投递 5 次 | 只执行 1 次，attempt=1，`deviation.completed` 仅 1 条 |
| `test_concurrent_claim_has_single_winner` | 6 个执行进程同时领取 | 只有 1 个拿到令牌；晚到执行写不进 |
| `test_stale_execution_cannot_overwrite_completed_result` | 旧执行带着旧令牌晚到 | 不能覆盖 completed 结果，也不能把状态改回 |
| `test_worker_loop_recovers_queued_after_restart` | 真实 worker `main()` 循环重启 | 启动即补投 queued 并完成 |
| `test_long_execution_keeps_lease_alive` | 执行时长超过租约 | 续约线程保活，其他进程抢不走，attempt 仍为 1 |
| `test_multi_process_race_only_current_result_survives` | 旧租约过期后新旧执行并发写 | 只有当前令牌写入，claim_owner 指向新执行，档案一致 |
| `test_completed_run_and_approved_note_unchanged` | 恢复扫描/重复投递碰到完成研究与已批准建议 | 状态、result、attempt、完成留痕、建议内容均不变 |
| `test_resumed_result_matches_uninterrupted_for_same_frozen_inputs` | 中途用户改了窗口批注 | 恢复执行仍用冻结快照；结果与不中断计算完全一致（alpha/seed 保持） |
| `test_worker_restart_recovers_all_interruptions`（进程级，SIGKILL） | 真实 worker 进程被杀后重启：通知丢失、领走后拟合中被杀 | attempt 递增、`deviation.resumed` 递增、完成留痕与档案一致；两次完成 fit/input 指纹相同 |

## 行为变化

- 研究行新增 `claim_token/claim_owner/attempt/lease_expires_at/started_at/finished_at`；
  `ensure_schema()` 在启动时补列（兼容既有库），`migrations/001_initial.sql` 同步。
- 新增 `backend/src/services/job_service.py`（领取/续约/提交/失败/恢复）与
  `execution_service.py`（执行与档案）。
- 队列通知失败不再把研究直接标 failed，保持 queued 由恢复扫描接管；接口不暴露 `claim_token`。
- 时序参数：`WORKER_LEASE_SECONDS=90`、`WORKER_HEARTBEAT_SECONDS=20`、`WORKER_RECOVERY_SECONDS=10`（环境变量可调）。

## 已知边界

- 租约到期到重新领取之间存在最多一个恢复周期的等待窗口，期间页面显示 running。
- 恢复按数据库状态收敛，不重放历史 attempt 的结果；旧执行结果一律丢弃。
