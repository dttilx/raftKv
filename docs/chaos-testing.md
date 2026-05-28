# 混沌测试与一致性校验

本文档说明 `scripts/chaos_runner.sh` 的最小可用闭环：在故障场景下注入扰动，并在前后执行 `kv_consistency`，最后输出 JSON 报告。

## 本地运行

前置：已成功编译，且存在 `bin/raftCoreRun` 与 `bin/kv_consistency`。

```bash
chmod +x scripts/*.sh
./scripts/chaos_runner.sh --scenario kill-restart --rounds 3 --keys 400 --sleep 6
```

默认输出：

- 日志目录：`logs/`
- 报告文件：`logs/chaos_report.json`
- 每轮日志：
  - `logs/chaos_round_<n>_baseline.log`
  - `logs/chaos_round_<n>_post.log`

## 场景说明

- `kill-restart`（默认）
  - kill 一个指定节点端口对应进程
  - 等待 `--sleep`
  - 重启集群并执行 post-check
- `partition`
  - 通过 `iptables` 在本机对指定端口做输入/输出丢弃
  - 需要 root 权限（`sudo`）
- `delay-loss`
  - 通过 `tc netem` 在回环网卡注入延迟和丢包
  - 需要 root 权限（`sudo`）

## 常用参数

- `--scenario <kill-restart|partition|delay-loss>`
- `--rounds <N>`：故障轮数
- `--keys <N>`：每轮一致性检查 key 数
- `--check-rounds <N>`：`kv_consistency -r`
- `--node <idx>`：故障目标节点（默认 1）
- `--sleep <secs>`：故障持续秒数
- `--report <path>`：JSON 报告路径

## 报告格式

`chaos_report.json` 关键字段：

- `totalRounds` / `passRounds` / `failRounds`
- `scenario`
- `consistencyKeys`
- `rounds[]`
  - `baseline_rc`：故障前一致性检查退出码
  - `fault_rc`：故障注入步骤退出码
  - `post_rc`：故障后一致性检查退出码
  - `ok`：本轮是否成功

当任意一轮失败时，`chaos_runner.sh` 会以非 0 退出，方便 CI 门禁使用。
