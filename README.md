# KVstorageBaseRaft-cpp

[![CI](https://github.com/dttilx/raftKv/actions/workflows/ci.yml/badge.svg)](https://github.com/dttilx/raftKv/actions/workflows/ci.yml)
[![Bench Nightly](https://github.com/dttilx/raftKv/actions/workflows/bench-nightly.yml/badge.svg)](https://github.com/dttilx/raftKv/actions/workflows/bench-nightly.yml)

基于 **C++20** 的 Raft 共识 + 分布式 KV 示例工程：Raft 核心、KV 服务、**gRPC / protobuf** RPC、客户端 Clerk、示例与压测工具。

## 功能概要

- Raft：选主、日志复制、提交与 apply 循环、持久化、快照。
- KV：经 Raft 提供 `Get`、`Put`、`Append`。
- RPC：gRPC + protobuf 双服务（节点间 Raft RPC、客户端↔KvServer）。
- 读路径：`Get` 采用 **ReadIndex 风格** 的线性一致读（见 `docs/一致性读流程.md`）。
- 工具：`test/kv_bench` 压测、`test/kv_consistency` 一致性校验。

## 压测数据

### 压测口径（统一参数）

默认对比口径与 `bench_nightly.sh` 保持一致：

```text
./bin/kv_bench -c test.conf -t 8 -s 15 -k 10000 -v 64 -w 10
```

含义：**8 线程**、key 空间 **10000**、value **64B**、写比例 **10%**。  
门槛：`errors=0` 视为正确性通过；QPS/p50/p99 作为性能观测指标。

### 当前优化方案（`feat/perf-apply-wait`）

相对 `main`，本轮性能优化集中在“去轮询、改事件驱动、减少无效重试”：

1. **KV 等待 apply 改条件变量唤醒**
   - `KvServer::WaitApplied()` 从固定 sleep 轮询改为 `m_applyCv.wait_for(...)`。
   - apply 日志/快照落地后调用 `notifyAppliedProgress()` 及时唤醒等待读请求。
2. **Raft applier 改事件驱动**
   - 新增 `m_applyCommitCv` 与 `wakeApplier()`。
   - `commitIndex` 推进或快照安装后立即唤醒 applier，减少空转等待。
3. **读路径减少额外排队**
   - `RequestReadIndex()` 去掉批处理前 `sleepNMilliseconds(1)`。
4. **Clerk 失败路径治理**
   - RPC 失败退避（上限 80ms）+ 不可达节点短暂跳过。
   - gRPC 失败日志 5s 限流，降低日志刷屏对压测的干扰。

### 当前结果与阶段性成果

#### 1) GitHub runner（手动触发 `Bench Nightly`）

最近一次 `feat/perf-apply-wait` 实测：

```text
ops=40273 errors=0 qps=2684.87
latency_us: avg=2978.8 p50=970.0 p99=23230.4
```

解读：中位延迟约 **0.97ms**，尾延迟约 **23.2ms**，正确性门槛 `errors=0` 满足。

#### 2) 维护者云机历史基线（优化前，供对比）

旧基线（同参数、15s）稳定区间：

| 指标 | 历史基线（第 2～5 轮） |
|------|------------------------|
| **QPS** | **1250.73 – 1278.20** |
| **错误数** | **0** |
| **p50** | **约 2.2 ms**（约 2200 µs） |
| **平均延迟** | **约 6.3 – 6.4 ms** |
| **p99** | **约 35 – 36 ms** |

> 说明：不同机器（GitHub runner / 云机）不可直接做绝对值对比，但趋势上可见本轮优化显著改善吞吐与延迟分布。

### 定时压测（GitHub Actions）

每周日在 GitHub Ubuntu runner 自动运行 [`bench-nightly.yml`](.github/workflows/bench-nightly.yml)：

- 查看结果：**[Actions → Bench Nightly](https://github.com/dttilx/raftKv/actions/workflows/bench-nightly.yml)** → 最新 Run → Summary 或下载 artifact。
- 失败条件：仅 `errors != 0`。
- 角色定位：用于持续跟踪性能趋势，不作为 PR 合并硬门禁。

## 目录结构

- `src/raftCore`：Raft、持久化、KvServer。
- `src/raftRpcPro`：仅保留 `.proto`；`.pb.cc` / `.grpc.pb.*` 在构建目录 **`cmake-build/src/raftRpcPro/generated/`** 生成，不提交。
- `src/raftClerk`：客户端与 RPC 封装。
- `src/common`：配置、工具等。
- `src/skipList/include`：跳表（头文件实现），作 KV 状态机存储。
- `example`：`raftCoreRun`（集群入口）、`callerMain` 示例客户端。
- `test`：`kv_consistency`、`kv_bench`。
- `docs`：架构与一致性读说明。

## 构建

依赖：**CMake ≥ 3.22**、**C++20**、**protobuf**、**gRPC**、**Boost.serialization**、**pthread**、**dl**。当前实现**不依赖 Muduo**。

每个 `src/raftRpcPro/*.proto` 须包含：

```text
option cc_generic_services = false;
```

否则 `grpc_cpp_plugin` 可能报 generic service 相关错误。

```bash
rm -rf cmake-build
cmake -S . -B cmake-build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build cmake-build -j
```

产物：可执行文件在 `bin/`，静态库在 `lib/`。

## 运行

### 推荐：固定端口 + 脚本（本地 / CI 共用）

依赖 **Linux**，在项目根目录：

```bash
chmod +x scripts/*.sh
./scripts/cluster_up.sh          # 使用 deploy/test.conf.fixed → test.conf，端口 19001–19003
./bin/kv_consistency -c test.conf -k 1000 -r 1
./scripts/cluster_down.sh
```

一键冒烟（起集群 → 一致性 → 短压测 `errors=0` → 关集群）：

```bash
./scripts/smoke.sh
```

杀单节点后再测一致性：

```bash
./scripts/smoke_chaos.sh
```

`raftCoreRun` 使用已有配置（不随机覆盖端口）：

```bash
cp deploy/test.conf.fixed test.conf
./bin/raftCoreRun -f test.conf -u
```

### 开发模式：随机端口

`raftCoreRun` 会**生成** `test.conf`（`127.0.0.1` 上随机端口），再 `fork` 子进程：

```bash
./bin/raftCoreRun -n 3 -f test.conf
```

**另开终端**，在同一目录：

```bash
./bin/callerMain
```

停止：`./scripts/cluster_down.sh`，或 `Ctrl+C` / `pkill -f raftCoreRun`。

压测 / 一致性（集群已启动后）：

```bash
./bin/kv_consistency -c test.conf -k 1000 -r 3
./bin/kv_bench -c test.conf -t 8 -s 15 -k 10000 -v 64 -w 10
```

### CI 与定时任务

| 工作流 | 触发 | 内容 |
|--------|------|------|
| [**CI**](.github/workflows/ci.yml) | `push` / `PR` | 编译 → `smoke.sh`（一致性 + 5s 压测）→ `smoke_chaos.sh` |
| [**Bench Nightly**](.github/workflows/bench-nightly.yml) | 每周日 UTC 03:00 / 手动 **Run workflow** | 编译 → `bench_nightly.sh`（8 线程 × **15 s**，README 同参） |
| [**Chaos Nightly**](.github/workflows/chaos-nightly.yml) | 每周日 UTC 03:30 / 手动 **Run workflow** | 编译 → `chaos_runner.sh`（故障注入 + 前后一致性校验 + JSON/Markdown 报告） |

PR 门禁以 **CI** 为准；**Bench Nightly** 用于跟踪长跑 QPS/延迟趋势，失败仅当 `errors≠0`。

### Chaos Testing

最小可用闭环：故障前一致性检查 → 注入故障 → 故障后一致性检查，并输出报告。

```bash
chmod +x scripts/*.sh
./scripts/chaos_runner.sh --scenario kill-restart --rounds 3 --keys 400 --sleep 6
```

默认产物：

- `logs/chaos_report.json`
- `logs/chaos_summary.md`
- `logs/chaos_round_*_baseline.log`
- `logs/chaos_round_*_post.log`

示例结果（`kill-restart`）：

```text
chaos_runner OK rounds=3 pass=3 report=logs/chaos_report.json summary=logs/chaos_summary.md
```

更多参数与场景说明见：[`docs/chaos-testing.md`](docs/chaos-testing.md)。

### 单测覆盖范围（当前）

`raftkv_unit_tests` 目前分三层：

- 基础数据结构与序列化：
  - `skip_list_test.cpp`
  - `op_test.cpp`
  - `persister_test.cpp`
- Clerk 策略逻辑（纯逻辑）：
  - `clerk_policy_test.cpp`（`ErrWrongLeader` 解析、节点选路、退避上限）
- Raft/KV 性能关键路径（组件级）：
  - `raft_apply_path_test.cpp`（commit 推进与 applier 唤醒）
  - `kv_read_wait_test.cpp`（`WaitApplied` 唤醒/超时行为）

说明：这些测试用于保护本次 `feat/perf-apply-wait` 的事件驱动优化，防止后续回归到固定轮询等待。

生产形态多机部署通常需要每节点独立进程与启动编排；当前脚本面向 **本机 fork + 固定端口** 的可复现演示与自动化测试。

## Ubuntu 依赖示例

发行版包名可能略有差异：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config \
  libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc \
  libboost-serialization-dev
```

请保证 `protoc` 与 `grpc_cpp_plugin`、libprotobuf 版本匹配（同一大版本族为宜）。

## 常见问题

### 曾出现紫色乱码刷 `leaderHearBeatTicker` 等

旧版本在定时器路径上打印了损坏编码的调试信息；已在 `3f1dc32` 移除。若仍出现：

1. `git pull`
2. 删除 `cmake-build`、`bin`、`lib`（或至少删掉 `bin/raftCoreRun`）后重新编译。

### 启动时 `RequestVote` / `failed to connect`（gRPC code 14）

旧版父进程在每次 `fork` 后 `sleep(1)`，导致后启动的节点晚 1～2 秒才监听，先启动的节点已开始选举。现版已改为子进程内错峰 `Start()`（见 `example/raftCoreExample/raftKvDB.cpp`）。请拉最新 `main` 并重编。极慢机器上若仍有零星失败，稍等片刻，Raft 会重试选举。

### 压测时终端刷屏 `[grpc][Get] code=14`（failed to connect）

**含义**：Clerk 连不上 `test.conf` 里某个端口（常见为 `19002` / `19003`），gRPC 14 = 服务不可用。

**常见原因**：

1. **未先起集群**或 `cluster_up` 未出现 `cluster is ready` 就运行 `kv_bench`。
2. **只有部分节点在跑**（另两个子进程启动失败）→ 先 `./scripts/check_cluster.sh`，再看 `logs/cluster.log`。
3. 旧版 Clerk **失败即死循环重试且每条都打日志**；新版已加退避、不可达节点短暂跳过、日志 5s 限流。

**推荐顺序**：

```bash
./scripts/cluster_down.sh
./scripts/cluster_up.sh    # 等到 cluster is ready
./scripts/check_cluster.sh
./bin/kv_bench -c test.conf -t 8 -s 15 -k 10000 -v 64 -w 10
```

调试时可设 `RAFTKV_VERBOSE_GRPC=1` 查看每一次 RPC 失败。

一键诊断（进程、端口、`cluster.log`）：

```bash
./scripts/diagnose_cluster.sh
```

说明：默认用 **nohup → `logs/cluster.log`**，**不必** `journalctl`（除非你把节点做成 systemd 服务）。

### 跳表调试输出

跳表热路径日志默认关闭。需要时用编译选项加入 `-DSKIP_LIST_TRACE=1`。

## 清理本地产物

构建目录、运行生成的 `test.conf`、持久化文件等（一级垃圾）可一键删除：

```bash
chmod +x scripts/*.sh
./scripts/clean.sh
cmake -S . -B cmake-build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build cmake-build -j
```

## 说明

- 上述路径均已写入 `.gitignore`，不应提交到 Git。

---

*压测与运行说明由维护者维护；欢迎提 issue/PR。*
