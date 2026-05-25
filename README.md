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

### 定时压测（GitHub Actions）

每周日在 GitHub Ubuntu runner 上自动跑与下文**相同参数**的 15 s 压测（[`bench-nightly.yml`](.github/workflows/bench-nightly.yml)）：

- 查看结果：**[Actions → Bench Nightly](https://github.com/dttilx/raftKv/actions/workflows/bench-nightly.yml)** → 最新 Run → **Summary** 或下载 **bench-nightly** 附件。
- 门禁：仅 **`errors=0`**；QPS / p50 / p99 随 runner 性能波动，**不与下方云机数值直接对比**。
- 本地/CI 同参手动跑：`./scripts/bench_nightly.sh`（需先 `cluster_up` 或由脚本自动启停）。

### 维护者云机实测（历史参考）

下列为**腾讯云 Ubuntu 云主机**上实测（旧版随机端口集群；现推荐 `./scripts/cluster_up.sh` + 固定 `19001–19003`）。压测命令：

```text
./bin/kv_bench -c test.conf -t 8 -s 15 -k 10000 -v 64 -w 10
```

含义：**8 线程**、key 空间 **10000**、value **64 字节**、**10% Put / 90% Get**。数值随 CPU、机器负载与参数变化，**仅供参考**。

### 方案 A：5 轮 × 每轮 **15 s**

热身后 **第 2～5 轮** 波动范围较窄，各轮 **errors = 0**。

| 指标 | 观测（第 2～5 轮） |
|------|-------------------|
| **QPS** | **1250.73 – 1278.20** |
| **错误数** | **0** |
| **p50** | **约 2.2 ms**（约 2200 µs） |
| **平均延迟** | **约 6.3 – 6.4 ms** |
| **p99** | **约 35 – 36 ms** |

**小结：** 短跑约 **1.25k–1.28k QPS**，中位延迟稳定，尾延迟约 **35–36 ms**。

### 方案 B：3 轮 × 每轮 **60 s**

相对方案 A，长跑下 QPS 略降（单机 GC、日志增长、缓存等因素）；**三轮 errors 均为 0**。

| 轮次 | Ops | QPS | 平均延迟 (µs) | p50 (µs) | p99 (µs) |
|-----:|----:|----:|---------------:|---------:|---------:|
| 1 | 69 343 | **1155.72** | 6921.8 | 2194.0 | 39487.6 |
| 2 | 60 301 | **1005.02** | 7960.0 | 2255.0 | 45775.0 |
| 3 | 59 635 | **993.92** | 8048.0 | 2300.0 | ~46 200* |

\*第 3 轮 p99 在终端输出中被截断，数量级与第 2 轮相近。

**小结：** 60 s 窗口内约 **1.0k–1.16k QPS**、基准统计 **0 错误**；平均延迟向 **约 8 ms** 漂移，p50 仍约 **2.2–2.3 ms**。

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

PR 门禁以 **CI** 为准；**Bench Nightly** 用于跟踪长跑 QPS/延迟趋势，失败仅当 `errors≠0`。

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
