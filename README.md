# KVstorageBaseRaft-cpp

基于 **C++20** 的 Raft 共识 + 分布式 KV 示例工程：Raft 核心、KV 服务、**gRPC / protobuf** RPC、客户端 Clerk、示例与压测工具。

## 功能概要

- Raft：选主、日志复制、提交与 apply 循环、持久化、快照。
- KV：经 Raft 提供 `Get`、`Put`、`Append`。
- RPC：gRPC + protobuf 双服务（节点间 Raft RPC、客户端↔KvServer）。
- 读路径：`Get` 采用 **ReadIndex 风格** 的线性一致读（见 `docs/一致性读流程.md`）。
- 工具：`test/kv_bench` 压测、`test/kv_consistency` 一致性校验。

## 压测数据（维护者实测）

下列数据为**仓库维护者在腾讯云 Ubuntu 云主机**上，使用本仓库 **`./bin/raftCoreRun -n 3 -f test.conf` 启动的三节点本机集群**（节点 `127.0.0.1` 连续端口，例如 `15914–15916`）实测得到；压测命令为：

```text
./bin/kv_bench -c test.conf -t 8 -k 10000 -v 64 -w 10
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

`raftCoreRun` 会**生成** `test.conf`（`127.0.0.1` 上随机端口），再为每个节点 `fork` 子进程：

```bash
./bin/raftCoreRun -n 3 -f test.conf
```

**另开终端**，在同一目录（保证读到同一份 `test.conf`）：

```bash
./bin/callerMain
```

停止：在该进程终端 `Ctrl+C`，或 `pkill -f raftCoreRun`。

集群已启动后的压测 / 一致性校验：

```bash
./bin/kv_consistency -c test.conf -k 1000 -r 3
./bin/kv_bench -c test.conf -t 8 -s 15 -k 10000 -v 64 -w 10
```

### 可选：手写固定 `test.conf`

```text
node0ip=127.0.0.1
node0port=8000
node1ip=127.0.0.1
node1port=8001
node2ip=127.0.0.1
node2port=8002
```

生产形态多机部署通常需要每节点独立进程与启动编排；仓库自带 `raftCoreRun` 面向 **本机 fork + 随机端口** 的快速演示。

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

## 说明

- 运行产生的 `raftstatePersist*.txt`、`snapshotPersist*.txt` 已 `.gitignore`。
- `cmake-build/`、`build/`、`bin/`、`lib/` 已忽略。

---

*压测与运行说明由维护者维护；欢迎提 issue/PR。*
