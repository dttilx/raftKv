# KVstorageBaseRaft-cpp

A C++20 Raft-based key-value storage project: Raft core, KV service, gRPC/protobuf RPC, client clerk, examples, and benchmarks.

## Features

- Raft leader election, log replication, commit/apply loop, persistence, and snapshot support.
- KV operations over Raft: `Get`, `Put`, and `Append`.
- gRPC/protobuf RPC layer.
- ReadIndex-style linearizable read path for `Get`.
- Benchmark (`kv_bench`) and consistency check (`kv_consistency`) under `test/`.

## Benchmarks (maintainer-measured)

The tables below are **measured by the repository maintainer** on a **Tencent Cloud Ubuntu VM** running a **3-node** local cluster (`./bin/raftCoreRun -n 3 -f test.conf`, peers on `127.0.0.1` with consecutive ports, e.g. `15914–15916`). Load generator:

```text
./bin/kv_bench -c test.conf -t 8 -k 10000 -v 64 -w 10
```

Meaning: **8 threads**, key space **10 000**, value size **64 B**, **10 % Put / 90 % Get**. Figures are **indicative only** (your CPU, noisy neighbors, and tuning will differ).

### Plan A — five rounds × **15 s** each

After warm-up, **rounds 2–5** stayed in a narrow band; **errors = 0** on every round.

| Metric | Observed (rounds 2–5) |
|--------|------------------------|
| **QPS** | **1 250.73 – 1 278.20** |
| **Errors** | **0** |
| **p50** | **~2.2 ms** (~2 200 µs) |
| **Average** | **~6.3 – 6.4 ms** |
| **p99** | **~35 – 36 ms** |

**Takeaway:** Short bursts hold **~1.25k–1.28k QPS** with stable median latency and **~35–36 ms** tails on this VM.

### Plan B — three rounds × **60 s** each

Longer runs show **slight QPS decay** vs Plan A (GC, log growth, cache effects on a single VM); **errors = 0** for all rounds.

| Round | Ops | QPS | Avg latency (µs) | p50 (µs) | p99 (µs) |
|------:|----:|----:|-----------------:|---------:|---------:|
| 1 | 69 343 | **1 155.72** | 6 921.8 | 2 194.0 | 39 487.6 |
| 2 | 60 301 | **1 005.02** | 7 960.0 | 2 255.0 | 45 775.0 |
| 3 | 59 635 | **993.92** | 8 048.0 | 2 300.0 | ~46 200* |

\*Round 3 p99 was truncated in the terminal capture; order of magnitude matches round 2.

**Takeaway:** **~1.0k–1.16k QPS** over 60 s windows, **0** benchmark errors; average latency drifts toward **~8 ms** while p50 stays **~2.2–2.3 ms**.

## Layout

- `src/raftCore`: Raft core, persister, and KV server.
- `src/raftRpcPro`: `.proto` only; `.pb.cc` / `.grpc.pb.*` are **generated** under `cmake-build/src/raftRpcPro/generated/` (not committed).
- `src/raftClerk`: client-side clerk and RPC utilities.
- `src/common`: shared config and utility code.
- `src/skipList/include`: header-only skip list for the KV state machine.
- `example`: `raftCoreRun` (cluster entry) and `callerMain` client sample.
- `test`: `kv_consistency`, `kv_bench`.
- `docs`: architecture and design notes.

## Build

Dependencies: CMake ≥ 3.22, C++20, **protobuf**, **gRPC**, **Boost** (serialization), **pthread**, **dl**. **Muduo is not used** by the current code path.

Each `src/raftRpcPro/*.proto` must contain:

```text
option cc_generic_services = false;
```

Otherwise `grpc_cpp_plugin` fails with generic-service errors.

```bash
rm -rf cmake-build
cmake -S . -B cmake-build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build cmake-build -j
```

Outputs: executables in `bin/`, static libs in `lib/`.

## Run

`raftCoreRun` **generates** `test.conf` (random ports on 127.0.0.1), then forks one process per node:

```bash
./bin/raftCoreRun -n 3 -f test.conf
```

In another terminal (same directory so `test.conf` matches):

```bash
./bin/callerMain
```

Stop: `Ctrl+C` or `pkill -f raftCoreRun`.

To run benchmarks after the cluster is up:

```bash
./bin/kv_consistency -c test.conf -k 1000 -r 3
./bin/kv_bench -c test.conf -t 8 -s 15 -k 10000 -v 64 -w 10
```

### Optional: fixed `test.conf`

You can hand-write:

```text
node0ip=127.0.0.1
node0port=8000
node1ip=127.0.0.1
node1port=8001
node2ip=127.0.0.1
node2port=8002
```

Production-style multi-process deployment would need separate binaries or launchers; the stock `raftCoreRun` is oriented around local fork + dynamic ports.

## Ubuntu dependencies (typical)

Adjust names if your distro differs:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config \
  libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc \
  libboost-serialization-dev
```

Ensure `protoc` and `grpc_cpp_plugin` versions are compatible (same major gRPC/protobuf family).

## Troubleshooting

### Garbled purple logs: `123 leaderHearBeatTicker(); 鍑芥暟…`

Older builds printed **every Raft timer sleep** using broken UTF-8 strings. That spam was **removed** in commit `3f1dc32`. If you still see it:

1. `git pull`
2. `rm -rf cmake-build bin lib` (or at least delete `bin/raftCoreRun`)
3. Re-run the **Build** commands above so `raftCoreRun` is re-linked.

The program was not necessarily “stuck”; timers run in a loop by design—the output was misleading debug noise.

### `RequestVote` / `failed to connect to all addresses` (gRPC code 14) at startup

Earlier `raftCoreRun` called `sleep(1)` in the **parent** after each `fork`, so child 0 started **one to two seconds** before the last child. Node 0 then ran elections and opened gRPC client calls before peer `N-1` had called `AddListeningPort`. Fix: fork children without that delay and stagger `KvServer::Start()` inside each child so higher-index peers bind first (`raftKvDB.cpp`). Pull latest `main` and rebuild.

If you still see a few transient failures on a very slow host, wait a second and watch for steady `grpc server started` lines for all nodes; Raft retries elections.

### Skip-list trace

KV lookups can print verbose skip-list traces; they are **off** by default. To enable, build with e.g. `-DSKIP_LIST_TRACE=1` on the compiler command line.

## Notes

- Runtime files `raftstatePersist*.txt`, `snapshotPersist*.txt` are git-ignored.
- Build dirs `cmake-build/`, `build/` and `bin/`, `lib/` are git-ignored.
- Some legacy comments contain mojibake; they can be cleaned without behavior changes.
