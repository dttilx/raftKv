# KVstorageBaseRaft-cpp

A C++20 Raft-based key-value storage project. The repository contains a Raft core, a KV service, gRPC/protobuf RPC definitions, a client clerk, examples, benchmark tools, and scripts for local cluster testing.

## Features

- Raft leader election, log replication, commit/apply loop, persistence, and snapshot support.
- KV operations over Raft: `Get`, `Put`, and `Append`.
- gRPC/protobuf based RPC layer.
- ReadIndex-style linearizable read path for `Get`.
- Benchmark and consistency check programs under `test/`.
- Shell scripts for cluster start/stop, fault injection, network partition testing, snapshot testing, and perf/flamegraph workflows.

## Layout

- `src/raftCore`: Raft core, persister, and KV server.
- `src/raftRpcPro`: protobuf definitions and generated RPC code.
- `src/raftClerk`: client-side clerk and RPC utilities.
- `src/common`: shared config and utility code.
- `src/skipList`: in-memory skip list used by the KV state machine.
- `example`: runnable examples.
- `test`: correctness checks and benchmark programs.
- `scripts`: helper scripts for build, cluster, fault, and performance tests.
- `docs`: architecture and design notes.

## Build

The project expects CMake, a C++20 compiler, protobuf, gRPC, Boost serialization, Muduo, pthread, and dl.

```bash
cmake -S . -B cmake-build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build cmake-build -j
```

Build outputs are configured to go to `bin/` and `lib/`.

## Run

Prepare a node config file like `bin/test.conf` with entries in this style:

```text
node0ip=127.0.0.1
node0port=8000
node1ip=127.0.0.1
node1port=8001
node2ip=127.0.0.1
node2port=8002
```

Common helper scripts:

```bash
bash scripts/cluster_start.sh
bash scripts/cluster_stop.sh
bash scripts/run_raft_correctness.sh
bash scripts/run_snapshot_suite.sh
bash scripts/workload_bench.sh
```

## Tests And Benchmarks

- `test/kv_consistency.cpp`: writes deterministic key/value data and verifies repeated reads.
- `test/kv_bench.cpp`: runs concurrent `Get`/`Put` workloads and reports QPS plus latency percentiles.

Example:

```bash
./bin/kv_consistency -c bin/test.conf -k 1000 -r 3
./bin/kv_bench -c bin/test.conf -t 8 -s 15 -k 10000 -v 64 -w 10
```

## Notes

- Runtime persistence files named `raftstatePersist*.txt` and `snapshotPersist*.txt` are ignored by git.
- Build directories such as `cmake-build/` and `build/` are ignored by git.
- Some legacy source comments may still contain mojibake and can be cleaned incrementally without changing behavior.
