# KVstorageBaseRaft-cpp

A C++20 Raft-based key-value storage project: Raft core, KV service, gRPC/protobuf RPC, client clerk, examples, and benchmarks.

## Features

- Raft leader election, log replication, commit/apply loop, persistence, and snapshot support.
- KV operations over Raft: `Get`, `Put`, and `Append`.
- gRPC/protobuf RPC layer.
- ReadIndex-style linearizable read path for `Get`.
- Benchmark (`kv_bench`) and consistency check (`kv_consistency`) under `test/`.

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

## Notes

- Runtime files `raftstatePersist*.txt`, `snapshotPersist*.txt` are git-ignored.
- Build dirs `cmake-build/`, `build/` and `bin/`, `lib/` are git-ignored.
- Some legacy comments contain mojibake; they can be cleaned without behavior changes.
