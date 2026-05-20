#include "clerk.h"
#include "util.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

struct BenchOpts {
  // 连接配置文件：由集群启动程序生成，包含 node0ip/node0port... 等键值。
  std::string configFile = "test.conf";
  // 并发线程数：每个线程内部持有一个独立的 Clerk（各自维护 recentLeaderId/requestId 等）。
  int threads = 4;
  // 压测时长（秒）：每个线程循环执行读/写直到 deadline。
  int seconds = 10;
  // key 的取值空间大小：key 为 "k<rand>"，rand ∈ [0, keySpace-1]；
  // keySpace 越小越“热点”，越大越“均匀分布”。
  int keySpace = 1000;
  // Put 写入 value 的大小（字节）：模拟不同 payload 下的序列化/拷贝/RPC 开销。
  int valueBytes = 16;
  // Put 的百分比（0-100），剩余为 Get。
  int writePercent = 50;  // Put 占比，其余为 Get
};

static void usage(const char* argv0) {
  std::fprintf(
      stderr,
      "Usage: %s [-c config] [-t threads] [-s seconds] [-k keySpace] [-v valueBytes] [-w writePercent]\n",
      argv0);
  std::fprintf(stderr, "Example: %s -c test.conf -t 8 -s 15 -k 10000 -v 64 -w 10\n", argv0);
}

static BenchOpts parseArgs(int argc, char** argv) {
  BenchOpts o;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto needVal = [&](const char* flag) {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "missing value for %s\n", flag);
        usage(argv[0]);
        std::exit(2);
      }
      return std::string(argv[++i]);
    };
    if (a == "-c") {
      o.configFile = needVal("-c");
    } else if (a == "-t") {
      o.threads = std::atoi(needVal("-t").c_str());
    } else if (a == "-s") {
      o.seconds = std::atoi(needVal("-s").c_str());
    } else if (a == "-k") {
      o.keySpace = std::atoi(needVal("-k").c_str());
    } else if (a == "-v") {
      o.valueBytes = std::atoi(needVal("-v").c_str());
    } else if (a == "-w") {
      o.writePercent = std::atoi(needVal("-w").c_str());
    } else if (a == "-h" || a == "--help") {
      usage(argv[0]);
      std::exit(0);
    } else {
      std::fprintf(stderr, "unknown arg: %s\n", a.c_str());
      usage(argv[0]);
      std::exit(2);
    }
  }
  o.threads = std::max(1, o.threads);
  o.seconds = std::max(1, o.seconds);
  o.keySpace = std::max(1, o.keySpace);
  o.valueBytes = std::max(0, o.valueBytes);
  o.writePercent = std::min(100, std::max(0, o.writePercent));
  return o;
}

static std::string makeValue(int n, std::mt19937& gen) {
  static const char alphabet[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<int> dis(0, (int)sizeof(alphabet) - 2);
  std::string s;
  s.resize((size_t)n);
  for (int i = 0; i < n; ++i) s[(size_t)i] = alphabet[dis(gen)];
  return s;
}

static double percentile(std::vector<int64_t>& v, double p) {
  // 计算延迟分位数。这里对所有请求的 latency 样本排序，再做线性插值取 p 分位。
  if (v.empty()) return 0.0;
  std::sort(v.begin(), v.end());
  double idx = (p / 100.0) * (double)(v.size() - 1);
  size_t lo = (size_t)idx;
  size_t hi = std::min(v.size() - 1, lo + 1);
  double frac = idx - (double)lo;
  return (1.0 - frac) * (double)v[lo] + frac * (double)v[hi];
}

int main(int argc, char** argv) {
  BenchOpts o = parseArgs(argc, argv);

  // stop 用于支持后续扩展（例如信号中断）；当前主要由 deadline 控制退出。
  std::atomic<bool> stop{false};
  // ops 统计“客户端完成的请求次数”（每次 Put/Get 返回即 +1），用于计算 QPS=ops/seconds。
  std::atomic<int64_t> ops{0};
  // errOps 仅统计 C++ 异常。注意：底层 RPC 失败后若 Clerk 重试成功，不一定会计入错误。
  std::atomic<int64_t> errOps{0};

  // 每个线程维护自己的延迟样本（单位：微秒），避免多线程写同一个 vector 的锁竞争。
  std::vector<std::vector<int64_t>> threadLatUs((size_t)o.threads);
  std::vector<std::thread> workers;
  workers.reserve((size_t)o.threads);

  // 所有线程共享同一个“截至时间”，到点后停止发请求，保证压测时长接近 seconds。
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(o.seconds);

  for (int ti = 0; ti < o.threads; ++ti) {
    workers.emplace_back([&, ti]() {
      Clerk c;
      c.Init(o.configFile);

      // 每个线程独立随机源：key 随机、读写随机、value 随机。
      std::mt19937 gen((unsigned)std::chrono::steady_clock::now().time_since_epoch().count() ^ (unsigned)ti);
      std::uniform_int_distribution<int> keyDis(0, o.keySpace - 1);
      std::uniform_int_distribution<int> opDis(0, 99);
      auto& lats = threadLatUs[(size_t)ti];
      lats.reserve(20000);

      while (!stop.load(std::memory_order_relaxed)) {
        if (std::chrono::steady_clock::now() >= deadline) break;

        // key 为 "k<rand>"：rand 的范围由 keySpace 决定（热点/均匀）。
        int k = keyDis(gen);
        std::string key = "k" + std::to_string(k);
        // doWrite=true 表示发 Put，否则发 Get；概率由 writePercent 控制。
        bool doWrite = opDis(gen) < o.writePercent;

        // 端到端延迟：从调用 Put/Get 到返回（包含 RPC + Raft 共识等待 + 状态机执行）。
        auto t0 = std::chrono::steady_clock::now();
        try {
          if (doWrite) {
            auto val = makeValue(o.valueBytes, gen);
            c.Put(key, val);
          } else {
            (void)c.Get(key);
          }
        } catch (...) {
          errOps.fetch_add(1, std::memory_order_relaxed);
        }
        auto t1 = std::chrono::steady_clock::now();
        int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        lats.push_back(us);
        ops.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& th : workers) th.join();
  stop.store(true);

  // 合并所有线程的延迟样本，用于计算整体的 avg/p50/p99。
  std::vector<int64_t> all;
  size_t total = 0;
  for (auto& v : threadLatUs) total += v.size();
  all.reserve(total);
  for (auto& v : threadLatUs) all.insert(all.end(), v.begin(), v.end());

  double p50 = percentile(all, 50);
  double p99 = percentile(all, 99);
  double avg = 0.0;
  if (!all.empty()) {
    avg = (double)std::accumulate(all.begin(), all.end(), (int64_t)0) / (double)all.size();
  }
  // 这里的 QPS 是“客户端完成次数/持续时间”，用于横向对比不同配置；
  // 若需要更严格的“提交吞吐”（只统计写提交成功、或区分 Get/Put），需要在此处细分计数器。
  double qps = (double)ops.load() / (double)o.seconds;

  std::printf("kv_bench result\n");
  std::printf("  config=%s threads=%d seconds=%d keySpace=%d valueBytes=%d writePercent=%d\n",
              o.configFile.c_str(), o.threads, o.seconds, o.keySpace, o.valueBytes, o.writePercent);
  std::printf("  ops=%lld errors=%lld qps=%.2f\n", (long long)ops.load(), (long long)errOps.load(), qps);
  std::printf("  latency_us: avg=%.1f p50=%.1f p99=%.1f\n", avg, p50, p99);
  return 0;
}

