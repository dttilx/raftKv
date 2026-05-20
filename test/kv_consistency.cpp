#include "clerk.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

struct Opts {
  std::string configFile = "test.conf";
  int keys = 1000;
  int rounds = 1;  // 重复校验轮数
};

static void usage(const char* argv0) {
  std::fprintf(stderr, "Usage: %s [-c config] [-k keys] [-r rounds]\n", argv0);
  std::fprintf(stderr, "Example: %s -c test.conf -k 2000 -r 3\n", argv0);
}

static Opts parseArgs(int argc, char** argv) {
  Opts o;
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
    } else if (a == "-k") {
      o.keys = std::atoi(needVal("-k").c_str());
    } else if (a == "-r") {
      o.rounds = std::atoi(needVal("-r").c_str());
    } else if (a == "-h" || a == "--help") {
      usage(argv[0]);
      std::exit(0);
    } else {
      std::fprintf(stderr, "unknown arg: %s\n", a.c_str());
      usage(argv[0]);
      std::exit(2);
    }
  }
  o.keys = std::max(1, o.keys);
  o.rounds = std::max(1, o.rounds);
  return o;
}

// 简单滚动 hash（不是密码学），用于跨节点/跨次对比状态机是否一致
static uint64_t fnv1a(uint64_t h, const std::string& s) {
  const uint64_t prime = 1099511628211ULL;
  for (unsigned char c : s) {
    h ^= (uint64_t)c;
    h *= prime;
  }
  return h;
}

int main(int argc, char** argv) {
  Opts o = parseArgs(argc, argv);
  Clerk c;
  c.Init(o.configFile);

  // 约定：先写入确定性数据，再重复读校验 hash（方便在故障/重启后复用该校验）
  for (int i = 0; i < o.keys; ++i) {
    std::string key = "ck" + std::to_string(i);
    std::string val = "v" + std::to_string(i);
    c.Put(key, val);
  }

  for (int r = 0; r < o.rounds; ++r) {
    uint64_t h = 1469598103934665603ULL;
    for (int i = 0; i < o.keys; ++i) {
      std::string key = "ck" + std::to_string(i);
      std::string expected = "v" + std::to_string(i);
      std::string got = c.Get(key);
      if (got != expected) {
        std::fprintf(stderr, "MISMATCH key=%s expected=%s got=%s\n", key.c_str(), expected.c_str(), got.c_str());
        return 1;
      }
      h = fnv1a(h, key);
      h = fnv1a(h, got);
    }
    std::printf("consistency round=%d keys=%d hash=0x%016llx\n", r, o.keys, (unsigned long long)h);
  }

  std::printf("consistency OK\n");
  return 0;
}

