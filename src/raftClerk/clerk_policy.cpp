#include "clerk_policy.h"

#include <algorithm>
#include <cstdlib>

#include "util.h"

namespace raftkv_clerk_policy {

bool ParseWrongLeaderHint(const std::string& err, int serverCount, int* leaderId) {
  *leaderId = -1;
  if (err == ErrWrongLeader) {
    return true;
  }

  const std::string prefix = ErrWrongLeader + ":";
  if (err.rfind(prefix, 0) != 0) {
    return false;
  }

  int parsed = std::atoi(err.substr(prefix.size()).c_str());
  if (parsed >= 0 && parsed < serverCount) {
    *leaderId = parsed;
  }
  return true;
}

int ComputeRpcBackoffMs(int consecutiveRpcFailures) {
  return std::min(5 * consecutiveRpcFailures, 80);
}

int PickNextServerFromView(int current, bool rpcOk, const std::string& err, const std::vector<bool>& reachable) {
  const int serverCount = static_cast<int>(reachable.size());
  if (serverCount <= 0) {
    return 0;
  }

  if (rpcOk) {
    int leaderId = -1;
    if (ParseWrongLeaderHint(err, serverCount, &leaderId) && leaderId >= 0 && reachable[static_cast<size_t>(leaderId)]) {
      return leaderId;
    }
  }

  for (int step = 1; step <= serverCount; ++step) {
    const int next = (current + step) % serverCount;
    if (reachable[static_cast<size_t>(next)]) {
      return next;
    }
  }
  return (current + 1) % serverCount;
}

}  // namespace raftkv_clerk_policy
