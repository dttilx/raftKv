#ifndef RAFTKV_CLERK_POLICY_H
#define RAFTKV_CLERK_POLICY_H

#include <string>
#include <vector>

namespace raftkv_clerk_policy {

bool ParseWrongLeaderHint(const std::string& err, int serverCount, int* leaderId);

int ComputeRpcBackoffMs(int consecutiveRpcFailures);

int PickNextServerFromView(int current, bool rpcOk, const std::string& err, const std::vector<bool>& reachable);

}  // namespace raftkv_clerk_policy

#endif
