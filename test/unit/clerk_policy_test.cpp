#include <gtest/gtest.h>

#include <vector>

#include "clerk_policy.h"
#include "util.h"

namespace {

TEST(ClerkPolicyTest, ParseWrongLeaderHintWithLeaderId) {
  int leaderId = -1;
  EXPECT_TRUE(raftkv_clerk_policy::ParseWrongLeaderHint(ErrWrongLeader + ":2", 3, &leaderId));
  EXPECT_EQ(leaderId, 2);
}

TEST(ClerkPolicyTest, ParseWrongLeaderHintWithoutLeaderId) {
  int leaderId = 7;
  EXPECT_TRUE(raftkv_clerk_policy::ParseWrongLeaderHint(ErrWrongLeader, 3, &leaderId));
  EXPECT_EQ(leaderId, -1);
}

TEST(ClerkPolicyTest, ParseWrongLeaderHintWithInvalidFormat) {
  int leaderId = 0;
  EXPECT_FALSE(raftkv_clerk_policy::ParseWrongLeaderHint("not-wrong-leader", 3, &leaderId));
  EXPECT_EQ(leaderId, -1);
}

TEST(ClerkPolicyTest, PickNextServerPrefersHintedReachableLeader) {
  const std::vector<bool> reachable = {true, false, true};
  EXPECT_EQ(raftkv_clerk_policy::PickNextServerFromView(0, true, ErrWrongLeader + ":2", reachable), 2);
}

TEST(ClerkPolicyTest, PickNextServerSkipsUnreachableOnRpcFailure) {
  const std::vector<bool> reachable = {false, false, true};
  EXPECT_EQ(raftkv_clerk_policy::PickNextServerFromView(0, false, "", reachable), 2);
}

TEST(ClerkPolicyTest, RpcBackoffMsGrowsAndCaps) {
  EXPECT_EQ(raftkv_clerk_policy::ComputeRpcBackoffMs(1), 5);
  EXPECT_EQ(raftkv_clerk_policy::ComputeRpcBackoffMs(5), 25);
  EXPECT_EQ(raftkv_clerk_policy::ComputeRpcBackoffMs(16), 80);
  EXPECT_EQ(raftkv_clerk_policy::ComputeRpcBackoffMs(99), 80);
}

}  // namespace
