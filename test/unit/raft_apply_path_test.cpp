#include "raft.h"

#include <gtest/gtest.h>

#include <chrono>
#include <mutex>
#include <thread>

namespace {

raftRpcProctoc::LogEntry MakeLogEntry(int index, int term) {
  raftRpcProctoc::LogEntry e;
  e.set_logindex(index);
  e.set_logterm(term);
  e.set_command("cmd-" + std::to_string(index));
  return e;
}

TEST(RaftApplyPathTest, WakeApplierNotifiesWaitingThreadWhenLagging) {
  Raft raft;
  raft.TestSetCoreState(/*currentTerm=*/1, /*commitIndex=*/2, /*lastApplied=*/1,
                        /*lastSnapshotIncludeIndex=*/0, /*lastSnapshotIncludeTerm=*/0);

  std::mutex mu;
  std::chrono::milliseconds waited{0};

  std::thread waiter([&]() {
    std::unique_lock<std::mutex> lk(mu);
    const auto start = std::chrono::steady_clock::now();
    raft.TestApplyCommitCv().wait_for(lk, std::chrono::milliseconds(200));
    waited = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  raft.TestWakeApplier();
  waiter.join();
  EXPECT_LT(waited.count(), 120);
}

TEST(RaftApplyPathTest, LeaderUpdateCommitIndexAdvancesOnMajorityAndCurrentTerm) {
  Raft raft;
  raft.TestSetCoreState(/*currentTerm=*/3, /*commitIndex=*/1, /*lastApplied=*/1,
                        /*lastSnapshotIncludeIndex=*/0, /*lastSnapshotIncludeTerm=*/0);
  raft.TestSetPeersAndMatchIndex(/*peerCount=*/3, {0, 4, 4});
  raft.TestSetLogs({MakeLogEntry(1, 1), MakeLogEntry(2, 2), MakeLogEntry(3, 3), MakeLogEntry(4, 3)});

  raft.TestLeaderUpdateCommitIndex();
  EXPECT_EQ(raft.TestCommitIndex(), 4);
}

TEST(RaftApplyPathTest, LeaderUpdateCommitIndexKeepsOldValueWhenTermMismatched) {
  Raft raft;
  raft.TestSetCoreState(/*currentTerm=*/5, /*commitIndex=*/1, /*lastApplied=*/1,
                        /*lastSnapshotIncludeIndex=*/0, /*lastSnapshotIncludeTerm=*/0);
  raft.TestSetPeersAndMatchIndex(/*peerCount=*/3, {0, 4, 4});
  raft.TestSetLogs({MakeLogEntry(1, 1), MakeLogEntry(2, 2), MakeLogEntry(3, 2), MakeLogEntry(4, 2)});

  raft.TestLeaderUpdateCommitIndex();
  EXPECT_EQ(raft.TestCommitIndex(), 1);
}

}  // namespace
