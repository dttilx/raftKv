#include "kvServer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace {

TEST(KvReadWaitTest, WaitAppliedReturnsImmediatelyWhenAlreadyApplied) {
  KvServer kv(0, -1, "test.conf", 19001);
  kv.TestSetLastAppliedRaftLogIndex(10);
  EXPECT_TRUE(kv.TestWaitApplied(3));
}

TEST(KvReadWaitTest, WaitAppliedReturnsTrueAfterApplyProgressNotify) {
  KvServer kv(0, -1, "test.conf", 19001);
  kv.TestSetLastAppliedRaftLogIndex(0);

  std::thread applier([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    kv.TestSetLastAppliedRaftLogIndex(5);
    kv.TestNotifyAppliedProgress();
  });

  EXPECT_TRUE(kv.TestWaitApplied(5));
  applier.join();
}

TEST(KvReadWaitTest, WaitAppliedTimesOutWhenIndexDoesNotAdvance) {
  KvServer kv(0, -1, "test.conf", 19001);
  kv.TestSetLastAppliedRaftLogIndex(0);
  EXPECT_FALSE(kv.TestWaitApplied(100));
}

}  // namespace
