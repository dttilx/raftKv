// =============================================================================
// 文件：persister_test.cpp
// 被测模块：src/raftCore/Persister.cpp（Raft 状态与快照的本地文件持久化）
// 测试类型：单元测试（真实写临时文件，无 gRPC / 无多节点）
// 隔离方式：每个用例独占 nodeId（从 90000 递增），TearDown 删除
//             raftstatePersist{me}.txt / snapshotPersist{me}.txt
// 运行方式：./bin/raftkv_unit_tests  或  cd cmake-build && ctest
// -----------------------------------------------------------------------------
// 用例一览：
//   PersisterTest.SaveRaftStateReadBack
//     - SaveRaftState 后 ReadRaftState 内容一致，RaftStateSize 等于字节长度
//   PersisterTest.SaveStateAndSnapshotTogether
//     - Save(raftState, snapshot) 同时持久化，两者各自可读回
//   PersisterTest.SaveOverwritesPreviousRaftState
//     - 第二次 SaveRaftState 覆盖第一次，旧内容不再读出
//   PersisterTest.ReadEmptySnapshotBeforeSave
//     - 新建 Persister 未写快照时，ReadSnapshot 返回空串
// =============================================================================

#include <gtest/gtest.h>

#include "Persister.h"
#include <atomic>
#include <filesystem>
#include <string>

namespace {

std::atomic<int> g_nextPersisterMe{90000};

class PersisterTest : public ::testing::Test {
 protected:
  void SetUp() override { me_ = g_nextPersisterMe.fetch_add(1); }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove(RaftStatePath(), ec);
    std::filesystem::remove(SnapshotPath(), ec);
  }

  std::string RaftStatePath() const { return "raftstatePersist" + std::to_string(me_) + ".txt"; }

  std::string SnapshotPath() const { return "snapshotPersist" + std::to_string(me_) + ".txt"; }

  int me_{0};
};

TEST_F(PersisterTest, SaveRaftStateReadBack) {
  Persister persister(me_);
  const std::string state = "raft-state-v1";
  persister.SaveRaftState(state);

  EXPECT_EQ(persister.RaftStateSize(), static_cast<long long>(state.size()));
  EXPECT_EQ(persister.ReadRaftState(), state);
}

TEST_F(PersisterTest, SaveStateAndSnapshotTogether) {
  Persister persister(me_);
  const std::string state = "combined-raft-state";
  const std::string snapshot = "snapshot-bytes";
  persister.Save(state, snapshot);

  EXPECT_EQ(persister.ReadRaftState(), state);
  EXPECT_EQ(persister.ReadSnapshot(), snapshot);
  EXPECT_EQ(persister.RaftStateSize(), static_cast<long long>(state.size()));
}

TEST_F(PersisterTest, SaveOverwritesPreviousRaftState) {
  Persister persister(me_);
  persister.SaveRaftState("old-state");
  persister.SaveRaftState("new-state");

  EXPECT_EQ(persister.ReadRaftState(), "new-state");
  EXPECT_EQ(persister.RaftStateSize(), static_cast<long long>(std::string("new-state").size()));
}

TEST_F(PersisterTest, ReadEmptySnapshotBeforeSave) {
  Persister persister(me_);
  EXPECT_TRUE(persister.ReadSnapshot().empty());
}

}  // namespace
