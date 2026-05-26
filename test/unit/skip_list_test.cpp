// =============================================================================
// 文件：skip_list_test.cpp
// 被测模块：src/skipList/include/skipList.h（KV 状态机底层跳表）
// 测试类型：单元测试（纯内存、无网络、无 Raft/gRPC）
// 运行方式：./bin/raftkv_unit_tests  或  cd cmake-build && ctest
// -----------------------------------------------------------------------------
// 用例一览：
//   SkipListTest.InsertAndSearch
//     - insert_element 成功（返回 0）后，search_element 能读到相同 value，size 为 1
//   SkipListTest.SearchMissingKey
//     - 未插入的 key，search_element 返回 false
//   SkipListTest.InsertSetElementOverwrites
//     - insert_set_element 对同一 key 再次写入会覆盖旧值，size 仍为 1
//   SkipListTest.DumpAndLoadRoundTrip
//     - dump_file → load_file 后，新实例能查到原数据，条数一致（快照序列化路径）
// =============================================================================

#include <gtest/gtest.h>

#include "skipList.h"
namespace {

TEST(SkipListTest, InsertAndSearch) {
  SkipList<std::string, std::string> list(16);
  EXPECT_EQ(list.insert_element("k1", "v1"), 0);
  std::string value;
  EXPECT_TRUE(list.search_element("k1", value));
  EXPECT_EQ(value, "v1");
  EXPECT_EQ(list.size(), 1);
}

TEST(SkipListTest, SearchMissingKey) {
  SkipList<std::string, std::string> list(16);
  std::string value;
  EXPECT_FALSE(list.search_element("missing", value));
}

TEST(SkipListTest, InsertSetElementOverwrites) {
  SkipList<std::string, std::string> list(16);
  std::string k1 = "k1";
  std::string v1 = "v1";
  std::string v2 = "v2";
  list.insert_set_element(k1, v1);
  list.insert_set_element(k1, v2);
  std::string value;
  ASSERT_TRUE(list.search_element("k1", value));
  EXPECT_EQ(value, "v2");
  EXPECT_EQ(list.size(), 1);
}

TEST(SkipListTest, DumpAndLoadRoundTrip) {
  SkipList<std::string, std::string> list(16);
  list.insert_element("alpha", "1");
  list.insert_element("beta", "2");

  const std::string dumped = list.dump_file();

  SkipList<std::string, std::string> restored(16);
  restored.load_file(dumped);

  std::string value;
  EXPECT_TRUE(restored.search_element("alpha", value));
  EXPECT_EQ(value, "1");
  EXPECT_TRUE(restored.search_element("beta", value));
  EXPECT_EQ(value, "2");
  EXPECT_EQ(restored.size(), 2);
}

}  // namespace
