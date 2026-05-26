// =============================================================================
// 文件：op_test.cpp
// 被测模块：src/common/include/util.h 中的 Op（Raft 日志 / Apply 命令体）
// 测试类型：单元测试（序列化往返，不启集群）
// 说明：Op 经 Boost text_archive 编码为 string，供 Raft Start 与状态机 apply 使用
// 运行方式：./bin/raftkv_unit_tests  或  cd cmake-build && ctest
// -----------------------------------------------------------------------------
// 用例一览：
//   OpTest.RoundTripPut
//     - Put 命令 asString → parseFromString 后，Operation/Key/Value/ClientId/RequestId 全一致
//   OpTest.RoundTripGet
//     - Get 命令往返，Value 为空字符串时字段仍正确
//   OpTest.RoundTripAppend
//     - Append 命令往返，RequestId 等大字段不丢失
//   OpTest.EmptyKeyRoundTrip
//     - 空 key 的边界：序列化/反序列化不破坏字段
// =============================================================================

#include <gtest/gtest.h>

#include "util.h"
namespace {

Op MakeOp(const std::string& operation, const std::string& key, const std::string& value, int requestId) {
  Op op;
  op.Operation = operation;
  op.Key = key;
  op.Value = value;
  op.ClientId = "client-test";
  op.RequestId = requestId;
  return op;
}

TEST(OpTest, RoundTripPut) {
  const Op original = MakeOp("Put", "key-a", "value-a", 7);
  const std::string encoded = original.asString();

  Op decoded;
  ASSERT_TRUE(decoded.parseFromString(encoded));
  EXPECT_EQ(decoded.Operation, original.Operation);
  EXPECT_EQ(decoded.Key, original.Key);
  EXPECT_EQ(decoded.Value, original.Value);
  EXPECT_EQ(decoded.ClientId, original.ClientId);
  EXPECT_EQ(decoded.RequestId, original.RequestId);
}

TEST(OpTest, RoundTripGet) {
  const Op original = MakeOp("Get", "key-b", "", 3);
  Op decoded;
  ASSERT_TRUE(decoded.parseFromString(original.asString()));
  EXPECT_EQ(decoded.Operation, "Get");
  EXPECT_EQ(decoded.Key, "key-b");
  EXPECT_TRUE(decoded.Value.empty());
}

TEST(OpTest, RoundTripAppend) {
  const Op original = MakeOp("Append", "key-c", "tail", 99);
  Op decoded;
  ASSERT_TRUE(decoded.parseFromString(original.asString()));
  EXPECT_EQ(decoded.Operation, "Append");
  EXPECT_EQ(decoded.Value, "tail");
  EXPECT_EQ(decoded.RequestId, 99);
}

TEST(OpTest, EmptyKeyRoundTrip) {
  const Op original = MakeOp("Put", "", "empty-key", 1);
  Op decoded;
  ASSERT_TRUE(decoded.parseFromString(original.asString()));
  EXPECT_EQ(decoded.Key, "");
  EXPECT_EQ(decoded.Value, "empty-key");
}

}  // namespace
