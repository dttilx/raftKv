//
// Created by swx on 24-1-4.
//
#include "raftServerRpcUtil.h"

#include <chrono>
#include <string>

#include "config.h"

namespace {

void SetRpcDeadline(grpc::ClientContext* context) {
  context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(RPC_CALL_TIMEOUT));
}

void LogRpcFailure(const std::string& target, const char* method, const grpc::Status& status) {
  if (status.ok()) {
    return;
  }
  std::cerr << "[grpc][" << method << "] target=" << target << " code=" << status.error_code()
            << " message=" << status.error_message() << std::endl;
}

}  // namespace

// kvserver不同于raft节点之间，kvserver的rpc是用于clerk向kvserver调用，不会被调用，因此只用写caller功能，不用写callee功能
//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动
raftServerRpcUtil::raftServerRpcUtil(const std::string& ip, short port) {
  m_target = ip + ":" + std::to_string(port);
  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(m_target, grpc::InsecureChannelCredentials());
  stub = raftKVRpcProctoc::kvServerRpc::NewStub(channel);
}

raftServerRpcUtil::~raftServerRpcUtil() = default;

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply) {
  grpc::ClientContext context;
  SetRpcDeadline(&context);
  grpc::Status status = stub->Get(&context, *GetArgs, reply);
  LogRpcFailure(m_target, "Get", status);
  return status.ok();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  grpc::ClientContext context;
  SetRpcDeadline(&context);
  grpc::Status status = stub->PutAppend(&context, *args, reply);
  LogRpcFailure(m_target, "PutAppend", status);
  return status.ok();
}
