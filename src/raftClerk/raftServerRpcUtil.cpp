//
// Created by swx on 24-1-4.
//
#include "raftServerRpcUtil.h"

#include <chrono>

#include "config.h"
#include "grpc_log_util.h"

namespace {

void SetRpcDeadline(grpc::ClientContext* context) {
  context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(RPC_CALL_TIMEOUT));
}

}  // namespace

raftServerRpcUtil::raftServerRpcUtil(const std::string& ip, short port) {
  m_target = ip + ":" + std::to_string(port);
  m_channel = grpc::CreateChannel(m_target, grpc::InsecureChannelCredentials());
  stub = raftKVRpcProctoc::kvServerRpc::NewStub(m_channel);
}

raftServerRpcUtil::~raftServerRpcUtil() = default;

bool raftServerRpcUtil::WaitForReady(int timeoutMs) const {
  if (!m_channel) {
    return false;
  }
  const auto deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(std::max(1, timeoutMs));
  return m_channel->WaitForConnected(deadline);
}

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs* GetArgs, raftKVRpcProctoc::GetReply* reply) {
  grpc::ClientContext context;
  SetRpcDeadline(&context);
  grpc::Status status = stub->Get(&context, *GetArgs, reply);
  raftkv_grpc_log::LogRpcFailure(m_target, "Get", status);
  return status.ok();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply) {
  grpc::ClientContext context;
  SetRpcDeadline(&context);
  grpc::Status status = stub->PutAppend(&context, *args, reply);
  raftkv_grpc_log::LogRpcFailure(m_target, "PutAppend", status);
  return status.ok();
}
