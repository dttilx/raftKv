//
// Created by swx on 23-12-28.
//

#include "raftRpcUtil.h"

#include <chrono>
#include <iostream>
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

bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response) {
  grpc::ClientContext context;
  SetRpcDeadline(&context);
  grpc::Status status = stub_->AppendEntries(&context, *args, response);
  LogRpcFailure(m_target, "AppendEntries", status);
  return status.ok();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response) {
  grpc::ClientContext context;
  SetRpcDeadline(&context);
  grpc::Status status = stub_->InstallSnapshot(&context, *args, response);
  LogRpcFailure(m_target, "InstallSnapshot", status);
  return status.ok();
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response) {
  grpc::ClientContext context;
  SetRpcDeadline(&context);
  grpc::Status status = stub_->RequestVote(&context, *args, response);
  LogRpcFailure(m_target, "RequestVote", status);
  return status.ok();
}

//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动

RaftRpcUtil::RaftRpcUtil(const std::string& ip, short port) {
  m_target = ip + ":" + std::to_string(port);
  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(m_target, grpc::InsecureChannelCredentials());
  stub_ = raftRpcProctoc::raftRpc::NewStub(channel);
}

RaftRpcUtil::~RaftRpcUtil() = default;
