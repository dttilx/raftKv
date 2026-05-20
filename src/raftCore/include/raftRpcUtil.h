#ifndef RAFTRPC_H
#define RAFTRPC_H

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

#include "raftRPC.grpc.pb.h"

class RaftRpcUtil {
 private:
  std::unique_ptr<raftRpcProctoc::raftRpc::Stub> stub_;
  std::string m_target;

 public:
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* response);
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest* args,
                       raftRpcProctoc::InstallSnapshotResponse* response);
  bool RequestVote(raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* response);

  RaftRpcUtil(const std::string& ip, short port);
  ~RaftRpcUtil();
};

#endif  // RAFTRPC_H
