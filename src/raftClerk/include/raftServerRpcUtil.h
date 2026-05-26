//
// Created by swx on 24-1-4.
//

#ifndef RAFTSERVERRPC_H
#define RAFTSERVERRPC_H

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include "kvServerRPC.grpc.pb.h"

/// @brief 维护当前节点对其他某一个结点的所有rpc通信，包括接收其他节点的rpc和发送
// 对于一个节点来说，对于任意其他的节点都要维护一个rpc连接，
class raftServerRpcUtil {
 private:
  std::shared_ptr<grpc::Channel> m_channel;
  std::unique_ptr<raftKVRpcProctoc::kvServerRpc::Stub> stub;
  std::string m_target;

 public:
  bool WaitForReady(int timeoutMs) const;
  const std::string& Target() const { return m_target; }
  //主动调用其他节点的三个方法,可以按照mit6824来调用，但是别的节点调用自己的好像就不行了，要继承protoc提供的service类才行

  //响应其他节点的方法
  bool Get(raftKVRpcProctoc::GetArgs* GetArgs, raftKVRpcProctoc::GetReply* reply);
  bool PutAppend(raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply);

  raftServerRpcUtil(const std::string& ip, short port);
  ~raftServerRpcUtil();
};

#endif  // RAFTSERVERRPC_H
