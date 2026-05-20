//
// Created by swx on 23-6-4.
//
#include "clerk.h"

#include "raftServerRpcUtil.h"

#include "util.h"

#include <cstdlib>
#include <string>
#include <vector>

namespace {

bool ParseWrongLeader(const std::string& err, int serverCount, int* leaderId) {
  *leaderId = -1;
  if (err == ErrWrongLeader) {
    return true;
  }

  const std::string prefix = ErrWrongLeader + ":";
  if (err.rfind(prefix, 0) != 0) {
    return false;
  }

  int parsed = std::atoi(err.substr(prefix.size()).c_str());
  if (parsed >= 0 && parsed < serverCount) {
    *leaderId = parsed;
  }
  return true;
}

int NextServerAfterWrongLeader(const std::string& err, int currentServer, int serverCount) {
  int leaderId = -1;
  if (ParseWrongLeader(err, serverCount, &leaderId) && leaderId >= 0) {
    return leaderId;
  }
  return (currentServer + 1) % serverCount;
}

}  // namespace

std::string Clerk::Get(std::string key) {
  m_requestId++;
  auto requestId = m_requestId;
  int server = m_recentLeaderId;
  raftKVRpcProctoc::GetArgs args;
  args.set_key(key);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  while (true) {
    raftKVRpcProctoc::GetReply reply;
    bool ok = m_servers[server]->Get(&args, &reply);
    int leaderId = -1;
    bool wrongLeader = ok && ParseWrongLeader(reply.err(), static_cast<int>(m_servers.size()), &leaderId);
    if (!ok || wrongLeader) {
      server = ok ? NextServerAfterWrongLeader(reply.err(), server, static_cast<int>(m_servers.size()))
                  : (server + 1) % m_servers.size();
      continue;
    }
    if (reply.err() == ErrNoKey) {
      return "";
    }
    if (reply.err() == OK) {
      m_recentLeaderId = server;
      return reply.value();
    }
  }
  return "";
}

void Clerk::PutAppend(std::string key, std::string value, std::string op) {
  // You will have to modify this function.
  m_requestId++;
  auto requestId = m_requestId;
  auto server = m_recentLeaderId;
  while (true) {
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);
    raftKVRpcProctoc::PutAppendReply reply;
    bool ok = m_servers[server]->PutAppend(&args, &reply);
    int leaderId = -1;
    bool wrongLeader = ok && ParseWrongLeader(reply.err(), static_cast<int>(m_servers.size()), &leaderId);
    if (!ok || wrongLeader) {
      DPrintf("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}", server, server + 1,
              op.c_str());
      if (!ok) {
        DPrintf("重试原因 ，rpc失敗 ，");
      }
      if (wrongLeader) {
        DPrintf("重試原因：非leader");
      }
      server = ok ? NextServerAfterWrongLeader(reply.err(), server, static_cast<int>(m_servers.size()))
                  : (server + 1) % m_servers.size();
      continue;
    }
    if (reply.err() == OK) {  //什么时候reply errno为ok呢？？？
      m_recentLeaderId = server;
      return;
    }
  }
}

void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }
//初始化客户端
void Clerk::Init(std::string configFileName) {
  //获取所有raft节点ip、port ，并进行连接
  NodeConfig config;
  config.LoadConfigFile(configFileName.c_str());
  std::vector<std::pair<std::string, short>> ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);

    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));  //沒有atos方法，可以考慮自己实现
  }
  //进行连接
  for (const auto& item : ipPortVt) {
    std::string ip = item.first;
    short port = item.second;
    // 2024-01-04 todo：bug fix
    auto* rpc = new raftServerRpcUtil(ip, port);
    m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
  }
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
