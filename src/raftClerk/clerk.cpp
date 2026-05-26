//
// Created by swx on 23-6-4.
//
#include "clerk.h"

#include "raftServerRpcUtil.h"

#include "util.h"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <iostream>
#include <mutex>
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

}  // namespace

bool Clerk::IsServerReachable(int server) const {
  if (server < 0 || server >= static_cast<int>(m_unhealthyUntil.size())) {
    return false;
  }
  return std::chrono::steady_clock::now() >= m_unhealthyUntil[static_cast<size_t>(server)];
}

void Clerk::MarkServerUnreachable(int server) {
  if (server < 0 || server >= static_cast<int>(m_unhealthyUntil.size())) {
    return;
  }
  m_unhealthyUntil[static_cast<size_t>(server)] =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(800);
}

void Clerk::RpcBackoff() {
  ++m_consecutiveRpcFailures;
  const int backoffMs = std::min(5 * m_consecutiveRpcFailures, 80);
  sleepNMilliseconds(backoffMs);
}

int Clerk::PickNextServer(int current, bool rpcOk, const std::string& err) {
  const int serverCount = static_cast<int>(m_servers.size());
  if (serverCount <= 0) {
    return 0;
  }

  if (rpcOk) {
    int leaderId = -1;
    if (ParseWrongLeader(err, serverCount, &leaderId) && leaderId >= 0 && IsServerReachable(leaderId)) {
      return leaderId;
    }
  } else {
    MarkServerUnreachable(current);
  }

  for (int step = 1; step <= serverCount; ++step) {
    const int next = (current + step) % serverCount;
    if (IsServerReachable(next)) {
      return next;
    }
  }
  return (current + 1) % serverCount;
}

void Clerk::CheckClusterReachableOrExit() {
  static std::once_flag once;
  std::call_once(once, [this]() {
    int reachable = 0;
    for (int i = 0; i < static_cast<int>(m_servers.size()); ++i) {
      if (m_servers[static_cast<size_t>(i)]->WaitForReady(3000)) {
        ++reachable;
      } else {
        std::cerr << "[Clerk] unreachable: " << m_servers[static_cast<size_t>(i)]->Target() << std::endl;
      }
    }
    if (reachable == 0) {
      std::cerr << "[Clerk] no KV node reachable. Start cluster first: ./scripts/cluster_up.sh\n"
                << "  then verify: ss -tlnp | grep -E '19001|19002|19003'\n"
                << "  logs: logs/cluster.log" << std::endl;
      std::exit(1);
    }
    if (reachable < static_cast<int>(m_servers.size())) {
      std::cerr << "[Clerk] warning: only " << reachable << "/" << m_servers.size()
                << " nodes reachable (fix cluster before bench; see logs/cluster.log)" << std::endl;
    }
  });
}

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
    bool ok = m_servers[static_cast<size_t>(server)]->Get(&args, &reply);
    int leaderId = -1;
    bool wrongLeader = ok && ParseWrongLeader(reply.err(), static_cast<int>(m_servers.size()), &leaderId);
    if (!ok || wrongLeader) {
      if (!ok) {
        RpcBackoff();
      }
      server = PickNextServer(server, ok, reply.err());
      continue;
    }
    m_consecutiveRpcFailures = 0;
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
    bool ok = m_servers[static_cast<size_t>(server)]->PutAppend(&args, &reply);
    int leaderId = -1;
    bool wrongLeader = ok && ParseWrongLeader(reply.err(), static_cast<int>(m_servers.size()), &leaderId);
    if (!ok || wrongLeader) {
      if (!ok) {
        RpcBackoff();
      }
      server = PickNextServer(server, ok, reply.err());
      continue;
    }
    m_consecutiveRpcFailures = 0;
    if (reply.err() == OK) {
      m_recentLeaderId = server;
      return;
    }
  }
}

void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }

void Clerk::Init(std::string configFileName) {
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
    ipPortVt.emplace_back(nodeIp, static_cast<short>(std::atoi(nodePortStr.c_str())));
  }
  for (const auto& item : ipPortVt) {
    auto rpc = std::make_shared<raftServerRpcUtil>(item.first, item.second);
    m_servers.push_back(rpc);
  }
  m_unhealthyUntil.assign(m_servers.size(), std::chrono::steady_clock::time_point{});
  CheckClusterReachableOrExit();
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
