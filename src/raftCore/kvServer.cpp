#include "kvServer.h"

#include <algorithm>
#include <climits>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <utility>

#include "node_config.h"

namespace {
void UpdateMaxAtomic(std::atomic<std::uint64_t> *target, std::uint64_t value) {
  auto current = target->load();
  while (current < value && !target->compare_exchange_weak(current, value)) {
  }
}
}  // namespace

void KvServer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  DEFER {
    // for (const auto &item: m_kvDB) {
    //     DPrintf("[DBInfo ----]Key : %s, Value : %s", &item.first, &item.second);
    // }
    m_skipList.display_list();
  };
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {

  //	return
  // }
  m_mtx.lock();

  m_skipList.insert_set_element(op.Key, op.Value);

  // if (m_kvDB.find(op.Key) != m_kvDB.end()) {
  //     m_kvDB[op.Key] = m_kvDB[op.Key] + op.Value;
  // } else {
  //     m_kvDB.insert(std::make_pair(op.Key, op.Value));
  // }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  //    DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
  m_mtx.lock();
  *value = "";
  *exist = false;
  if (m_skipList.search_element(op.Key, *value)) {
    *exist = true;
  }
  // if (m_kvDB.find(op.Key) != m_kvDB.end()) {
  //     *exist = true;
  //     *value = m_kvDB[op.Key];
  // }
  m_mtx.unlock();

  if (*exist) {
    //                DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId,
    //                op.RequestId, op.Key, value)
  } else {
    //        DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId,
    //        op.RequestId, op.Key)
  }
  DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);
  // m_kvDB[op.Key] = op.Value;
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  //    DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  int readIndex = -1;
  if (!RequestReadIndex(&readIndex)) {
    PrintReadMetricsIfNeeded();
    reply->set_err(WrongLeaderErr());
    return;
  }

  auto applyWaitStart = now();
  if (!WaitApplied(readIndex)) {
    m_readApplyTimeout.fetch_add(1);
    m_readApplyWaitMs.fetch_add(
        static_cast<std::uint64_t>(std::chrono::duration<double, std::milli>(now() - applyWaitStart).count()));
    PrintReadMetricsIfNeeded();
    reply->set_err(WrongLeaderErr());
    return;
  }
  m_readApplyWaitMs.fetch_add(
      static_cast<std::uint64_t>(std::chrono::duration<double, std::milli>(now() - applyWaitStart).count()));

  std::string value;
  bool exist = false;
  ExecuteGetOpOnKVDB(op, &value, &exist);
  if (exist) {
    reply->set_err(OK);
    reply->set_value(value);
  } else {
    reply->set_err(ErrNoKey);
    reply->set_value("");
  }
  PrintReadMetricsIfNeeded();
}

void KvServer::notifyAppliedProgress() { m_applyCv.notify_all(); }

std::string KvServer::WrongLeaderErr() {
  int leaderId = m_raftNode->GetKnownLeaderId();
  if (leaderId >= 0) {
    return ErrWrongLeader + ":" + std::to_string(leaderId);
  }
  return ErrWrongLeader;
}

bool KvServer::RequestReadIndex(int *readIndex) {
  auto pending = std::make_shared<PendingRead>();
  bool shouldRunReadIndex = false;
  {
    std::lock_guard<std::mutex> lg(m_readIndexMtx);
    m_pendingReads.push_back(pending);
    if (!m_readIndexInFlight) {
      m_readIndexInFlight = true;
      shouldRunReadIndex = true;
    }
  }

  if (shouldRunReadIndex) {
    while (true) {
      std::vector<std::shared_ptr<PendingRead> > batch;
      {
        std::lock_guard<std::mutex> lg(m_readIndexMtx);
        batch.swap(m_pendingReads);
      }

      int batchReadIndex = -1;
      auto readIndexStart = now();
      bool ok = m_raftNode->ReadIndex(&batchReadIndex);
      auto elapsedMs =
          static_cast<std::uint64_t>(std::chrono::duration<double, std::milli>(now() - readIndexStart).count());

      {
        std::lock_guard<std::mutex> lg(m_readIndexMtx);
        for (auto &item : batch) {
          item->done = true;
          item->ok = ok;
          item->readIndex = batchReadIndex;
        }
      }

      auto batchSize = static_cast<std::uint64_t>(batch.size());
      if (ok) {
        m_readIndexSuccess.fetch_add(batchSize);
      } else {
        m_readIndexFailure.fetch_add(batchSize);
      }
      m_readIndexLatencyMs.fetch_add(elapsedMs * batchSize);
      m_readBatchCount.fetch_add(1);
      m_readBatchSizeTotal.fetch_add(batchSize);
      UpdateMaxAtomic(&m_readBatchMaxSize, batchSize);

      for (auto &item : batch) {
        item->cv.notify_all();
      }

      std::lock_guard<std::mutex> lg(m_readIndexMtx);
      if (m_pendingReads.empty()) {
        m_readIndexInFlight = false;
        break;
      }
    }
  }

  std::unique_lock<std::mutex> ul(m_readIndexMtx);
  if (!pending->done) {
    pending->cv.wait_for(ul, std::chrono::milliseconds(CONSENSUS_TIMEOUT), [&pending]() { return pending->done; });
  }
  if (!pending->done || !pending->ok) {
    return false;
  }
  *readIndex = pending->readIndex;
  return true;
}

bool KvServer::WaitApplied(int raftIndex) {
  const auto start = now();
  std::unique_lock<std::mutex> lock(m_mtx);
  while (m_lastAppliedRaftLogIndex < raftIndex) {
    if (std::chrono::duration<double, std::milli>(now() - start).count() >= CONSENSUS_TIMEOUT) {
      return false;
    }
    m_applyCv.wait_for(lock, std::chrono::milliseconds(ApplyInterval),
                       [this, raftIndex]() { return m_lastAppliedRaftLogIndex >= raftIndex; });
  }
  return true;
}

void KvServer::PrintReadMetricsIfNeeded() {
  auto success = m_readIndexSuccess.load();
  auto failure = m_readIndexFailure.load();
  auto total = success + failure;
  if (total == 0 || total % 100 != 0) {
    return;
  }

  auto readIndexAvg = m_readIndexLatencyMs.load() / total;
  auto applyAvg = m_readApplyWaitMs.load() / total;
  auto batchCount = m_readBatchCount.load();
  auto avgBatchSize = batchCount == 0 ? 0 : m_readBatchSizeTotal.load() / batchCount;
  std::cout << "[ReadMetrics] server=" << m_me << " total=" << total << " readIndexOk=" << success
            << " readIndexFail=" << failure << " applyTimeout=" << m_readApplyTimeout.load()
            << " avgReadIndexMs=" << readIndexAvg << " avgApplyWaitMs=" << applyAvg
            << " readBatchCount=" << batchCount << " avgReadBatchSize=" << avgBatchSize
            << " maxReadBatchSize=" << m_readBatchMaxSize.load() << std::endl;
}
void KvServer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);

  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }

  // State Machine (KVServer solute the duplicate problem)
  // duplicate command will not be exed
  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    // execute command
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    }
    if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    }
  }
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
  }

  {
    std::lock_guard<std::mutex> lg(m_mtx);
    m_lastAppliedRaftLogIndex = std::max(m_lastAppliedRaftLogIndex, message.CommandIndex);
    notifyAppliedProgress();
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
    return false;
  }
  return RequestId <= m_lastRequestId[ClientId];
}

void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();
  int raftIndex = -1;
  int _ = -1;
  bool isleader = false;

  m_raftNode->Start(op, &raftIndex, &_, &isleader);

  if (!isleader) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
        "not leader",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

    reply->set_err(WrongLeaderErr());
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
      m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, std::make_shared<LockQueue<Op> >()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];

  m_mtx.unlock();

  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);
    } else {
      reply->set_err(WrongLeaderErr());
    }
  } else {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      reply->set_err(OK);
    } else {
      reply->set_err(WrongLeaderErr());
    }
  }

  m_mtx.lock();

  waitApplyCh.erase(raftIndex);
  m_mtx.unlock();
}

void KvServer::ReadRaftApplyCommandLoop() {
  while (!m_stopping.load()) {
    auto message = applyChan->Pop();
    if (m_stopping.load()) {
      break;
    }  //闂冭顢ｅ鐟板毉
    DPrintf("[KvServer::ReadRaftApplyCommandLoop-kvserver{%d}] received raft apply message", m_me);
    // listen to every command applied by its raft ,delivery to relative RPC Handler

    if (message.CommandValid) {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}

// snapShot閲岄潰鍖呭惈kvserver闇€瑕佺淮鎶ょ殑persist_lastRequestId 浠ュ強kvDB鐪熸淇濆瓨鐨勬暟鎹畃ersist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
  if (snapshot.empty()) {
    // bootstrap without any state?
    return;
  }
  parseFromString(snapshot);

  //    r := bytes.NewBuffer(snapshot)
  //    d := labgob.NewDecoder(r)
  //
  //
  //    if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
  //                DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
  //        } else {
  //        kv.kvDB = persist_kvdb
  //        kv.lastRequestId = persist_lastRequestId
  //    }
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
  std::lock_guard<std::mutex> lg(m_mtx);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    return false;
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];
  chForRaftIndex->Push(op);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
  return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (proportion <= 0) {
    proportion = 1;
  }
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState * proportion / 10.0) {
    // Send SnapShot Command
    auto snapshot = MakeSnapShot();
    m_raftNode->Snapshot(raftIndex, snapshot);
  }
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    m_lastAppliedRaftLogIndex = std::max(m_lastAppliedRaftLogIndex, message.SnapshotIndex);
    notifyAppliedProgress();
  }
}

std::string KvServer::MakeSnapShot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

grpc::Status KvServer::PutAppend(grpc::ServerContext *context, const ::raftKVRpcProctoc::PutAppendArgs *request,
                                 ::raftKVRpcProctoc::PutAppendReply *response) {
  KvServer::PutAppend(request, response);
  return grpc::Status::OK;
}

grpc::Status KvServer::Get(grpc::ServerContext *context, const ::raftKVRpcProctoc::GetArgs *request,
                           ::raftKVRpcProctoc::GetReply *response) {
  KvServer::Get(request, response);
  return grpc::Status::OK;
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port)
    : m_me(me),
      m_port(port),
      m_nodeInfoFileName(std::move(nodeInforFileName)),
      m_raftNode(std::make_shared<Raft>()),
      m_persister(std::make_shared<Persister>(me)),
      applyChan(std::make_shared<LockQueue<ApplyMsg> >()),
      m_maxRaftState(maxraftstate),
      m_lastSnapShotRaftLogIndex(0),
      m_lastAppliedRaftLogIndex(0),
      m_skipList(6) {
  auto snapshot = m_persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
    m_lastAppliedRaftLogIndex = m_lastSnapShotRaftLogIndex;
  }
}

KvServer::~KvServer() { Shutdown(); }

bool KvServer::Start() {
  bool expected = false;
  if (!m_started.compare_exchange_strong(expected, true)) {
    return true;
  }
  m_stopping.store(false);
  NodeConfig config;
  config.LoadConfigFile(m_nodeInfoFileName.c_str());
  std::vector<std::pair<std::string, short> > ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);
    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    ipPortVt.emplace_back(nodeIp, static_cast<short>(std::atoi(nodePortStr.c_str())));
  }

  std::vector<std::shared_ptr<RaftRpcUtil> > servers;
  for (int i = 0; i < ipPortVt.size(); ++i) {
    if (i == m_me) {
      servers.push_back(nullptr);
      continue;
    }
    servers.push_back(std::make_shared<RaftRpcUtil>(ipPortVt[i].first, ipPortVt[i].second));
  }
  m_raftNode->init(servers, m_me, m_persister, applyChan);

  const std::string serverAddress = "0.0.0.0:" + std::to_string(m_port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  builder.RegisterService(m_raftNode.get());
  m_grpcServer = builder.BuildAndStart();
  if (!m_grpcServer) {
    std::cout << "grpc server startup failed on " << serverAddress << std::endl;
    m_started.store(false);
    return false;
  }

  std::cout << "grpc server started on " << serverAddress << std::endl;
  m_grpcServerThread = std::thread([this]() { m_grpcServer->Wait(); });
  m_applyThread = std::thread(&KvServer::ReadRaftApplyCommandLoop, this);
  return true;
}

void KvServer::Shutdown() {
  if (!m_started.load()) {
    return;
  }
  m_stopping.store(true);
  if (m_grpcServer) {
    m_grpcServer->Shutdown();
  }
  if (applyChan) {
    applyChan->Push(ApplyMsg());
  }
  Wait();
  m_grpcServer.reset();
  m_started.store(false);
}

void KvServer::Wait() {
  if (m_grpcServerThread.joinable()) {
    m_grpcServerThread.join();
  }
  if (m_applyThread.joinable()) {
    m_applyThread.join();
  }
}

void KvServer::StartKVServer() { (void)Start(); }
