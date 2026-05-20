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
  // if op.IfDuplicate {   //get鐠囬攱鐪伴弰顖氬讲闁插秴顦查幍褑顢戦惃鍕剁礉閸ョ姵顒濋崣顖欎簰娑撳秶鏁ら崚銈咁槻
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
    // *value = m_skipList.se //value瀹歌尙绮＄€瑰本鍨氱挧瀣偓闂寸啊
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

// 婢跺嫮鎮婇弶銉ㄥ殰clerk閻ㄥ嚕et RPC
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
      sleepNMilliseconds(1);

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
  auto start = now();
  while (true) {
    {
      std::lock_guard<std::mutex> lg(m_mtx);
      if (m_lastAppliedRaftLogIndex >= raftIndex) {
        return true;
      }
    }
    if (std::chrono::duration<double, std::milli>(now() - start).count() >= CONSENSUS_TIMEOUT) {
      return false;
    }
    sleepNMilliseconds(ApplyInterval);
  }
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
    //  kv.lastRequestId[op.ClientId] = op.RequestId  閸︹€ecutexxx閸戣姤鏆熼柌宀勬桨閺囧瓨鏌婇惃?
  }
  //閸掓媽绻栭柌瀹瑅DB瀹歌尙绮￠崚鏈电稊娴滃棗鎻╅悡?
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    //婵″倹鐏塺aft閻ㄥ埐og婢额亜銇囬敍鍫濄亣娴滃孩瀵氱€规氨娈戝В鏂剧伐閿涘姘ㄩ幎濠傚煑娴ｆ粌鎻╅悡?
  }

  {
    std::lock_guard<std::mutex> lg(m_mtx);
    m_lastAppliedRaftLogIndex = std::max(m_lastAppliedRaftLogIndex, message.CommandIndex);
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
    return false;
    // todo :娑撳秴鐡ㄩ崷銊ㄧ箹娑撶寶lient鐏忓崬鍨卞?
  }
  return RequestId <= m_lastRequestId[ClientId];
}

// get閸滃ut//append閸╃柉顢戦惃鍕徔妤傛梻绐楃弧鈧弰顖欑瑝娑撯偓濡絿娈?
// PutAppend閸︺劍鏁归崚鐨塧ft濞戝牊浼呮稊瀣乏閸╃柉顢戦敍灞藉徔妤傛柨鍤遍弫姝岊棓闂堛垹褰ч崚銈嗘煑閸愵亞鐡戦幀褝绱欓弰顖氭儊闁插秷顦敍?
// get閸戣姤鏆╅弨璺哄煂raft濞戝牊浼呮稊瀣乏閸︻煉绱濋崶鐘靛煇get閻捖ょ彨閺勵垰鎯侀柌宥堫槵闁棄褰叉禒銉ュ晙閸╃柉顢?
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

  m_mtx.unlock();  //閻╁瓨甯寸憴锝夋敚閿涘瞼鐡戝鍛崲閸斺剝澧界悰灞界暚閹存劧绱濇稉宥堝厴娑撯偓閻╁瓨瀣侀柨浣虹搼瀵?

  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);  // 鐡掑懏妞傛禍?娴ｅ棗娲滄稉鐑樻Ц闁插秴顦查惃鍕嚞濮瑰偊绱濇潻鏂挎礀ok閿涘苯鐤勯梽鍛瑐鐏忚京鐣诲▽鈩冩箒鐡掑懏妞傞敍灞芥躬閻喐顒滈幍褑顢戦惃鍕閸婃瑤绡冪憰浣稿灲閺傤厽妲搁崥锕傚櫢婢?
    } else {
      reply->set_err(WrongLeaderErr());  ///鏉╂瑩鍣锋潻鏂挎礀鏉╂瑤閲滈惃鍕窗閻ㄥ嫯顔€clerk闁插秵鏌婄亸婵婄槸
    }
  } else {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      //閸欘垵鍏橀崣鎴犳晸leader閻ㄥ嫬褰夐弴鏉戭嚤閼峰瓨妫╄箛妤勵潶鐟曞棛娲婇敍灞芥礈濮濄倕绻€妞ょ粯顥呴弻?
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
    //婵″倹鐏夐崣顏呮惙娴ｆ竵pplyChan娑撳秶鏁ら幏鍧楁敚閿涘苯娲滄稉绡磒plyChan閼奉亜绻佺敮锕傛敚
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

// raft浼氫笌persist灞備氦浜掞紝kvserver灞備篃浼氾紝鍥犱负kvserver灞傚紑濮嬬殑鏃跺€欓渶瑕佹仮澶峩vdb鐨勭姸鎬?
// 鍏充簬蹇収raft灞備笌persist鐨勪氦浜掞細淇濆瓨kvserver浼犳潵鐨剆napshot锛涚敓鎴恖eaderInstallSnapshot RPC鐨勬椂鍊欎篃闇€瑕佽鍙杝napshot
// 鍥犳snapshot鐨勫叿浣撴牸寮忔槸鐢眐vserver灞傛潵瀹氱殑锛宺aft鍙礋璐ｄ紶閫掕繖涓笢瑗?
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
  //    var persist_kvdb map[string]string  //閻炲棗绨茶箛顐ゅ弾
  //    var persist_lastRequestId map[int64]int //韫囶偆鍙庢潻娆庨嚋娑撹桨绨＄紒瀛樺Б缁炬寧鈧傜閼峰瓨鈧?
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
