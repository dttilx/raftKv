//
// Created by swx on 23-6-1.
//

#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <grpcpp/grpcpp.h>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "kvServerRPC.grpc.pb.h"
#include "raft.h"
#include "skipList.h"

class KvServer : public raftKVRpcProctoc::kvServerRpc::Service {
 private:
  std::mutex m_mtx;
  int m_me;
  short m_port;
  std::string m_nodeInfoFileName;
  std::shared_ptr<Raft> m_raftNode;
  std::shared_ptr<Persister> m_persister;
  std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // kvServer鍜宺aft鑺傜偣鐨勯€氫俊绠￠亾
  int m_maxRaftState;                               // snapshot if log grows this big
  std::unique_ptr<grpc::Server> m_grpcServer;
  std::thread m_grpcServerThread;
  std::thread m_applyThread;
  std::atomic<bool> m_started{false};
  std::atomic<bool> m_stopping{false};

  // Your definitions here.
  std::string m_serializedKVData;  // todo 锛?搴忓垪鍖栧悗鐨刱v鏁版嵁锛岀悊璁轰笂鍙互涓嶇敤锛屼絾鏄洰鍓嶆病鏈夋壘鍒扮壒鍒ソ鐨勬浛浠ｆ柟娉?
  SkipList<std::string, std::string> m_skipList;
  std::unordered_map<std::string, std::string> m_kvDB;

  std::unordered_map<int, std::shared_ptr<LockQueue<Op> > > waitApplyCh;
  // index(raft) -> chan  //锛燂紵锛熷瓧娈靛惈涔?  waitApplyCh鏄竴涓猰ap锛岄敭鏄痠nt锛屽€兼槸Op绫诲瀷鐨勭閬?

  std::unordered_map<std::string, int> m_lastRequestId;  // clientid -> requestID  //涓€涓猭V鏈嶅姟鍣ㄥ彲鑳借繛鎺ュ涓猚lient

  // last SnapShot point , raftIndex
  int m_lastSnapShotRaftLogIndex;
  int m_lastAppliedRaftLogIndex;
  std::atomic<std::uint64_t> m_readIndexSuccess{0};
  std::atomic<std::uint64_t> m_readIndexFailure{0};
  std::atomic<std::uint64_t> m_readApplyTimeout{0};
  std::atomic<std::uint64_t> m_readIndexLatencyMs{0};
  std::atomic<std::uint64_t> m_readApplyWaitMs{0};
  std::atomic<std::uint64_t> m_readBatchCount{0};
  std::atomic<std::uint64_t> m_readBatchSizeTotal{0};
  std::atomic<std::uint64_t> m_readBatchMaxSize{0};

  struct PendingRead {
    std::condition_variable cv;
    bool done = false;
    bool ok = false;
    int readIndex = -1;
  };
  std::mutex m_readIndexMtx;
  bool m_readIndexInFlight = false;
  std::vector<std::shared_ptr<PendingRead> > m_pendingReads;

 public:
  KvServer() = delete;

  KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);
  ~KvServer();

  bool Start();
  void Shutdown();
  void Wait();
  void StartKVServer();

  void DprintfKVDB();

  void ExecuteAppendOpOnKVDB(Op op);

  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

  void ExecutePutOpOnKVDB(Op op);

  void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply
               *reply);  //灏?GetArgs 鏀逛负rpc璋冪敤鐨勶紝鍥犱负鏄繙绋嬪鎴风锛屽嵆鏈嶅姟鍣ㄥ畷鏈哄瀹㈡埛绔潵璇存槸鏃犳劅鐨?
  std::string WrongLeaderErr();
  bool RequestReadIndex(int *readIndex);
  bool WaitApplied(int raftIndex);
  void PrintReadMetricsIfNeeded();
  /**
   * 寰瀝aft绡€榛炰腑鐛插彇娑堟伅  锛堜笉瑕佽浠ョ埐鏄煼琛屻€怗ET銆戝懡浠わ級
   * @param message
   */
  void GetCommandFromRaft(ApplyMsg message);

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  // clerk 浣跨敤RPC杩滅▼璋冪敤
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

  ////涓€鐩寸瓑寰卹aft浼犳潵鐨刟pplyCh
  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op &op, int raftIndex);

  // 妫€鏌ユ槸鍚﹂渶瑕佸埗浣滃揩鐓э紝闇€瑕佺殑璇濆氨鍚憆aft涔嬩笅鍒朵綔蹇収
  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  // Handler the SnapShot from kv.rf.applyCh
  void GetSnapShotFromRaft(ApplyMsg message);

  std::string MakeSnapShot();

 public:  // for rpc
  grpc::Status PutAppend(grpc::ServerContext *context, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response) override;

  grpc::Status Get(grpc::ServerContext *context, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response) override;

  /////////////////serialiazation start ///////////////////////////////
  // notice 锛?func serialize
 private:
  friend class boost::serialization::access;

  // When the class Archive corresponds to an output archive, the
  // & operator is defined similar to <<.  Likewise, when the class Archive
  // is a type of input archive the & operator is defined similar to >>.
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version)  //杩欓噷闈㈠啓闇€瑕佸簭鍒楄瘽鍜屽弽搴忓垪鍖栫殑瀛楁
  {
    ar &m_serializedKVData;

    // ar & m_kvDB;
    ar &m_lastRequestId;
  }

  std::string getSnapshotData() {
    m_serializedKVData = m_skipList.dump_file();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();
  }

  void parseFromString(const std::string &str) {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_skipList.load_file(m_serializedKVData);
    m_serializedKVData.clear();
  }

  /////////////////serialiazation end ///////////////////////////////
};

#endif  // SKIP_LIST_ON_RAFT_KVSERVER_H
