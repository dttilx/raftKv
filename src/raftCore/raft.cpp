#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNormal);
  // Your code here (2A, 2B).

  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);
    DPrintf("[func-AppendEntries-rf{%d}] 鎷掔粷浜?鍥犱负Leader{%d}鐨則erm{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return;
  }
  //    Defer ec1([this]() -> void { this->persist(); });
  DEFER { persist(); };
  if (args->term() > m_currentTerm) {
    // args.LeaderId, args.Term, rf.me, rf.currentTerm)
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
  }
  myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
  m_status = Follower;
  m_leaderId = args->leaderid();
  // term鐩哥瓑
  m_lastResetElectionTime = now();


  if (args->prevlogindex() > getLastLogIndex()) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    return;
  } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(
        m_lastSnapshotIncludeIndex +
        1);
    return;
  }
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {

    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex()) {
        m_logs.push_back(log);
      } else {
        // rf.logs[entry.Index-firstIndex].Term ?= entry.Term

        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
          myAssert(false, format("[func-AppendEntries-rf{%d}] 涓よ妭鐐筶ogIndex{%d}鍜宼erm{%d}鐩稿悓锛屼絾鏄叾command{%d:%d}   "
                                 " {%d:%d}鍗翠笉鍚岋紒锛乗n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                 log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
          int conflictIndex = getSlicesIndexFromLogIndex(log.logindex());
          m_logs.erase(m_logs.begin() + conflictIndex, m_logs.end());
          for (int j = i; j < args->entries_size(); ++j) {
            m_logs.push_back(args->entries(j));
          }
          break;
        }
      }
    }

    // rf.logs = append(rf.logs, args.Entries...)
    myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
    // if len(args.Entries) > 0 {
    //	fmt.Printf("[func-AppendEntries  rf:{%v}] ] : args.term:%v, rf.term:%v  ,rf.logs鐨勯暱搴︼細%v\n", rf.me, args.Term,
    // rf.currentTerm, len(rf.logs))
    // }
    if (args->leadercommit() > m_commitIndex) {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
    }

    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);

    //        rf.me,
    //                args.LeaderId, rf.getLastLogIndex(), reply)

    return;
  } else {
    // 浼樺寲
    reply->set_updatenextindex(args->prevlogindex());

    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    //        DPrintf("[func-AppendEntries-rf{%v}]
    //                rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
    //                rf.logs[rf.getSlicesIndexFromLogIndex(args.PrevLogIndex)].LogTerm, reply)
    //                args.PrevLogIndex, reply.UpdateNextIndex, args.PrevLogIndex - reply.UpdateNextIndex) //
    //                寰堝閮芥槸浼樺寲浜?
    return;
  }

  // fmt.Printf("[func-AppendEntries,rf{%v}]:len(rf.logs):%v, rf.commitIndex:%v\n", rf.me, len(rf.logs), rf.commitIndex)
}

void Raft::applierTicker() {
  while (true) {
    m_mtx.lock();
    if (m_status == Leader) {
      DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
              m_commitIndex);
    }
    auto applyMsgs = getApplyLogs();
    m_mtx.unlock();
    if (!applyMsgs.empty()) {
      DPrintf("[func- Raft::applierTicker()-raft{%d}] 鍚慿vserver鍫卞憡鐨刟pplyMsgs闀峰害鐖诧細{%d}", m_me, applyMsgs.size());
    }
    for (auto& message : applyMsgs) {
      applyChan->Push(message);
    }
    // usleep(1000 * ApplyInterval);
    sleepNMilliseconds(ApplyInterval);
  }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (lastIncludedIndex < m_lastSnapshotIncludeIndex) {
    return false;
  }
  if (lastIncludedIndex == m_lastSnapshotIncludeIndex && lastIncludedTerm != m_lastSnapshotIncludeTerm) {
    return false;
  }
  return true;
  //// Your code here (2D).
  // rf.mu.Lock()
  // defer rf.mu.Unlock()
  // DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex {%v} to check
  // whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)
  //// outdated snapshot
  // if lastIncludedIndex <= rf.commitIndex {
  //	return false
  // }
  //
  // lastLogIndex, _ := rf.getLastLogIndexAndTerm()
  // if lastIncludedIndex > lastLogIndex {
  //	rf.logs = make([]LogEntry, 0)
  // } else {
  //	rf.logs = rf.logs[rf.getSlicesIndexFromLogIndex(lastIncludedIndex)+1:]
  // }
  //// update dummy entry with lastIncludedTerm and lastIncludedIndex
  // rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
  //
  // rf.persister.Save(rf.persistData(), snapshot)
  // return true
}

void Raft::doElection() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    // fmt.Printf("[       ticker-func-rf(%v)              ] is a Leader,wait the  lock\n", rf.me)
  }
  // fmt.Printf("[       ticker-func-rf(%v)              ] get the  lock\n", rf.me)

  if (m_status != Leader) {
    DPrintf("[       ticker-func-rf(%d)              ]  閫変妇瀹氭椂鍣ㄥ埌鏈熶笖涓嶆槸leader锛屽紑濮嬮€変妇 \n", m_me);
    m_status = Candidate;
    m_currentTerm += 1;
    m_votedFor = m_me;
    persist();
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);
    m_lastResetElectionTime = now();
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();


      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                    votedNum);
      t.detach();
    }
  }
}

void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader鐨勫績璺冲畾鏃跺櫒瑙﹀彂浜嗕笖鎷垮埌mutex锛屽紑濮嬪彂閫丄E\n", m_me);
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader鐨勫績璺冲畾鏃跺櫒瑙﹀彂浜?index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        //                        DPrintf("[func-ticker()-rf{%v}]rf.nextIndex[%v] {%v} <=
        //                        rf.lastSnapshotIncludeIndex{%v},so leaderSendSnapShot", rf.me, i, rf.nextIndex[i],
        //                        rf.lastSnapshotIncludeIndex)
        std::thread t(&Raft::leaderSendSnapShot, this, i);
        t.detach();
        continue;
      }
      //鏋勯€犲彂閫佸€?
      int preLogIndex = -1;
      int PrevLogTerm = -1;
      getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(preLogIndex);
      appendEntriesArgs->set_prevlogterm(PrevLogTerm);
      appendEntriesArgs->clear_entries();
      appendEntriesArgs->set_leadercommit(m_commitIndex);
      if (preLogIndex != m_lastSnapshotIncludeIndex) {
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = m_logs[j];
        }
      } else {
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item;
        }
      }
      int lastLogIndex = getLastLogIndex();
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);

      std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply);
      t.detach();
    }
    m_lastResetHearBeatTime = now();
  }
}

void Raft::electionTimeOutTicker() {
  // Check if a Leader election should be started.
  while (true) {
    /**
     * 濡傛灉涓嶇潯鐪狅紝閭ｄ箞瀵逛簬leader锛岃繖涓嚱鏁颁細涓€鐩寸┖杞紝娴垂cpu銆備笖鍔犲叆鍗忕▼涔嬪悗锛岀┖杞細瀵艰嚧鍏朵粬鍗忕▼鏃犳硶杩愯锛屽浜庢椂闂存晱鎰熺殑AE锛屼細瀵艰嚧蹇冭烦鏃犳硶姝ｅ父鍙戦€佸鑷村紓甯?
     */
    while (m_status == Leader) {
      usleep(
          HeartBeatTimeout);
    }
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      m_mtx.lock();
      wakeTime = now();
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
      continue;
    }
    doElection();
  }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));

  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);
    //        rf.me, rf.lastApplied, rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogTerm,
    //        rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].Command)
  }
  return applyMsgs;
}

int Raft::getNewCommandIndex() {
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// getPrevLogInfo
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// GetState return currentTerm and whether this server
// believes it is the Leader.
void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  DEFER {
    m_mtx.unlock();
  };

  // Your code here (2A).
  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

int Raft::GetKnownLeaderId() {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_status == Leader) {
    return m_me;
  }
  return m_leaderId;
}

bool Raft::ReadIndex(int* readIndex) {
  int term = -1;
  int leaderCommit = -1;
  int successCount = 1;  // self
  const int majority = static_cast<int>(m_peers.size()) / 2 + 1;

  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_status != Leader) {
      *readIndex = -1;
      return false;
    }
    m_leaderId = m_me;
    term = m_currentTerm;
    leaderCommit = m_commitIndex;
  }

  if (successCount >= majority) {
    *readIndex = leaderCommit;
    return true;
  }

  for (int i = 0; i < m_peers.size(); ++i) {
    if (i == m_me) {
      continue;
    }

    raftRpcProctoc::AppendEntriesArgs args;
    {
      std::lock_guard<std::mutex> lg(m_mtx);
      if (m_status != Leader || m_currentTerm != term) {
        *readIndex = -1;
        return false;
      }
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        continue;
      }
      int preLogIndex = -1;
      int preLogTerm = -1;
      getPrevLogInfo(i, &preLogIndex, &preLogTerm);
      args.set_term(term);
      args.set_leaderid(m_me);
      args.set_prevlogindex(preLogIndex);
      args.set_prevlogterm(preLogTerm);
      args.clear_entries();
      args.set_leadercommit(m_commitIndex);
      if (preLogIndex != m_lastSnapshotIncludeIndex) {
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
          raftRpcProctoc::LogEntry* sendEntryPtr = args.add_entries();
          *sendEntryPtr = m_logs[j];
        }
      } else {
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr = args.add_entries();
          *sendEntryPtr = item;
        }
      }
    }

    raftRpcProctoc::AppendEntriesReply reply;
    reply.set_appstate(Disconnected);
    bool ok = m_peers[i]->AppendEntries(&args, &reply);
    if (!ok || reply.appstate() == Disconnected) {
      continue;
    }

    std::lock_guard<std::mutex> lg(m_mtx);
    if (reply.term() > m_currentTerm) {
      m_status = Follower;
      m_currentTerm = reply.term();
      m_votedFor = -1;
      m_leaderId = -1;
      persist();
      *readIndex = -1;
      return false;
    }
    if (m_status != Leader || m_currentTerm != term) {
      *readIndex = -1;
      return false;
    }
    if (reply.term() == term && reply.success()) {
      ++successCount;
      if (successCount >= majority) {
        *readIndex = leaderCommit;
        return true;
      }
    }
  }

  *readIndex = -1;
  return false;
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.term{%v}<rf{%v}.term{%v} ", rf.me, args.LeaderId,
    //        args.Term, rf.me, rf.currentTerm)

    return;
  }
  if (args->term() > m_currentTerm) {
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    m_leaderId = args->leaderid();
    persist();
  }
  m_status = Follower;
  m_leaderId = args->leaderid();
  m_lastResetElectionTime = now();
  // outdated snapshot
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.LastSnapShotIncludeIndex{%v} <=
    //        rf{%v}.lastSnapshotIncludeIndex{%v} ", rf.me, args.LeaderId, args.LastSnapShotIncludeIndex, rf.me,
    //        rf.lastSnapshotIncludeIndex)
    return;
  }
  auto lastLogIndex = getLastLogIndex();

  if (lastLogIndex > args->lastsnapshotincludeindex()) {
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
  } else {
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();

  std::thread t(&Raft::pushMsgToKvServer, this, msg);
  t.detach();
  //    DPrintf("[func-InstallSnapshot-rf{%v}] receive snapshot from {%v} ,LastSnapShotIncludeIndex ={%v} ", rf.me,
  //    args.LeaderId, args.LastSnapShotIncludeIndex)
  //鎸佷箙鍖?
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

void Raft::leaderHearBeatTicker() {
  while (true) {
    while (m_status != Leader) {
      usleep(1000 * HeartBeatTimeout);
      // std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
    }

    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0) {
      continue;
    }
    // DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader鐨勫績璺冲畾鏃跺櫒瑙﹀彂浜哱n", m_me);
    doHeartBeat();
  }
}

void Raft::leaderSendSnapShot(int server) {
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());

  raftRpcProctoc::InstallSnapshotResponse reply;
  m_mtx.unlock();
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (!ok) {
    return;
  }
  if (m_status != Leader || m_currentTerm != args.term()) {
    return;
  }
  if (reply.term() > m_currentTerm) {
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    m_leaderId = -1;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}

void Raft::leaderUpdateCommitIndex() {
  // for index := rf.commitIndex+1;index < len(rf.log);index++ {
  // for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
  for (int index = getLastLogIndex(); index > m_commitIndex; index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] >= index) {
        sum += 1;
      }
    }

    // log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index, rf.commitIndex,
    // rf.getLastIndex())
    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      break;
    }
  }
  //    DPrintf("[func-leaderUpdateCommitIndex()-rf{%v}] Leader %d(term%d) commitIndex
  //    %d",rf.me,rf.me,rf.currentTerm,rf.commitIndex)
}

bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("涓嶆弧瓒筹細logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
  // if logIndex == rf.lastSnapshotIncludeIndex {
  // 	return logTerm == rf.lastSnapshotIncludeTerm
  // } else {
  // 	return logTerm == rf.logs[rf.getSlicesIndexFromLogIndex(logIndex)].LogTerm
  // }
}

void Raft::persist() {
  // Your code here (2C).
  auto data = persistData();
  m_persister->SaveRaftState(data);
  // fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm,
  // rf.votedFor, rf.logs) fmt.Printf("%v\n", string(data))
}

void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);

  // Your code here (2A, 2B).
  DEFER {
    persist();
  };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }
  if (args->term() > m_currentTerm) {
    //        rf.currentTerm)
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_leaderId = -1;

  }
  myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 鍓嶉潰鏍￠獙杩嘺rgs.Term==rf.currentTerm锛岃繖閲屽嵈涓嶇瓑", m_me));

  int lastLogTerm = getLastLogTerm();
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
    // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
    //鏃ュ織澶棫浜?
    if (args->lastlogterm() < lastLogTerm) {
      //                    DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
      //                    candidate_lastlog_term{%v} < lastlog_term{%v}\n", rf.me, args.CandidateId, args.LastLogTerm,
      //                    lastLogTerm)
    } else {
      //            DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
      //            candidate_log_index{%v} < log_index{%v}\n", rf.me, args.CandidateId, args.LastLogIndex,
      //            rf.getLastLogIndex())
    }
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  }
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    //        DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because has voted\n",
    //        rf.me, args.CandidateId)
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  } else {
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();
    //        DPrintf("[	    func-RequestVote-rf(%v)		] : voted rf[%v]\n", rf.me, rf.votedFor)
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);

    return;
  }
}

bool Raft::UpToDate(int index, int term) {
  // lastEntry := rf.log[len(rf.log)-1]

  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 *
 * @return 鏈€鏂扮殑log鐨刲ogindex锛屽嵆log鐨勯€昏緫index銆傚尯鍒簬log鍦╩_logs涓殑鐗╃悊index
 * 鍙锛歡etLastLogIndexAndTerm()
 */
int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 *
 * @param logIndex log鐨勯€昏緫index銆傛敞鎰忓尯鍒簬m_logs鐨勭墿鐞唅ndex
 * @return
 */
int Raft::getLogTermFromLogIndex(int logIndex) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
  // ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  // todo
  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 鍚憇erver{%d} 鐧奸€?RequestVote 闁嬪", m_me, m_currentTerm, getLastLogIndex());
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[func-sendRequestVote rf{%d}] 鍚憇erver{%d} 鐧奸€?RequestVote 瀹岀暍锛岃€楁檪:{%d} ms", m_me, m_currentTerm,
          getLastLogIndex(), now() - start);

  if (!ok) {
    return ok;
  }
  // for !ok {
  //
  //	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  //	//if ok {
  //	//	break
  //	//}

  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    m_leaderId = -1;
    persist();
    return true;
  } else if (reply->term() < m_currentTerm) {
    return true;
  }
  myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

  if (!reply->votegranted()) {
    return true;
  }

  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    *votedNum = 0;
    if (m_status == Leader) {
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 鍚屼竴涓猼erm褰撲袱娆￠瀵硷紝error", m_me, m_currentTerm));
    }
    m_status = Leader;
    m_leaderId = m_me;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;
      m_matchIndex[i] = 0;
    }
    std::thread t(&Raft::doHeartBeat, this);  //椹笂鍚戝叾浠栬妭鐐瑰鍛婅嚜宸卞氨鏄痩eader
    t.detach();

    persist();
  }
  return true;
}

bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply) {
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 鍚戣妭鐐箋%d}鍙戦€丄E rpc闁嬪 锛?args->entries_size():{%d}", m_me,
          server, args->entries_size());
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  if (!ok) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 鍚戣妭鐐箋%d}鍙戦€丄E rpc澶辨晽", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 鍚戣妭鐐箋%d}鍙戦€丄E rpc鎴愬姛", m_me, server);
  if (reply->appstate() == Disconnected) {
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);

  if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    m_leaderId = -1;
    persist();
    return ok;
  } else if (reply->term() < m_currentTerm) {
    DPrintf("[func -sendAppendEntries  rf{%d}]  鑺傜偣锛歿%d}鐨則erm{%d}<rf{%d}鐨則erm{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader) {
    return ok;
  }
  // term鐩哥瓑

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
  if (!reply->success()) {
    if (reply->updatenextindex() != -100) {
      DPrintf("[func -sendAppendEntries  rf{%d}]  杩斿洖鐨勬棩蹇梩erm鐩哥瓑锛屼絾鏄笉鍖归厤锛屽洖缂﹏extIndex[%d]锛歿%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();  //澶辫触鏄笉鏇存柊mathIndex鐨?
    }
  } else {
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    int lastLogIndex = getLastLogIndex();

    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                    m_logs.size(), server, lastLogIndex));
    leaderUpdateCommitIndex();
    myAssert(m_commitIndex <= lastLogIndex,
             format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                    m_commitIndex));
  }
  return ok;
}

grpc::Status Raft::AppendEntries(grpc::ServerContext* context, const ::raftRpcProctoc::AppendEntriesArgs* request,
                                 ::raftRpcProctoc::AppendEntriesReply* response) {
  AppendEntries1(request, response);
  return grpc::Status::OK;
}

grpc::Status Raft::InstallSnapshot(grpc::ServerContext* context, const ::raftRpcProctoc::InstallSnapshotRequest* request,
                                   ::raftRpcProctoc::InstallSnapshotResponse* response) {
  InstallSnapshot(request, response);
  return grpc::Status::OK;
}

grpc::Status Raft::RequestVote(grpc::ServerContext* context, const ::raftRpcProctoc::RequestVoteArgs* request,
                               ::raftRpcProctoc::RequestVoteReply* response) {
  RequestVote(request, response);
  return grpc::Status::OK;
}

void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
  std::lock_guard<std::mutex> lg1(m_mtx);
  //    m_mtx.lock();
  //    Defer ec1([this]()->void {
  //       m_mtx.unlock();
  //    });
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader");
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString());
  newLogEntry.set_logterm(m_currentTerm);
  newLogEntry.set_logindex(getNewCommandIndex());
  m_logs.emplace_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();

  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  // Your initialization code here (2A, 2B, 2C).
  m_mtx.lock();

  // applier
  this->applyChan = applyCh;
  //    rf.ApplyMsgQueue = make(chan ApplyMsg)
  m_currentTerm = 0;
  m_status = Follower;
  m_leaderId = -1;
  m_commitIndex = 0;
  m_lastApplied = 0;
  m_logs.clear();
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);
    m_nextIndex.push_back(0);
  }
  m_votedFor = -1;

  m_lastSnapshotIncludeIndex = 0;
  m_lastSnapshotIncludeTerm = 0;
  m_lastResetElectionTime = now();
  m_lastResetHearBeatTime = now();

  // initialize from state persisted before a crash
  readPersist(m_persister->ReadRaftState());
  if (m_lastSnapshotIncludeIndex > 0) {
    m_lastApplied = m_lastSnapshotIncludeIndex;
  }

  DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  m_mtx.unlock();

  std::thread heartbeatTicker(&Raft::leaderHearBeatTicker, this);
  heartbeatTicker.detach();

  std::thread electionTicker(&Raft::electionTimeOutTicker, this);
  electionTicker.detach();

  std::thread t3(&Raft::applierTicker, this);
  t3.detach();
}

std::string Raft::persistData() {
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
  for (auto& item : m_logs) {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode;
  return ss.str();
}

void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  // read class state from archive
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto& item : boostPersistRaftNode.m_logs) {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex();

  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}
