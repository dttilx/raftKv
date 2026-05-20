#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNormal);  // 鑳芥帴鏀跺埌浠ｈ〃缃戠粶鏄甯哥殑
  // Your code here (2A, 2B).
  //	涓嶅悓鐨勪汉鏀跺埌AppendEntries鐨勫弽搴旀槸涓嶅悓鐨勶紝瑕佹敞鎰忔棤璁轰粈涔堟椂鍊欐敹鍒皉pc璇锋眰鍜屽搷搴旈兘瑕佹鏌erm

  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);  // 璁烘枃涓細璁╅瀵间汉鍙互鍙婃椂鏇存柊鑷繁
    DPrintf("[func-AppendEntries-rf{%d}] 鎷掔粷浜?鍥犱负Leader{%d}鐨則erm{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return;  // 娉ㄦ剰浠庤繃鏈熺殑棰嗗浜烘敹鍒版秷鎭笉瑕侀噸璁惧畾鏃跺櫒
  }
  //    Defer ec1([this]() -> void { this->persist(); });
  //    //鐢变簬杩欎釜灞€閮ㄥ彉閲忓垱寤哄湪閿佷箣鍚庯紝鍥犳鎵цpersist鐨勬椂鍊欏簲璇ヤ篃鏄嬁鍒伴攣鐨?
  DEFER { persist(); };  //鐢变簬杩欎釜灞€閮ㄥ彉閲忓垱寤哄湪閿佷箣鍚庯紝鍥犳鎵цpersist鐨勬椂鍊欏簲璇ヤ篃鏄嬁鍒伴攣鐨?
  if (args->term() > m_currentTerm) {
    // 涓夊彉 ,闃叉閬楁紡锛屾棤璁轰粈涔堟椂鍊欓兘鏄笁鍙?
    // DPrintf("[func-AppendEntries-rf{%v} ] 鍙樻垚follower涓旀洿鏂皌erm 鍥犱负Leader{%v}鐨則erm{%v}> rf{%v}.term{%v}\n", rf.me,
    // args.LeaderId, args.Term, rf.me, rf.currentTerm)
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;  // 杩欓噷璁剧疆鎴?1鏈夋剰涔夛紝濡傛灉绐佺劧瀹曟満鐒跺悗涓婄嚎鐞嗚涓婃槸鍙互鎶曠エ鐨?
    // 杩欓噷鍙笉杩斿洖锛屽簲璇ユ敼鎴愯鏀硅妭鐐瑰皾璇曟帴鏀舵棩蹇?
    // 濡傛灉鏄瀵间汉鍜宑andidate绐佺劧杞埌Follower濂藉儚涔熶笉鐢ㄥ叾浠栨搷浣?
    // 濡傛灉鏈潵灏辨槸Follower锛岄偅涔堝叾term鍙樺寲锛岀浉褰撲簬鈥滀笉瑷€鑷槑鈥濈殑鎹簡杩介殢鐨勫璞★紝鍥犱负鍘熸潵鐨刲eader鐨則erm鏇村皬锛屾槸涓嶄細鍐嶆帴鏀跺叾娑堟伅浜?
  }
  myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
  // 濡傛灉鍙戠敓缃戠粶鍒嗗尯锛岄偅涔坈andidate鍙兘浼氭敹鍒板悓涓€涓猼erm鐨刲eader鐨勬秷鎭紝瑕佽浆鍙樹负Follower锛屼负浜嗗拰涓婇潰锛屽洜姝ょ洿鎺ュ啓
  m_status = Follower;  // 杩欓噷鏄湁蹇呰鐨勶紝鍥犱负濡傛灉candidate鏀跺埌鍚屼竴涓猼erm鐨刲eader鐨凙E锛岄渶瑕佸彉鎴恌ollower
  m_leaderId = args->leaderid();
  // term鐩哥瓑
  m_lastResetElectionTime = now();
  //  DPrintf("[	AppendEntries-func-rf(%v)		] 閲嶇疆浜嗛€変妇瓒呮椂瀹氭椂鍣╘n", rf.me);

  // 涓嶈兘鏃犺剳鐨勪粠prevlogIndex寮€濮嬮樁娈垫棩蹇楋紝鍥犱负rpc鍙兘浼氬欢杩燂紝瀵艰嚧鍙戣繃鏉ョ殑log鏄緢涔呬箣鍓嶇殑

  //	閭ｄ箞灏辨瘮杈冩棩蹇楋紝鏃ュ織鏈?绉嶆儏鍐?
  if (args->prevlogindex() > getLastLogIndex()) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    //  DPrintf("[func-AppendEntries-rf{%v}] 鎷掔粷浜嗚妭鐐箋%v}锛屽洜涓烘棩蹇楀お鏂?args.PrevLogIndex{%v} >
    //  lastLogIndex{%v}锛岃繑鍥炲€硷細{%v}\n", rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex(), reply)
    return;
  } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    // 濡傛灉prevlogIndex杩樻病鏈夋洿涓婂揩鐓?
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(
        m_lastSnapshotIncludeIndex +
        1);  // todo 濡傛灉鎯崇洿鎺ュ紕鍒版渶鏂板ソ鍍忎笉瀵癸紝鍥犱负鏄粠鍚庢參鎱㈠線鍓嶅尮閰嶇殑锛岃繖閲屼笉鍖归厤璇存槑鍚庨潰鐨勯兘涓嶅尮閰?
    //  DPrintf("[func-AppendEntries-rf{%v}] 鎷掔粷浜嗚妭鐐箋%v}锛屽洜涓簂og澶€侊紝杩斿洖鍊硷細{%v}\n", rf.me, args.LeaderId, reply)
    return;
  }
  //	鏈満鏃ュ織鏈夐偅涔堥暱锛屽啿绐?same index,different term),鎴柇鏃ュ織
  // 娉ㄦ剰锛氳繖閲岀洰鍓嶅綋args.PrevLogIndex == rf.lastSnapshotIncludeIndex涓庝笉绛夌殑鏃跺€欒鍒嗗紑鑰冭檻锛屽彲浠ョ湅鐪嬭兘涓嶈兘浼樺寲杩欏潡
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {
    //	todo锛?鏁寸悊logs
    //锛屼笉鑳界洿鎺ユ埅鏂紝蹇呴』涓€涓竴涓鏌ワ紝鍥犱负鍙戦€佹潵鐨刲og鍙兘鏄箣鍓嶇殑锛岀洿鎺ユ埅鏂彲鑳藉鑷粹€滃彇鍥炩€濆凡缁忓湪follower鏃ュ織涓殑鏉＄洰
    // 閭ｆ剰鎬濇槸涓嶆槸鍙兘浼氭湁涓€娈靛彂鏉ョ殑AE涓殑logs涓墠鍗婃槸鍖归厤鐨勶紝鍚庡崐鏄笉鍖归厤鐨勶紝杩欑搴旇锛?.follower濡備綍澶勭悊锛?2.濡備綍缁檒eader鍥炲
    // 3. leader濡備綍澶勭悊

    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex()) {
        //瓒呰繃灏辩洿鎺ユ坊鍔犳棩蹇?
        m_logs.push_back(log);
      } else {
        //娌¤秴杩囧氨姣旇緝鏄惁鍖归厤锛屼笉鍖归厤鍐嶆洿鏂帮紝鑰屼笉鏄洿鎺ユ埅鏂?
        // todo 锛?杩欓噷鍙互鏀硅繘涓烘瘮杈冨搴攍ogIndex浣嶇疆鐨則erm鏄惁鐩哥瓑锛宼erm鐩哥瓑灏变唬琛ㄥ尮閰?
        //  todo锛氳繖涓湴鏂规斁鍑烘潵浼氬嚭闂,鎸夌悊璇磇ndex鐩稿悓锛宼erm鐩稿悓锛宭og涔熷簲璇ョ浉鍚屾墠瀵?
        // rf.logs[entry.Index-firstIndex].Term ?= entry.Term

        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
          //鐩稿悓浣嶇疆鐨刲og 锛屽叾logTerm鐩哥瓑锛屼絾鏄懡浠ゅ嵈涓嶇浉鍚岋紝涓嶇鍚坮aft鐨勫墠鍚戝尮閰嶏紝寮傚父浜嗭紒
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

    // 閿欒鍐欐硶like锛? rf.shrinkLogsToIndex(args.PrevLogIndex)
    // rf.logs = append(rf.logs, args.Entries...)
    // 鍥犱负鍙兘浼氭敹鍒拌繃鏈熺殑log锛侊紒锛?鍥犳杩欓噷鏄ぇ浜庣瓑浜?
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
      // 杩欎釜鍦版柟涓嶈兘鏃犺剳璺熶笂getLastLogIndex()锛屽洜涓哄彲鑳藉瓨鍦╝rgs->leadercommit()钀藉悗浜?getLastLogIndex()鐨勬儏鍐?
    }

    // 棰嗗浼氫竴娆″彂閫佸畬鎵€鏈夌殑鏃ュ織
    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);

    //        DPrintf("[func-AppendEntries-rf{%v}] 鎺ユ敹浜嗘潵鑷妭鐐箋%v}鐨刲og锛屽綋鍓峫astLogIndex{%v}锛岃繑鍥炲€硷細{%v}\n",
    //        rf.me,
    //                args.LeaderId, rf.getLastLogIndex(), reply)

    return;
  } else {
    // 浼樺寲
    // PrevLogIndex 闀垮害鍚堥€傦紝浣嗘槸涓嶅尮閰嶏紝鍥犳寰€鍓嶅鎵?鐭涚浘鐨則erm鐨勭涓€涓厓绱?
    // 涓轰粈涔堣term鐨勬棩蹇楅兘鏄煕鐩剧殑鍛紵涔熶笉涓€瀹氶兘鏄煕鐩剧殑锛屽彧鏄繖涔堜紭鍖栧噺灏憆pc鑰屽凡
    // 锛熶粈涔堟椂鍊檛erm浼氱煕鐩惧憿锛熷緢澶氭儏鍐碉紝姣斿leader鎺ユ敹浜嗘棩蹇椾箣鍚庨┈涓婂氨宕╂簝绛夌瓑
    reply->set_updatenextindex(args->prevlogindex());

    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    // 瀵筓pdateNextIndex寰呬紭鍖? todo  鎵惧埌绗﹀悎鐨則erm鐨勬渶鍚庝竴涓?
    //        DPrintf("[func-AppendEntries-rf{%v}]
    //        鎷掔粷浜嗚妭鐐箋%v}锛屽洜涓簆revLodIndex{%v}鐨刟rgs.term{%v}涓嶅尮閰嶅綋鍓嶈妭鐐圭殑logterm{%v}锛岃繑鍥炲€硷細{%v}\n",
    //                rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
    //                rf.logs[rf.getSlicesIndexFromLogIndex(args.PrevLogIndex)].LogTerm, reply)
    //        DPrintf("[func-AppendEntries-rf{%v}] 杩斿洖鍊? reply.UpdateNextIndex浠巤%v}浼樺寲鍒皗%v}锛屼紭鍖栦簡{%v}\n", rf.me,
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
    //浣跨敤鍖垮悕鍑芥暟鏄洜涓轰紶閫掔閬撶殑鏃跺€欎笉鐢ㄦ嬁閿?
    // todo:濂藉儚蹇呴』鎷块攣锛屽洜涓轰笉鎷块攣鐨勮瘽濡傛灉璋冪敤澶氭applyLog鍑芥暟锛屽彲鑳戒細瀵艰嚧搴旂敤鐨勯『搴忎笉涓€鏍?
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
    //褰撻€変妇鐨勬椂鍊欏畾鏃跺櫒瓒呮椂灏卞繀椤婚噸鏂伴€変妇锛屼笉鐒舵病鏈夐€夌エ灏变細涓€鐩村崱涓?
    //閲嶇珵閫夎秴鏃讹紝term涔熶細澧炲姞鐨?
    m_status = Candidate;
    ///寮€濮嬫柊涓€杞殑閫変妇
    m_currentTerm += 1;
    m_votedFor = m_me;  //鍗虫槸鑷繁缁欒嚜宸辨姇锛屼篃閬垮厤candidate缁欏悓杈堢殑candidate鎶?
    persist();
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);  // 浣跨敤 make_shared 鍑芥暟鍒濆鍖?!! 浜偣
    //	閲嶆柊璁剧疆瀹氭椂鍣?
    m_lastResetElectionTime = now();
    //	鍙戝竷RequestVote RPC
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //鑾峰彇鏈€鍚庝竴涓猯og鐨則erm鍜屼笅鏍?

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      //浣跨敤鍖垮悕鍑芥暟鎵ц閬垮厤鍏舵嬁鍒伴攣

      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                    votedNum);  // 鍒涘缓鏂扮嚎绋嬪苟鎵цb鍑芥暟锛屽苟浼犻€掑弬鏁?
      t.detach();
    }
  }
}

void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader鐨勫績璺冲畾鏃跺櫒瑙﹀彂浜嗕笖鎷垮埌mutex锛屽紑濮嬪彂閫丄E\n", m_me);
    //瀵笷ollower锛堥櫎浜嗚嚜宸卞鐨勬墍鏈夎妭鐐瑰彂閫丄E锛?
    // todo 杩欓噷鑲畾鏄淇敼鐨勶紝鏈€濂戒娇鐢ㄤ竴涓崟鐙殑goruntime鏉ヨ礋璐ｇ鐞嗗彂閫乴og锛屽洜涓哄悗闈㈢殑log鍙戦€佹秹鍙婁紭鍖栦箣绫荤殑
    //鏈€灏戣鍗曠嫭鍐欎竴涓嚱鏁版潵绠＄悊锛岃€屼笉鏄湪杩欎竴鍧?
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader鐨勫績璺冲畾鏃跺櫒瑙﹀彂浜?index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
      //鏃ュ織鍘嬬缉鍔犲叆鍚庤鍒ゆ柇鏄彂閫佸揩鐓ц繕鏄彂閫丄E
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        //                        DPrintf("[func-ticker()-rf{%v}]rf.nextIndex[%v] {%v} <=
        //                        rf.lastSnapshotIncludeIndex{%v},so leaderSendSnapShot", rf.me, i, rf.nextIndex[i],
        //                        rf.lastSnapshotIncludeIndex)
        std::thread t(&Raft::leaderSendSnapShot, this, i);  // 鍒涘缓鏂扮嚎绋嬪苟鎵цb鍑芥暟锛屽苟浼犻€掑弬鏁?
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
          *sendEntryPtr = m_logs[j];  //=鏄彲浠ョ偣杩涘幓鐨勶紝鍙互鐐硅繘鍘荤湅涓媝rotobuf濡備綍閲嶅啓杩欎釜鐨?
        }
      } else {
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item;  //=鏄彲浠ョ偣杩涘幓鐨勶紝鍙互鐐硅繘鍘荤湅涓媝rotobuf濡備綍閲嶅啓杩欎釜鐨?
        }
      }
      int lastLogIndex = getLastLogIndex();
      // leader瀵规瘡涓妭鐐瑰彂閫佺殑鏃ュ織闀跨煭涓嶄竴锛屼絾鏄兘淇濊瘉浠巔revIndex鍙戦€佺洿鍒版渶鍚?
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
      //鏋勯€犺繑鍥炲€?
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);

      std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply);
      t.detach();
    }
    m_lastResetHearBeatTime = now();  // leader鍙戦€佸績璺筹紝灏变笉鏄殢鏈烘椂闂翠簡
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
          HeartBeatTimeout);  //瀹氭椂鏃堕棿娌℃湁涓ヨ皑璁剧疆锛屽洜涓篐eartBeatTimeout姣旈€変妇瓒呮椂涓€鑸皬涓€涓暟閲忕骇锛屽洜姝ゅ氨璁剧疆涓篐eartBeatTimeout浜?
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
      //璇存槑鐫＄湢鐨勮繖娈垫椂闂存湁閲嶇疆瀹氭椂鍣紝閭ｄ箞灏辨病鏈夎秴鏃讹紝鍐嶆鐫＄湢
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
    //        DPrintf("[	applyLog func-rf{%v}	] apply Log,logIndex:%v  锛宭ogTerm锛歿%v},command锛歿%v}\n",
    //        rf.me, rf.lastApplied, rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogTerm,
    //        rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].Command)
  }
  return applyMsgs;
}

// 鑾峰彇鏂板懡浠ゅ簲璇ュ垎閰嶇殑Index
int Raft::getNewCommandIndex() {
  //	濡傛灉len(logs)==0,灏变负蹇収鐨刬ndex+1锛屽惁鍒欎负log鏈€鍚庝竴涓棩蹇?1
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// getPrevLogInfo
// leader璋冪敤锛屼紶鍏ワ細鏈嶅姟鍣╥ndex锛屼紶鍑猴細鍙戦€佺殑AE鐨刾reLogIndex鍜孭revLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  // logs闀垮害涓?杩斿洖0,0锛屼笉鏄?灏辨牴鎹畁extIndex鏁扮粍鐨勬暟鍊艰繑鍥?
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    //瑕佸彂閫佺殑鏃ュ織鏄涓€涓棩蹇楋紝鍥犳鐩存帴杩斿洖m_lastSnapshotIncludeIndex鍜宮_lastSnapshotIncludeTerm
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
    // todo 鏆傛椂涓嶆竻妤氫細涓嶄細瀵艰嚧姝婚攣
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
    //鍚庨潰涓ょ鎯呭喌閮借鎺ユ敹鏃ュ織
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
  //鎴柇鏃ュ織锛屼慨鏀筩ommitIndex鍜宭astApplied
  //鎴柇鏃ュ織鍖呮嫭锛氭棩蹇楅暱浜嗭紝鎴柇涓€閮ㄥ垎锛屾棩蹇楃煭浜嗭紝鍏ㄩ儴娓呯┖锛屽叾瀹炰袱涓槸涓€绉嶆儏鍐?
  //浣嗘槸鐢变簬鐜板湪getSlicesIndexFromLogIndex鐨勫疄鐜帮紝涓嶈兘浼犲叆涓嶅瓨鍦╨ogIndex锛屽惁鍒欎細panic
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

  std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 鍒涘缓鏂扮嚎绋嬪苟鎵цb鍑芥暟锛屽苟浼犻€掑弬鏁?
  t.detach();
  //鐪嬩笅杩欓噷鑳戒笉鑳藉啀浼樺寲
  //    DPrintf("[func-InstallSnapshot-rf{%v}] receive snapshot from {%v} ,LastSnapShotIncludeIndex ={%v} ", rf.me,
  //    args.LeaderId, args.LastSnapShotIncludeIndex)
  //鎸佷箙鍖?
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

void Raft::leaderHearBeatTicker() {
  while (true) {
    //涓嶆槸leader鐨勮瘽灏辨病鏈夊繀瑕佽繘琛屽悗缁搷浣滐紝鍐典笖杩樿鎷块攣锛屽緢褰卞搷鎬ц兘锛岀洰鍓嶆槸鐫＄湢锛屽悗闈㈠啀浼樺寲浼樺寲
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
      //鐫＄湢鐨勮繖娈垫椂闂存湁閲嶇疆瀹氭椂鍣紝娌℃湁瓒呮椂锛屽啀娆＄潯鐪?
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
    return;  //涓棿閲婃斁杩囬攣锛屽彲鑳界姸鎬佸凡缁忔敼鍙樹簡
  }
  //	鏃犺浠€涔堟椂鍊欓兘瑕佸垽鏂璽erm
  if (reply.term() > m_currentTerm) {
    //涓夊彉
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

    //        !!!鍙湁褰撳墠term鏈夋柊鎻愪氦鐨勶紝鎵嶄細鏇存柊commitIndex锛侊紒锛侊紒
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

//杩涙潵鍓嶈淇濊瘉logIndex鏄瓨鍦ㄧ殑锛屽嵆鈮f.lastSnapshotIncludeIndex	锛岃€屼笖灏忎簬绛変簬rf.getLastLogIndex()
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
    //搴旇鍏堟寔涔呭寲锛屽啀鎾ら攢lock
    persist();
  };
  //瀵筧rgs鐨則erm鐨勪笁绉嶆儏鍐靛垎鍒繘琛屽鐞嗭紝澶т簬灏忎簬绛変簬鑷繁鐨則erm閮芥槸涓嶅悓鐨勫鐞?
  // reason: 鍑虹幇缃戠粶鍒嗗尯锛岃绔為€夎€呭凡缁廜utOfDate(杩囨椂锛?
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }
  // fig2:鍙充笅瑙掞紝濡傛灉浠讳綍鏃跺€檙pc璇锋眰鎴栬€呭搷搴旂殑term澶т簬鑷繁鐨則erm锛屾洿鏂皌erm锛屽苟鍙樻垚follower
  if (args->term() > m_currentTerm) {
    //        DPrintf("[	    func-RequestVote-rf(%v)		] : 鍙樻垚follower涓旀洿鏂皌erm
    //        鍥犱负candidate{%v}鐨則erm{%v}> rf{%v}.term{%v}\n ", rf.me, args.CandidateId, args.Term, rf.me,
    //        rf.currentTerm)
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_leaderId = -1;

    //	閲嶇疆瀹氭椂鍣細鏀跺埌leader鐨刟e锛屽紑濮嬮€変妇锛岄€忓嚭绁?
    //杩欐椂鍊欐洿鏂颁簡term涔嬪悗锛寁otedFor涔熻缃负-1
  }
  myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 鍓嶉潰鏍￠獙杩嘺rgs.Term==rf.currentTerm锛岃繖閲屽嵈涓嶇瓑", m_me));
  //	鐜板湪鑺傜偣浠绘湡閮芥槸鐩稿悓鐨?浠绘湡灏忕殑涔熷凡缁忔洿鏂板埌鏂扮殑args鐨則erm浜?锛岃繕闇€瑕佹鏌og鐨則erm鍜宨ndex鏄笉鏄尮閰嶇殑浜?

  int lastLogTerm = getLastLogTerm();
  //鍙湁娌℃姇绁紝涓攃andidate鐨勬棩蹇楃殑鏂扮殑绋嬪害 鈮?鎺ュ彈鑰呯殑鏃ュ織鏂扮殑绋嬪害 鎵嶄細鎺堢エ
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
  // todo 锛?鍟ユ椂鍊欎細鍑虹幇rf.votedFor == args.CandidateId 锛屽氨绠梒andidate閫変妇瓒呮椂鍐嶉€変妇锛屽叾term涔熸槸涓嶄竴鏍风殑鍛€
  //     褰撳洜涓虹綉缁滆川閲忎笉濂藉鑷寸殑璇锋眰涓㈠け閲嶅彂灏辨湁鍙兘锛侊紒锛侊紒
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    //        DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because has voted\n",
    //        rf.me, args.CandidateId)
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  } else {
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();  //璁や负蹇呴』瑕佸湪鎶曞嚭绁ㄧ殑鏃跺€欐墠閲嶇疆瀹氭椂鍣紝
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

// 鎵惧埌index瀵瑰簲鐨勭湡瀹炰笅鏍囦綅缃紒锛侊紒
// 闄愬埗锛岃緭鍏ョ殑logIndex蹇呴』淇濆瓨鍦ㄥ綋鍓嶇殑logs閲岄潰锛堜笉鍖呭惈snapshot锛?
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
  //杩欎釜ok鏄綉缁滄槸鍚︽甯搁€氫俊鐨刼k锛岃€屼笉鏄痳equestVote rpc鏄惁鎶曠エ鐨剅pc
  // ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  // todo
  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 鍚憇erver{%d} 鐧奸€?RequestVote 闁嬪", m_me, m_currentTerm, getLastLogIndex());
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[func-sendRequestVote rf{%d}] 鍚憇erver{%d} 鐧奸€?RequestVote 瀹岀暍锛岃€楁檪:{%d} ms", m_me, m_currentTerm,
          getLastLogIndex(), now() - start);

  if (!ok) {
    return ok;  //涓嶇煡閬撲负浠€涔堜笉鍔犺繖涓殑璇濆鏋滄湇鍔″櫒瀹曟満浼氬嚭鐜伴棶棰樼殑锛岄€氫笉杩?B  todo
  }
  // for !ok {
  //
  //	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  //	//if ok {
  //	//	break
  //	//}
  // } //杩欓噷鏄彂閫佸嚭鍘讳簡锛屼絾鏄笉鑳戒繚璇佷粬涓€瀹氬埌杈?
  //瀵瑰洖搴旇繘琛屽鐞嗭紝瑕佽寰楁棤璁轰粈涔堟椂鍊欐敹鍒板洖澶嶅氨瑕佹鏌erm
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) {
    m_status = Follower;  //涓夊彉锛氳韩浠斤紝term锛屽拰鎶曠エ
    m_currentTerm = reply->term();
    m_votedFor = -1;
    m_leaderId = -1;
    persist();
    return true;
  } else if (reply->term() < m_currentTerm) {
    return true;
  }
  myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

  // todo锛氳繖閲屾病鏈夋寜鍗氬鍐?
  if (!reply->votegranted()) {
    return true;
  }

  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    //鍙樻垚leader
    *votedNum = 0;
    if (m_status == Leader) {
      //濡傛灉宸茬粡鏄痩eader浜嗭紝閭ｄ箞鏄氨鏄簡锛屼笉浼氳繘琛屼笅涓€姝ュ鐞嗕簡k
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 鍚屼竴涓猼erm褰撲袱娆￠瀵硷紝error", m_me, m_currentTerm));
    }
    //	绗竴娆″彉鎴恖eader锛屽垵濮嬪寲鐘舵€佸拰nextIndex銆乵atchIndex
    m_status = Leader;
    m_leaderId = m_me;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;  //鏈夋晥涓嬫爣浠?寮€濮嬶紝鍥犳瑕?1
      m_matchIndex[i] = 0;                //姣忔崲涓€涓瀵奸兘鏄粠0寮€濮嬶紝瑙乫ig2
    }
    std::thread t(&Raft::doHeartBeat, this);  //椹笂鍚戝叾浠栬妭鐐瑰鍛婅嚜宸卞氨鏄痩eader
    t.detach();

    persist();
  }
  return true;
}

bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply) {
  //杩欎釜ok鏄綉缁滄槸鍚︽甯搁€氫俊鐨刼k锛岃€屼笉鏄痳equestVote rpc鏄惁鎶曠エ鐨剅pc
  // 濡傛灉缃戠粶涓嶉€氱殑璇濊偗瀹氭槸娌℃湁杩斿洖鐨勶紝涓嶇敤涓€鐩撮噸璇?
  // todo锛?paper涓?.3鑺傜涓€娈垫湯灏炬彁鍒帮紝濡傛灉append澶辫触搴旇涓嶆柇鐨剅etries ,鐩村埌杩欎釜log鎴愬姛鐨勮store
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

  //瀵箁eply杩涜澶勭悊
  // 瀵逛簬rpc閫氫俊锛屾棤璁轰粈涔堟椂鍊欓兘瑕佹鏌erm
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
    //濡傛灉涓嶆槸leader锛岄偅涔堝氨涓嶈瀵硅繑鍥炵殑鎯呭喌杩涜澶勭悊浜?
    return ok;
  }
  // term鐩哥瓑

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
  if (!reply->success()) {
    //鏃ュ織涓嶅尮閰嶏紝姝ｅ父鏉ヨ灏辨槸index瑕佸線鍓?1锛屾棦鐒惰兘鍒拌繖閲岋紝绗竴涓棩蹇楋紙idnex =
    // 1锛夊彂閫佸悗鑲畾鏄尮閰嶇殑锛屽洜姝や笉鐢ㄨ€冭檻鍙樻垚璐熸暟 鍥犱负鐪熸鐨勭幆澧冧笉浼氱煡閬撴槸鏈嶅姟鍣ㄥ畷鏈鸿繕鏄彂鐢熺綉缁滃垎鍖轰簡
    if (reply->updatenextindex() != -100) {
      // todo:寰呮€荤粨锛屽氨绠梩erm鍖归厤锛屽け璐ョ殑鏃跺€檔extIndex涔熶笉鏄収鍗曞叏鏀剁殑锛屽洜涓哄鏋滃彂鐢焤pc寤惰繜锛宭eader鐨則erm鍙兘浠庝笉绗﹀悎term瑕佹眰
      //鍙樺緱绗﹀悎term瑕佹眰
      //浣嗘槸涓嶈兘鐩存帴璧嬪€紃eply.UpdateNextIndex
      DPrintf("[func -sendAppendEntries  rf{%d}]  杩斿洖鐨勬棩蹇梩erm鐩哥瓑锛屼絾鏄笉鍖归厤锛屽洖缂﹏extIndex[%d]锛歿%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();  //澶辫触鏄笉鏇存柊mathIndex鐨?
    }
    //	鎬庝箞瓒婂啓瓒婃劅瑙塺f.nextIndex鏁扮粍鏄啑浣欑殑鍛紝鐪嬩笅璁烘枃fig2锛屽叾瀹炰笉鏄啑浣欑殑
  } else {
    // rf.matchIndex[server] = len(args.Entries) //鍙杩斿洖涓€涓搷搴斿氨瀵瑰叾matchIndex搴旇瀵瑰叾鍋氬嚭鍙嶅簲锛?
    //浣嗘槸杩欎箞淇敼鏄湁闂鐨勶紝濡傛灉瀵规煇涓秷鎭彂閫佷簡澶氶亶锛堝績璺虫椂灏变細鍐嶅彂閫侊級锛岄偅涔堜竴鏉℃秷鎭細瀵艰嚧n娆′笂娑?
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

  // leader搴旇涓嶅仠鐨勫悜鍚勪釜Follower鍙戦€丄E鏉ョ淮鎶ゅ績璺冲拰淇濇寔鏃ュ織鍚屾锛岀洰鍓嶇殑鍋氭硶鏄柊鐨勫懡浠ゆ潵浜嗕笉浼氱洿鎺ユ墽琛岋紝鑰屾槸绛夊緟leader鐨勫績璺宠Е鍙?
  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
  // rf.timer.Reset(10) //鎺ユ敹鍒板懡浠ゅ悗椹笂缁檉ollower鍙戦€?鏀规垚杩欐牱涓嶇煡涓轰綍浼氬嚭鐜伴棶棰橈紝寰呬慨姝?todo
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
    // rf.commitIndex = rf.lastSnapshotIncludeIndex   todo 锛氬穿婧冩仮澶嶄负浣曚笉鑳借鍙朿ommitIndex
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
  auto lastLogIndex = getLastLogIndex();  //涓轰簡妫€鏌napshot鍓嶅悗鏃ュ織鏄惁涓€鏍凤紝闃叉澶氭埅鍙栨垨鑰呭皯鎴彇鏃ュ織

  //鍒堕€犲畬姝ゅ揩鐓у悗鍓╀綑鐨勬墍鏈夋棩蹇?
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // todo :杩欑鍐欐硶鏈夌偣绗紝寰呮敼杩涳紝鑰屼笖鏈夊唴瀛樻硠婕忕殑椋庨櫓
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    //娉ㄦ剰鏈?锛屽洜涓鸿鎷垮埌鏈€鍚庝竴涓棩蹇?
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  // rf.lastApplied = index //lastApplied 鍜?commit搴斾笉搴旇鏀瑰彉鍛紵锛燂紵 涓轰粈涔? 涓嶅簲璇ユ敼鍙樺惂
  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}
