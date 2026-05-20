//
// Created by swx on 23-5-30.
//
#include "Persister.h"
#include "util.h"

#include <iterator>

void Persister::Save(const std::string raftstate, const std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  clearRaftStateAndSnapshot();
  m_raftStateOutStream << raftstate;
  m_snapshotOutStream << snapshot;
  m_raftStateOutStream.flush();
  m_snapshotOutStream.flush();
  m_raftStateSize = raftstate.size();
}

std::string Persister::ReadSnapshot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }

  DEFER {
    m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);  //榛樿鏄拷鍔?
  };
  std::fstream ifs(m_snapshotFileName, std::ios_base::in);
  if (!ifs.good()) {
    return "";
  }
  std::string snapshot((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  ifs.close();
  return snapshot;
}

void Persister::SaveRaftState(const std::string &data) {
  std::lock_guard<std::mutex> lg(m_mtx);
  clearRaftState();
  m_raftStateOutStream << data;
  m_raftStateOutStream.flush();
  m_raftStateSize = data.size();
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lg(m_mtx);

  return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lg(m_mtx);

  std::fstream ifs(m_raftStateFileName, std::ios_base::in);
  if (!ifs.good()) {
    return "";
  }
  std::string snapshot((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  ifs.close();
  return snapshot;
}

Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0) {
  /**
   * 妫€鏌ユ枃浠剁姸鎬佸苟娓呯┖鏂囦欢
   */
  bool fileOpenFlag = true;
  std::fstream file(m_raftStateFileName, std::ios::out | std::ios::app);
  if (file.is_open()) {
    file.close();
  } else {
    fileOpenFlag = false;
  }
  file = std::fstream(m_snapshotFileName, std::ios::out | std::ios::app);
  if (file.is_open()) {
    file.close();
  } else {
    fileOpenFlag = false;
  }
  if (!fileOpenFlag) {
    DPrintf("[func-Persister::Persister] file open error");
  }
  /**
   * 缁戝畾娴?
   */
  {
    std::fstream ifs(m_raftStateFileName, std::ios_base::in | std::ios_base::binary | std::ios_base::ate);
    if (ifs.good()) {
      m_raftStateSize = ifs.tellg();
    }
  }
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app);
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);
}

Persister::~Persister() {
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
}

void Persister::clearRaftState() {
  m_raftStateSize = 0;
  // 鍏抽棴鏂囦欢娴?
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  // 閲嶆柊鎵撳紑鏂囦欢娴佸苟娓呯┖鏂囦欢鍐呭
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearSnapshot() {
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearRaftStateAndSnapshot() {
  clearRaftState();
  clearSnapshot();
}

