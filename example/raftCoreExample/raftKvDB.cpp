//
// Created by swx on 23-12-28.
//
#include <climits>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <unistd.h>

#include <kvServer.h>
#include "node_config.h"
#include "raft.h"

void ShowArgsHelp();

namespace {

int CountNodesInConfig(const NodeConfig& config) {
  int count = 0;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    const std::string ipKey = "node" + std::to_string(i) + "ip";
    if (config.Load(ipKey).empty()) {
      break;
    }
    ++count;
  }
  return count;
}

bool LoadPortsFromConfig(const char* configFile, int nodeNum, std::vector<unsigned short>* ports) {
  NodeConfig config;
  config.LoadConfigFile(configFile);
  ports->clear();
  ports->reserve(static_cast<size_t>(nodeNum));
  for (int i = 0; i < nodeNum; ++i) {
    const std::string portKey = "node" + std::to_string(i) + "port";
    const std::string portStr = config.Load(portKey);
    if (portStr.empty()) {
      std::cerr << "missing " << portKey << " in " << configFile << std::endl;
      return false;
    }
    const long port = std::strtol(portStr.c_str(), nullptr, 10);
    if (port <= 0 || port > 65535) {
      std::cerr << "invalid port in " << portKey << ": " << portStr << std::endl;
      return false;
    }
    ports->push_back(static_cast<unsigned short>(port));
  }
  return true;
}

void WriteRandomConfig(const std::string& configFileName, int nodeNum, unsigned short startPort) {
  std::ofstream file(configFileName, std::ios::out | std::ios::trunc);
  if (!file.is_open()) {
    std::cout << "failed to open " << configFileName << std::endl;
    exit(EXIT_FAILURE);
  }
  for (int i = 0; i < nodeNum; i++) {
    file << "node" << i << "ip=127.0.0.1" << std::endl;
    file << "node" << i << "port=" << startPort + static_cast<unsigned short>(i) << std::endl;
  }
  file.close();
  std::cout << "generated raft node config: " << configFileName << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }

  int c = 0;
  int nodeNum = 0;
  std::string configFileName;
  bool useFixedConfig = false;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(10000, 29999);
  unsigned short startPort = dis(gen);

  while ((c = getopt(argc, argv, "n:f:u")) != -1) {
    switch (c) {
      case 'n':
        nodeNum = atoi(optarg);
        break;
      case 'f':
        configFileName = optarg;
        break;
      case 'u':
        useFixedConfig = true;
        break;
      default:
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
  }

  if (configFileName.empty()) {
    std::cerr << "missing -f <configFileName>" << std::endl;
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }

  std::vector<unsigned short> ports;

  if (useFixedConfig) {
    if (nodeNum <= 0) {
      NodeConfig probe;
      probe.LoadConfigFile(configFileName.c_str());
      nodeNum = CountNodesInConfig(probe);
      if (nodeNum <= 0) {
        std::cerr << "no nodes found in config: " << configFileName << std::endl;
        exit(EXIT_FAILURE);
      }
    }
    if (!LoadPortsFromConfig(configFileName.c_str(), nodeNum, &ports)) {
      exit(EXIT_FAILURE);
    }
    std::cout << "using existing config: " << configFileName << " (" << nodeNum << " nodes)" << std::endl;
  } else {
    if (nodeNum <= 0) {
      std::cerr << "missing -n <nodeNum> (or use -u with a fixed config)" << std::endl;
      ShowArgsHelp();
      exit(EXIT_FAILURE);
    }
    WriteRandomConfig(configFileName, nodeNum, startPort);
    ports.resize(static_cast<size_t>(nodeNum));
    for (int i = 0; i < nodeNum; i++) {
      ports[static_cast<size_t>(i)] = startPort + static_cast<unsigned short>(i);
    }
  }

  for (int i = 0; i < nodeNum; i++) {
    const unsigned short port = ports[static_cast<size_t>(i)];
    std::cout << "fork raftkv node " << i << " port " << port << " (parent pid " << getpid() << ")\n";
    pid_t pid = fork();
    if (pid == 0) {
      const useconds_t stagger =
          static_cast<useconds_t>((nodeNum - 1 - i) * 200000);  // 200ms per rank, last node: 0
      if (stagger > 0) {
        usleep(stagger);
      }

      auto kvServer = std::make_unique<KvServer>(i, 500, configFileName, static_cast<short>(port));
      if (!kvServer->Start()) {
        exit(EXIT_FAILURE);
      }
      kvServer->Wait();
      return 0;
    }
    if (pid > 0) {
      continue;
    }
    std::cerr << "Failed to create child process." << std::endl;
    exit(EXIT_FAILURE);
  }
  pause();
  return 0;
}

void ShowArgsHelp() {
  std::cout << "format: raftCoreRun -f <configFile> [-n <nodeNum>] [-u]\n"
            << "  default: generate random ports and write config\n"
            << "  -u: use existing config (fixed ports); -n optional if nodes are in file\n";
}
