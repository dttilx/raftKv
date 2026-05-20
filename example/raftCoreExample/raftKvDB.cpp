//
// Created by swx on 23-12-28.
//
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>

#include <unistd.h>

#include <kvServer.h>
#include "raft.h"

void ShowArgsHelp();

int main(int argc, char **argv) {
  //////////////////////////////////ËØªÂè?Â?Ω‰ª§Âè?Ê?∞Ôº?Ë??Á?πÊ?∞È?è„?ÅÂ??Â?•raftË??Á?πË??Á?π‰ø°ÊÅØÂ?∞Â?™‰∏™Ê??‰ª?
  if (argc < 2) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }
  int c = 0;
  int nodeNum = 0;
  std::string configFileName;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(10000, 29999);
  unsigned short startPort = dis(gen);
  while ((c = getopt(argc, argv, "n:f:")) != -1) {
    switch (c) {
      case 'n':
        nodeNum = atoi(optarg);
        break;
      case 'f':
        configFileName = optarg;
        break;
      default:
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
  }
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

  for (int i = 0; i < nodeNum; i++) {
    short port = startPort + static_cast<short>(i);
    std::cout << "fork raftkv node " << i << " port " << port << " (parent pid " << getpid() << ")\n";
    pid_t pid = fork();
    if (pid == 0) {
      // Stagger Listen so higher-index peers (larger ports) are up before lower-index
      // nodes run Raft RPCs. The old sleep(1) after each fork delayed child i+1 by 1s,
      // so node 0 always started elections ~2s before node N-1 bound its gRPC port.
      const useconds_t stagger =
          static_cast<useconds_t>((nodeNum - 1 - i) * 200000);  // 200ms per rank, last node: 0
      if (stagger > 0) {
        usleep(stagger);
      }

      auto kvServer = std::make_unique<KvServer>(i, 500, configFileName, port);
      if (!kvServer->Start()) {
        exit(EXIT_FAILURE);
      }
      kvServer->Wait();
      return 0;
    } else if (pid > 0) {
      // Do not sleep between forks; see comment in child branch.
    } else {
      // Â¶?Ê??Â??Âª∫Ëø?Á®?Â§±Ë¥•
      std::cerr << "Failed to create child process." << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  pause();
  return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }
