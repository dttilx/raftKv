#include "node_config.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

void NodeConfig::LoadConfigFile(const char* configFile) {
  std::ifstream input(configFile);
  if (!input.is_open()) {
    std::cerr << configFile << " is not exist!" << std::endl;
    std::exit(EXIT_FAILURE);
  }

  std::string line;
  while (std::getline(input, line)) {
    Trim(line);
    if (line.empty() || line[0] == '#') {
      continue;
    }

    auto separator = line.find('=');
    if (separator == std::string::npos) {
      continue;
    }

    std::string key = line.substr(0, separator);
    std::string value = line.substr(separator + 1);
    Trim(key);
    Trim(value);
    if (!key.empty()) {
      m_configMap[key] = value;
    }
  }
}

std::string NodeConfig::Load(const std::string& key) const {
  auto it = m_configMap.find(key);
  if (it == m_configMap.end()) {
    return "";
  }
  return it->second;
}

void NodeConfig::Trim(std::string& value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    value.clear();
    return;
  }

  const auto end = value.find_last_not_of(" \t\r\n");
  value = value.substr(begin, end - begin + 1);
}
