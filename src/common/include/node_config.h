#ifndef NODE_CONFIG_H
#define NODE_CONFIG_H

#include <string>
#include <unordered_map>

class NodeConfig {
 public:
  void LoadConfigFile(const char* configFile);
  std::string Load(const std::string& key) const;

 private:
  static void Trim(std::string& value);

  std::unordered_map<std::string, std::string> m_configMap;
};

#endif  // NODE_CONFIG_H
