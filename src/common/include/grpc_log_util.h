#ifndef RAFTKV_GRPC_LOG_UTIL_H
#define RAFTKV_GRPC_LOG_UTIL_H

#include <chrono>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>

namespace raftkv_grpc_log {

inline bool VerboseEnabled() {
  const char* v = std::getenv("RAFTKV_VERBOSE_GRPC");
  return v != nullptr && v[0] != '\0' && v[0] != '0';
}

inline void LogRpcFailure(const std::string& target, const char* method, const grpc::Status& status) {
  if (status.ok()) {
    return;
  }
  if (!VerboseEnabled()) {
    static std::mutex mu;
    static std::unordered_map<std::string, std::chrono::steady_clock::time_point> lastLog;
    const std::string key = std::string(method) + "@" + target;
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mu);
    auto it = lastLog.find(key);
    if (it != lastLog.end() &&
        now - it->second < std::chrono::seconds(5)) {
      return;
    }
    lastLog[key] = now;
    std::cerr << "[grpc][" << method << "] target=" << target << " code=" << status.error_code()
              << " message=" << status.error_message()
              << " (further failures suppressed for 5s; set RAFTKV_VERBOSE_GRPC=1 for all)" << std::endl;
    return;
  }
  std::cerr << "[grpc][" << method << "] target=" << target << " code=" << status.error_code()
            << " message=" << status.error_message() << std::endl;
}

}  // namespace raftkv_grpc_log

#endif
