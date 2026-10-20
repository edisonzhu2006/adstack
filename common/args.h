#pragma once
// Minimal "--flag value" / "--flag" parser so tools do not need a dependency.

#include <cstdint>
#include <map>
#include <string>

namespace adstack {

class Args {
 public:
  Args(int argc, char** argv);

  bool has(const std::string& flag) const;
  std::string str(const std::string& flag, const std::string& def) const;
  int64_t i64(const std::string& flag, int64_t def) const;
  double dbl(const std::string& flag, double def) const;

  const std::string& program() const { return program_; }

 private:
  std::string program_;
  std::map<std::string, std::string> kv_;
};

}  // namespace adstack
