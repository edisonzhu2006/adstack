#include "common/args.h"

#include <cstdlib>

namespace adstack {

Args::Args(int argc, char** argv) : program_(argc > 0 ? argv[0] : "") {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--", 0) != 0) continue;
    auto eq = a.find('=');
    if (eq != std::string::npos) {
      kv_[a.substr(0, eq)] = a.substr(eq + 1);
    } else if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0) {
      kv_[a] = argv[++i];
    } else {
      kv_[a] = "";
    }
  }
}

bool Args::has(const std::string& flag) const { return kv_.count(flag) > 0; }

std::string Args::str(const std::string& flag, const std::string& def) const {
  auto it = kv_.find(flag);
  return it == kv_.end() ? def : it->second;
}

int64_t Args::i64(const std::string& flag, int64_t def) const {
  auto it = kv_.find(flag);
  return it == kv_.end() ? def : std::strtoll(it->second.c_str(), nullptr, 10);
}

double Args::dbl(const std::string& flag, double def) const {
  auto it = kv_.find(flag);
  return it == kv_.end() ? def : std::strtod(it->second.c_str(), nullptr);
}

}  // namespace adstack
