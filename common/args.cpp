#include "common/args.h"

#include <cerrno>
#include <cstdlib>
#include <stdexcept>

namespace adstack {

Args::Args(int argc, char** argv) : program_(argc > 0 ? argv[0] : "") {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--", 0) != 0) {
      order_.push_back(a);  // positional: no tool takes one, unknown() reports it
      continue;
    }
    auto eq = a.find('=');
    if (eq != std::string::npos) {
      kv_[a.substr(0, eq)] = a.substr(eq + 1);
      order_.push_back(a.substr(0, eq));
    } else if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0) {
      kv_[a] = argv[++i];
      order_.push_back(a);
    } else {
      kv_[a] = "";
      order_.push_back(a);
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
  if (it == kv_.end()) return def;
  const std::string& v = it->second;
  char* end = nullptr;
  errno = 0;
  const long long x = std::strtoll(v.c_str(), &end, 10);
  if (v.empty() || end != v.c_str() + v.size() || errno == ERANGE) {
    throw std::invalid_argument(flag + ": expected an integer, got '" + v + "'");
  }
  return x;
}

double Args::dbl(const std::string& flag, double def) const {
  auto it = kv_.find(flag);
  if (it == kv_.end()) return def;
  const std::string& v = it->second;
  char* end = nullptr;
  errno = 0;
  const double x = std::strtod(v.c_str(), &end);
  if (v.empty() || end != v.c_str() + v.size() || errno == ERANGE) {
    throw std::invalid_argument(flag + ": expected a number, got '" + v + "'");
  }
  return x;
}

std::vector<std::string> Args::unknown(std::initializer_list<const char*> known) const {
  std::vector<std::string> out;
  for (const std::string& a : order_) {
    bool ok = false;
    for (const char* k : known) {
      if (a == k) { ok = true; break; }
    }
    if (!ok) out.push_back(a);
  }
  return out;
}

}  // namespace adstack
