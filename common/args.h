#pragma once
// Minimal "--flag value" / "--flag" parser so tools do not need a dependency.
//
// Numbers are parsed strictly: i64() and dbl() throw std::invalid_argument
// when the value is empty, has trailing characters ("1.5" for an integer,
// "60s") or is out of range, instead of silently using whatever prefix
// strtoll could read. A tool that wants misspelled flags rejected too lists
// the flags it understands and calls unknown() (streamkit, gen, oracle and
// evdump do); without that a typo such as --threds would be ignored and the
// run would use the default with exit 0.

#include <cstdint>
#include <initializer_list>
#include <map>
#include <string>
#include <vector>

namespace adstack {

class Args {
 public:
  Args(int argc, char** argv);

  bool has(const std::string& flag) const;
  std::string str(const std::string& flag, const std::string& def) const;
  int64_t i64(const std::string& flag, int64_t def) const;
  double dbl(const std::string& flag, double def) const;

  // Every flag given on the command line that is not in `known`, plus every
  // stray positional argument, in command-line order. Empty means the
  // command line is fully understood.
  std::vector<std::string> unknown(std::initializer_list<const char*> known) const;

  const std::string& program() const { return program_; }

 private:
  std::string program_;
  std::map<std::string, std::string> kv_;
  std::vector<std::string> order_;  // flags and positionals as given
};

}  // namespace adstack
