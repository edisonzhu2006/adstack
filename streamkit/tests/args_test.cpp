// Unit tests for common/args (here rather than in tests/ because the root
// CMakeLists.txt lists that directory's files explicitly; this one is
// globbed). Review fix 2026-10-24: misspelled flags and malformed numbers
// used to be ignored or truncated silently with exit 0.

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

#include "common/args.h"

using namespace adstack;

namespace {

Args parse(std::vector<std::string> v) {
  static std::vector<std::string> keep;  // argv must outlive Args' constructor only
  keep = std::move(v);
  std::vector<char*> argv;
  for (auto& s : keep) argv.push_back(s.data());
  return Args(static_cast<int>(argv.size()), argv.data());
}

}  // namespace

TEST(Args, ValuesSwitchesAndEqualsForm) {
  const Args a = parse({"prog", "--threads", "4", "--resume", "--window-s=300", "--lateness-s", "-1"});
  EXPECT_EQ(a.i64("--threads", 1), 4);
  EXPECT_TRUE(a.has("--resume"));
  EXPECT_EQ(a.i64("--window-s", 60), 300);
  EXPECT_EQ(a.i64("--missing", 9), 9);
  // "-1" does not start with "--", so it is a value, not a flag.
  EXPECT_EQ(a.i64("--lateness-s", 0), -1);
}

TEST(Args, MalformedNumbersAreErrorsNotPrefixes) {
  const Args a = parse({"prog", "--lateness-s", "1.5", "--threads", "4x", "--n", "", "--big",
                        "99999999999999999999", "--rate", "2e5", "--frac", "0.5q"});
  EXPECT_THROW(a.i64("--lateness-s", 0), std::invalid_argument);  // used to run as 1
  EXPECT_THROW(a.i64("--threads", 1), std::invalid_argument);
  EXPECT_THROW(a.i64("--n", 1), std::invalid_argument);
  EXPECT_THROW(a.i64("--big", 1), std::invalid_argument);
  EXPECT_DOUBLE_EQ(a.dbl("--rate", 0), 2e5);
  EXPECT_THROW(a.dbl("--frac", 0), std::invalid_argument);
  EXPECT_DOUBLE_EQ(a.dbl("--lateness-s", 0), 1.5);  // a real number where one is expected
  EXPECT_THROW(a.i64("--rate", 0), std::invalid_argument);  // "2e5" is not an integer
}

TEST(Args, UnknownReportsMisspelledFlagsAndStrayWords) {
  // The NOTES zsh incident: --lateness and --threds were ignored, the run
  // used lateness 0 and the default thread count, and exited 0.
  const Args a = parse({"prog", "--in", "x.ads", "--lateness", "60", "--threds", "4", "stray"});
  const auto bad = a.unknown({"--in", "--out", "--lateness-s", "--threads"});
  ASSERT_EQ(bad.size(), 3u);
  EXPECT_EQ(bad[0], "--lateness");
  EXPECT_EQ(bad[1], "--threds");
  EXPECT_EQ(bad[2], "stray");
  EXPECT_TRUE(parse({"prog", "--in", "x", "--threads=2"}).unknown({"--in", "--threads"}).empty());
}
