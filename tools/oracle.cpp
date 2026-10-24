// oracle: the obviously-correct batch recompute. Reads a whole event file,
// drops duplicate event_ids, buckets by (campaign, window) on event_time and
// prints sorted CSV. Single-threaded and deliberately dumb: its only job is
// to be the ground truth streamkit is diffed against.
//
// Windows are sliding: length --window-s, advancing every --slide-s. The
// default slide equals the window, which is a tumbling window. Membership
// comes from common/agg.h so the oracle and streamkit cannot disagree on it.
//
//   oracle --in events.ads --out truth.csv [--window-s 60] [--slide-s WINDOW]

#include <cstdio>
#include <exception>
#include <map>
#include <unordered_set>

#include "common/agg.h"
#include "common/args.h"
#include "common/event.h"

using namespace adstack;

namespace {

int run(const Args& args) {
  const int64_t window_s = args.i64("--window-s", 60);
  const int64_t slide_s = args.i64("--slide-s", window_s);
  if (window_s < 1 || window_s > kMaxDurationUs / 1'000'000 || slide_s < 1 || slide_s > window_s) {
    std::fprintf(stderr, "oracle: need 1 <= --slide-s <= --window-s <= %lld\n",
                 static_cast<long long>(kMaxDurationUs / 1'000'000));
    return 2;
  }
  const int64_t window_us = window_s * 1'000'000;
  const int64_t slide_us = slide_s * 1'000'000;

  EventReader r(args.str("--in", ""));
  std::unordered_set<uint64_t> seen;
  seen.reserve(1 << 20);
  std::map<WindowKey, Agg> windows;

  Event e;
  uint64_t dups = 0;
  while (r.next(e)) {
    // Same range rule as streamkit (common/agg.h): outside it the window
    // arithmetic would overflow, so the record is an error, not an event.
    check_event_time(e);
    if (!seen.insert(e.event_id).second) { ++dups; continue; }
    for_each_window_start(e.event_time_us, window_us, slide_us, [&](int64_t ws) {
      windows[{ws, e.campaign_id}].add(e);
    });
  }

  const std::string out = args.str("--out", "-");
  FILE* f = out == "-" ? stdout : std::fopen(out.c_str(), "w");
  if (!f) { std::perror("oracle: open out"); return 1; }
  write_csv_header(f);
  for (const auto& [k, a] : windows) write_csv_row(f, k, a);
  if (std::fflush(f) != 0 || (f != stdout && std::fclose(f) != 0)) {
    std::fprintf(stderr, "oracle: write failed\n");
    return 1;
  }

  std::fprintf(stderr, "oracle: %llu events, %llu dups dropped, %zu windows\n",
               static_cast<unsigned long long>(r.count()),
               static_cast<unsigned long long>(dups), windows.size());
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  const auto bad = args.unknown({"--in", "--out", "--window-s", "--slide-s"});
  if (!bad.empty()) {
    std::fprintf(stderr, "oracle: unknown argument '%s'\n", bad.front().c_str());
    return 2;
  }
  if (!args.has("--in")) {
    std::fprintf(stderr, "oracle --in FILE [--out FILE] [--window-s 60] [--slide-s WINDOW]\n");
    return 2;
  }
  try {
    return run(args);
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "oracle: %s\n", ex.what());
    return 1;
  }
}
