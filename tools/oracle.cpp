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
#include <map>
#include <unordered_set>

#include "common/agg.h"
#include "common/args.h"
#include "common/event.h"

using namespace adstack;

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (!args.has("--in")) {
    std::fprintf(stderr, "oracle --in FILE [--out FILE] [--window-s 60] [--slide-s WINDOW]\n");
    return 2;
  }
  const int64_t window_us = args.i64("--window-s", 60) * 1'000'000;
  const int64_t slide_us = args.i64("--slide-s", window_us / 1'000'000) * 1'000'000;
  if (window_us <= 0 || slide_us <= 0 || slide_us > window_us) {
    std::fprintf(stderr, "oracle: need 1 <= --slide-s <= --window-s\n");
    return 2;
  }

  EventReader r(args.str("--in", ""));
  std::unordered_set<uint64_t> seen;
  seen.reserve(1 << 20);
  std::map<WindowKey, Agg> windows;

  Event e;
  uint64_t dups = 0;
  while (r.next(e)) {
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
  if (f != stdout) std::fclose(f);

  std::fprintf(stderr, "oracle: %llu events, %llu dups dropped, %zu windows\n",
               static_cast<unsigned long long>(r.count()),
               static_cast<unsigned long long>(dups), windows.size());
  return 0;
}
