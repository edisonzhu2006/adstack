// criteo2events: convert the Criteo Attribution Modeling dataset (real ad
// impressions, clicks and conversions from a 30-day sample of Criteo live
// traffic) into the adstack event format.
//
// Dataset: https://ailab.criteo.com/criteo-attribution-modeling-bidding-dataset/
// ~16M rows, TSV with header. Relevant columns:
//   timestamp, uid, campaign, conversion, conversion_timestamp, conversion_id,
//   attribution, click, click_pos, click_nb, cost, cpo, time_since_last_click,
//   cat1..cat9
// Each row is one displayed banner. We emit an impression per row, a click
// if click=1 (carrying the cost), and a conversion at conversion_timestamp
// if conversion=1. Timestamps are seconds relative to the dataset start, so
// --base-epoch-us anchors them.
//
//   gunzip -c criteo_attribution_dataset.tsv.gz | criteo2events --out criteo.ads

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/args.h"
#include "common/event.h"

using namespace adstack;

namespace {

constexpr int64_t kUs = 1'000'000;
constexpr int64_t kDefaultEpochUs = 1'767'225'600LL * kUs;  // 2026-01-01T00:00:00Z

uint64_t fnv1a(const std::string& s, uint64_t h = 1469598103934665603ULL) {
  for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
  return h;
}
uint64_t mix(uint64_t a, uint64_t b) {
  a ^= b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2);
  return a;
}

std::vector<std::string> split_tab(const std::string& line) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : line) {
    if (c == '\t') { out.push_back(cur); cur.clear(); }
    else cur.push_back(c);
  }
  out.push_back(cur);
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (!args.has("--out")) {
    std::fprintf(stderr, "criteo2events --out FILE [--base-epoch-us T] [--limit N]  < dataset.tsv\n");
    return 2;
  }
  const int64_t base_us = args.i64("--base-epoch-us", kDefaultEpochUs);
  const int64_t limit = args.i64("--limit", -1);

  std::ios::sync_with_stdio(false);
  std::string line;
  if (!std::getline(std::cin, line)) { std::fprintf(stderr, "empty input\n"); return 1; }

  std::unordered_map<std::string, size_t> col;
  auto header = split_tab(line);
  for (size_t i = 0; i < header.size(); ++i) col[header[i]] = i;
  for (const char* need : {"timestamp", "campaign", "conversion", "conversion_timestamp",
                           "click", "cost", "cat1", "cat2", "cat3"}) {
    if (!col.count(need)) { std::fprintf(stderr, "missing column: %s\n", need); return 1; }
  }

  EventWriter w(args.str("--out", "-"));
  uint64_t rows = 0, clicks = 0, convs = 0;

  while (std::getline(std::cin, line)) {
    if (limit >= 0 && static_cast<int64_t>(rows) >= limit) break;
    auto f = split_tab(line);
    if (f.size() < header.size()) continue;
    ++rows;

    const int64_t ts_s = std::strtoll(f[col["timestamp"]].c_str(), nullptr, 10);
    const uint64_t campaign_h = fnv1a(f[col["campaign"]]);
    const uint64_t advertiser_h = fnv1a(f[col["cat1"]]);
    const uint64_t video_h = fnv1a(f[col["cat2"]]);
    const uint64_t device_h = fnv1a(f[col["cat3"]]);
    const double cost = std::strtod(f[col["cost"]].c_str(), nullptr);
    const bool click = f[col["click"]] == "1";
    const bool conv = f[col["conversion"]] == "1";
    const int64_t conv_ts_s = std::strtoll(f[col["conversion_timestamp"]].c_str(), nullptr, 10);

    Event e;
    e.event_time_us = base_us + ts_s * kUs;
    e.ingest_time_us = e.event_time_us;  // dataset has no delivery time
    e.advertiser_id = static_cast<uint32_t>(advertiser_h);
    e.campaign_id = static_cast<uint32_t>(campaign_h);
    e.video_id = video_h;
    e.country = 0;  // not in the dataset
    e.device = static_cast<uint8_t>(device_h % 3);

    e.event_id = mix(rows, 0);
    e.event_type = 0;
    e.cost_micros = 0;
    w.write(e);

    if (click) {
      e.event_id = mix(rows, 1);
      e.event_type = 1;
      e.cost_micros = static_cast<int64_t>(cost * 1e6);
      w.write(e);
      ++clicks;
    }
    if (conv && conv_ts_s >= 0) {
      e.event_id = mix(rows, 2);
      e.event_type = 2;
      e.event_time_us = base_us + conv_ts_s * kUs;
      e.ingest_time_us = e.event_time_us;
      e.cost_micros = 0;
      w.write(e);
      ++convs;
    }
  }
  w.flush();
  std::fprintf(stderr, "criteo2events: %llu rows -> %llu events (%llu clicks, %llu conversions)\n",
               static_cast<unsigned long long>(rows), static_cast<unsigned long long>(w.count()),
               static_cast<unsigned long long>(clicks), static_cast<unsigned long long>(convs));
  return 0;
}
