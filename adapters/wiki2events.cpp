// wiki2events: turn Wikimedia's public real-time edit stream into adstack
// events, so streamkit can be run on live, genuinely out-of-order traffic
// with no account or API key.
//
// Mapping (documented, not pretended to be ads):
//   event_time   = change timestamp        ingest_time = now
//   event_type   = edit:0  new:1  log:2    (categorize is skipped)
//   campaign_id  = hash(wiki)               advertiser_id = hash(server_name)
//   video_id     = hash(title)              device = bot ? 2 : 0
//   cost_micros  = bytes added (length.new - length.old)
//   event_id     = hash(meta.id)            (stable uuid => replays dedup correctly)
//
//   curl -sN https://stream.wikimedia.org/v2/stream/recentchange \
//     | wiki2events --out - | streamkit --in - --out live.csv --window-s 10 --lateness-s 5

#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>

#include <nlohmann/json.hpp>

#include "common/args.h"
#include "common/event.h"

using namespace adstack;
using json = nlohmann::json;

namespace {

uint64_t fnv1a(const std::string& s) {
  uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
  return h;
}

int64_t now_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (!args.has("--out")) {
    std::fprintf(stderr, "wiki2events --out FILE  < sse-stream\n");
    return 2;
  }
  EventWriter w(args.str("--out", "-"));

  std::ios::sync_with_stdio(false);
  std::string line;
  uint64_t parsed = 0, skipped = 0;
  while (std::getline(std::cin, line)) {
    if (line.rfind("data: ", 0) != 0) continue;  // SSE: ignore event:/id:/blank lines
    json j;
    try {
      j = json::parse(line.substr(6));
    } catch (const std::exception&) {
      ++skipped;
      continue;
    }
    const std::string type = j.value("type", "");
    uint8_t et;
    if (type == "edit") et = 0;
    else if (type == "new") et = 1;
    else if (type == "log") et = 2;
    else { ++skipped; continue; }

    Event e;
    e.event_id = fnv1a(j.contains("meta") ? j["meta"].value("id", "") : "");
    e.event_time_us = static_cast<int64_t>(j.value("timestamp", 0LL)) * 1'000'000;
    e.ingest_time_us = now_us();
    e.event_type = et;
    e.campaign_id = static_cast<uint32_t>(fnv1a(j.value("wiki", "")));
    e.advertiser_id = static_cast<uint32_t>(fnv1a(j.value("server_name", "")));
    e.video_id = fnv1a(j.value("title", ""));
    e.country = 0;
    e.device = j.value("bot", false) ? 2 : 0;
    if (j.contains("length") && j["length"].is_object()) {
      const auto& len = j["length"];
      e.cost_micros = static_cast<int64_t>(len.value("new", 0LL)) -
                      static_cast<int64_t>(len.value("old", 0LL));
    }
    w.write(e);
    w.flush();  // live stream: never sit on a buffered event
    ++parsed;
  }
  std::fprintf(stderr, "wiki2events: %llu events, %llu skipped\n",
               static_cast<unsigned long long>(parsed), static_cast<unsigned long long>(skipped));
  return 0;
}
