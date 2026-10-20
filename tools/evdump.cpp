// evdump: print events as text for eyeballing.
//   evdump --in events.ads [--n 20]

#include <cstdio>

#include "common/args.h"
#include "common/event.h"

using namespace adstack;

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (!args.has("--in")) { std::fprintf(stderr, "evdump --in FILE [--n 20]\n"); return 2; }
  const int64_t n = args.i64("--n", 20);

  EventReader r(args.str("--in", ""));
  std::printf("%-10s %-20s %-20s %-10s %-10s %-10s %-10s %-7s %-6s %s\n",
              "event_id", "event_time_us", "ingest_time_us", "type", "advertiser",
              "campaign", "video", "country", "device", "cost_micros");
  Event e;
  for (int64_t i = 0; i < n && r.next(e); ++i) {
    std::printf("%-10llu %-20lld %-20lld %-10s %-10u %-10u %-10llu %-7u %-6u %lld\n",
                static_cast<unsigned long long>(e.event_id),
                static_cast<long long>(e.event_time_us),
                static_cast<long long>(e.ingest_time_us),
                event_type_name(e.event_type), e.advertiser_id, e.campaign_id,
                static_cast<unsigned long long>(e.video_id), e.country, e.device,
                static_cast<long long>(e.cost_micros));
  }
  return 0;
}
