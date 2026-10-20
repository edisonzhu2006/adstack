// gen: deterministic synthetic ad-event stream with controllable dirt.
//
// Events are emitted in *ingest* order. Most have event_time a few hundred
// ms before ingest_time; a fraction are "late" (event_time up to
// --max-late-s earlier) and a fraction are exact duplicates of a recent
// event re-delivered with a new ingest_time. Same seed => same bytes.
//
//   gen --n 1000000 --seed 1 --out events.ads
//   gen --n 1000000 --dup-frac 0 --late-frac 0 --out clean.ads

#include <chrono>
#include <cmath>
#include <cstdio>
#include <random>
#include <thread>
#include <vector>

#include "common/args.h"
#include "common/event.h"

using namespace adstack;

namespace {

constexpr int64_t kUs = 1'000'000;
constexpr int64_t kDefaultEpochUs = 1'767'225'600LL * kUs;  // 2026-01-01T00:00:00Z

// ISO 3166-1 numeric codes with rough traffic weights.
constexpr uint16_t kCountries[] = {840, 356, 76, 826, 276, 392, 124, 36, 484, 410};
constexpr double kCountryW[] = {30, 20, 10, 8, 7, 7, 6, 5, 4, 3};

void usage() {
  std::fprintf(stderr,
    "gen --out FILE [--n N] [--seed S] [--rate EPS] [--dup-frac F] [--late-frac F]\n"
    "    [--max-late-s S] [--zipf E] [--campaigns N] [--videos N] [--epoch-us T] [--pace]\n"
    "  --rate      events per simulated second (default 10000)\n"
    "  --dup-frac  fraction of events that are re-deliveries of a recent event (default 0.01)\n"
    "  --late-frac fraction of events arriving 1s..max-late-s late (default 0.05)\n"
    "  --zipf      skew exponent for video popularity, 1 = uniform (default 2.0)\n"
    "  --pace      sleep so ingest_time tracks wall clock (for live demos)\n");
}

}  // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (!args.has("--out") || args.has("--help")) { usage(); return args.has("--help") ? 0 : 2; }

  const int64_t n = args.i64("--n", 1'000'000);
  const uint64_t seed = static_cast<uint64_t>(args.i64("--seed", 1));
  const double rate = args.dbl("--rate", 10'000);
  const double dup_frac = args.dbl("--dup-frac", 0.01);
  const double late_frac = args.dbl("--late-frac", 0.05);
  const double max_late_s = args.dbl("--max-late-s", 30);
  const double zipf = args.dbl("--zipf", 2.0);
  const uint32_t n_campaigns = static_cast<uint32_t>(args.i64("--campaigns", 100'000));
  const uint64_t n_videos = static_cast<uint64_t>(args.i64("--videos", 1'000'000));
  const int64_t epoch_us = args.i64("--epoch-us", kDefaultEpochUs);
  const bool pace = args.has("--pace");

  std::mt19937_64 rng(seed);
  std::uniform_real_distribution<double> u01(0.0, 1.0);
  std::discrete_distribution<int> country_d(std::begin(kCountryW), std::end(kCountryW));

  EventWriter w(args.str("--out", "-"));

  // Ring of recent events that duplicates are drawn from. Bounded so memory
  // is flat regardless of --n.
  constexpr size_t kRing = 1 << 16;
  std::vector<Event> ring;
  ring.reserve(kRing);
  size_t ring_pos = 0;

  const double step_us = kUs / rate;
  double clock = static_cast<double>(epoch_us);
  uint64_t next_id = 1;
  int64_t dups = 0, lates = 0;

  const auto wall0 = std::chrono::steady_clock::now();

  for (int64_t i = 0; i < n; ++i) {
    clock += step_us;
    const int64_t ingest = static_cast<int64_t>(clock);

    if (pace) {
      auto target = wall0 + std::chrono::microseconds(ingest - epoch_us);
      std::this_thread::sleep_until(target);
    }

    Event e;
    if (!ring.empty() && u01(rng) < dup_frac) {
      // Re-deliver a recent event unchanged except for ingest_time.
      e = ring[static_cast<size_t>(u01(rng) * static_cast<double>(ring.size()))];
      e.ingest_time_us = ingest;
      ++dups;
      w.write(e);
      continue;
    }

    e.event_id = next_id++;
    e.ingest_time_us = ingest;
    if (u01(rng) < late_frac) {
      e.event_time_us = ingest - static_cast<int64_t>((1.0 + u01(rng) * (max_late_s - 1.0)) * kUs);
      ++lates;
    } else {
      e.event_time_us = ingest - static_cast<int64_t>(u01(rng) * 200'000);  // 0..200 ms
    }

    const double r = u01(rng);
    e.event_type = r < 0.90 ? 0 : (r < 0.99 ? 1 : 2);
    e.campaign_id = static_cast<uint32_t>(u01(rng) * n_campaigns);
    e.advertiser_id = e.campaign_id % 10'000;
    // Skewed popularity: small ids are hot. zipf=1 is uniform.
    e.video_id = static_cast<uint64_t>(static_cast<double>(n_videos) * std::pow(u01(rng), zipf));
    e.country = kCountries[country_d(rng)];
    e.device = static_cast<uint8_t>(u01(rng) < 0.6 ? 0 : (u01(rng) < 0.8 ? 1 : 2));
    switch (e.event_type) {
      case 0: e.cost_micros = static_cast<int64_t>(u01(rng) * 5'000); break;          // CPM-ish
      case 1: e.cost_micros = 100'000 + static_cast<int64_t>(u01(rng) * 1'900'000); break;  // $0.10..$2
      default: e.cost_micros = 0; break;
    }

    if (ring.size() < kRing) ring.push_back(e);
    else { ring[ring_pos] = e; ring_pos = (ring_pos + 1) % kRing; }

    w.write(e);
  }
  w.flush();

  std::fprintf(stderr, "gen: wrote %llu events (%lld unique, %lld dups, %lld late) seed=%llu\n",
               static_cast<unsigned long long>(w.count()), static_cast<long long>(next_id - 1),
               static_cast<long long>(dups), static_cast<long long>(lates),
               static_cast<unsigned long long>(seed));
  return 0;
}
