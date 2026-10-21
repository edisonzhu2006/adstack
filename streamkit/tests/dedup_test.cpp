// Unit tests for the time-bucketed dedup set (streamkit milestone 2).
// Each test pins one property from dedup.h: duplicates are caught inside the
// horizon, buckets expire behind it, an expired id is exactly one the engine
// would reject as late anyway, and bucket boundaries are handled exactly.

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/agg.h"
#include "dedup.h"

using namespace adstack;
using Verdict = Dedup::Verdict;

namespace {

constexpr int64_t kS = 1'000'000;   // one second in microseconds
constexpr int64_t kW = 60 * kS;     // streamkit's default tumbling window

// The late rule from streamkit/main.cpp, copied here so the tests pin the
// dedup horizon to the engine's actual rejection condition.
bool engine_rejects_as_late(int64_t event_time_us, int64_t watermark_us, int64_t window_us) {
  return window_start(event_time_us, window_us) + window_us <= watermark_us;
}

}  // namespace

TEST(Dedup, DuplicateDetectedWithinHorizon) {
  Dedup d(kW, kS);
  d.advance(1000 * kS);
  EXPECT_EQ(d.insert(1, 1000 * kS), Verdict::kNew);
  EXPECT_EQ(d.insert(1, 1000 * kS), Verdict::kDuplicate);
  EXPECT_EQ(d.insert(2, 1000 * kS), Verdict::kNew);  // different id, same bucket
  EXPECT_EQ(d.insert(1, 1000 * kS), Verdict::kDuplicate);
  EXPECT_EQ(d.ids(), 2u);
  EXPECT_EQ(d.peak_ids(), 2u);
}

TEST(Dedup, NoWatermarkMeansNothingExpires) {
  Dedup d(kW, kS);
  EXPECT_FALSE(d.forget_before().has_value());
  EXPECT_EQ(d.insert(1, -5000 * kS), Verdict::kNew);
  EXPECT_EQ(d.insert(1, -5000 * kS), Verdict::kDuplicate);
}

TEST(Dedup, OutOfOrderEventOlderThanFrontIsRemembered) {
  // First event at t=100s, then a distinct event 30s older, then its copy.
  // The older event must extend the span backwards, not be lost.
  Dedup d(kW, kS);
  d.advance(100 * kS);  // lateness 0: forget_before = 40s
  EXPECT_EQ(d.insert(1, 100 * kS), Verdict::kNew);
  EXPECT_EQ(d.insert(2, 70 * kS), Verdict::kNew);
  EXPECT_EQ(d.insert(2, 70 * kS), Verdict::kDuplicate);
  EXPECT_EQ(d.buckets(), 31u);  // 70s .. 100s inclusive
  EXPECT_EQ(d.ids(), 2u);
}

TEST(Dedup, HorizonIsWatermarkMinusWindowFlooredToBucket) {
  // 1000.123456s - 60s = 940.123456s, floored to the 1s bucket = 940s.
  EXPECT_EQ(Dedup::forget_before_for(1000 * kS + 123456, kW, kS), 940 * kS);
  EXPECT_EQ(Dedup::forget_before_for(1000 * kS, kW, kS), 940 * kS);
  // 5s buckets floor further: 940.123456s -> 940s; 943s -> 940s.
  EXPECT_EQ(Dedup::forget_before_for(1003 * kS, kW, 5 * kS), 940 * kS);
  // Negative timestamps floor toward -infinity, not toward zero.
  EXPECT_EQ(Dedup::forget_before_for(0, kW, kS), -60 * kS);
  EXPECT_EQ(Dedup::forget_before_for(59 * kS + 1, kW, kS), -1 * kS);
}

TEST(Dedup, BucketExpiryForgetsOldIds) {
  Dedup d(kW, kS);
  d.advance(1000 * kS);
  ASSERT_EQ(d.insert(1, 1000 * kS), Verdict::kNew);
  ASSERT_EQ(d.insert(2, 1000 * kS + 999'999), Verdict::kNew);  // same bucket, last us
  ASSERT_EQ(d.insert(3, 1001 * kS), Verdict::kNew);            // next bucket
  EXPECT_EQ(d.buckets(), 2u);

  // Watermark 1061s: forget_before = 1001s. Bucket [1000s,1001s) goes,
  // bucket [1001s,1002s) stays.
  d.advance(1061 * kS);
  EXPECT_EQ(d.forget_before(), 1001 * kS);
  EXPECT_EQ(d.buckets(), 1u);
  EXPECT_EQ(d.ids(), 1u);
  EXPECT_EQ(d.insert(3, 1001 * kS), Verdict::kDuplicate);  // survivor still known
  EXPECT_EQ(d.peak_ids(), 3u);
}

TEST(Dedup, CopyArrivingAfterExpiryIsExpiredNotNew) {
  // After its bucket is dropped, a late copy must not be re-admitted as a
  // new id (that would re-grow memory) and must not be called a duplicate
  // (we no longer know). It is kExpired, and the engine's late rule rejects
  // it, so the output is unaffected.
  Dedup d(kW, kS);
  d.advance(1000 * kS);
  ASSERT_EQ(d.insert(1, 1000 * kS), Verdict::kNew);
  d.advance(1061 * kS);
  EXPECT_EQ(d.insert(1, 1000 * kS), Verdict::kExpired);
  EXPECT_EQ(d.ids(), 0u);
  EXPECT_EQ(d.buckets(), 0u);
  EXPECT_TRUE(engine_rejects_as_late(1000 * kS, 1061 * kS, kW));
}

TEST(Dedup, ExpiredImpliesLateForEveryTimestampNearTheHorizon) {
  // Property: whenever insert() says kExpired, the engine would have
  // rejected the event as late. Checked exhaustively for every microsecond
  // in a band around the horizon, for several watermarks and bucket widths,
  // so no rounding corner slips through.
  const int64_t watermarks[] = {0, 1 * kS, 59 * kS + 999'999, 60 * kS, 1000 * kS + 123456,
                                -7 * kS - 1};
  const int64_t buckets[] = {kS, 5 * kS, 7 * kS + 3};  // the odd width is deliberate
  for (int64_t wm : watermarks) {
    for (int64_t bucket : buckets) {
      Dedup d(kW, bucket);
      d.advance(wm);
      const int64_t fb = *d.forget_before();
      EXPECT_EQ(fb, Dedup::forget_before_for(wm, kW, bucket));
      for (int64_t t = fb - 2 * bucket; t < fb + 2 * bucket; t += 997) {
        const Verdict v = d.insert(static_cast<uint64_t>(t + (1LL << 62)), t);
        if (t < fb) {
          EXPECT_EQ(v, Verdict::kExpired) << "wm=" << wm << " bucket=" << bucket << " t=" << t;
          EXPECT_TRUE(engine_rejects_as_late(t, wm, kW)) << "t=" << t << " wm=" << wm;
        } else {
          EXPECT_EQ(v, Verdict::kNew) << "wm=" << wm << " bucket=" << bucket << " t=" << t;
        }
      }
    }
  }
}

TEST(Dedup, BucketBoundaryEdges) {
  Dedup d(kW, kS);
  d.advance(1000 * kS);  // forget_before = 940s
  EXPECT_EQ(d.insert(1, 940 * kS - 1), Verdict::kExpired);  // last us below the line
  EXPECT_EQ(d.insert(1, 940 * kS), Verdict::kNew);          // exactly on it: kept
  EXPECT_EQ(d.buckets(), 1u);
  EXPECT_EQ(d.insert(2, 941 * kS - 1), Verdict::kNew);  // same bucket as 940s
  EXPECT_EQ(d.buckets(), 1u);
  EXPECT_EQ(d.insert(3, 941 * kS), Verdict::kNew);  // first us of the next bucket
  EXPECT_EQ(d.buckets(), 2u);

  // Advancing so the horizon lands exactly on 941s drops only bucket 940.
  d.advance(1001 * kS);
  EXPECT_EQ(d.forget_before(), 941 * kS);
  EXPECT_EQ(d.buckets(), 1u);
  EXPECT_EQ(d.insert(3, 941 * kS), Verdict::kDuplicate);
  EXPECT_EQ(d.insert(1, 940 * kS), Verdict::kExpired);
}

TEST(Dedup, NegativeTimesBucketTowardMinusInfinity) {
  Dedup d(kW, kS);
  EXPECT_EQ(d.insert(1, -1), Verdict::kNew);
  EXPECT_EQ(d.insert(2, -kS), Verdict::kNew);  // same bucket [-1s, 0)
  EXPECT_EQ(d.buckets(), 1u);
  EXPECT_EQ(d.insert(3, 0), Verdict::kNew);  // bucket [0, 1s)
  EXPECT_EQ(d.buckets(), 2u);
  EXPECT_EQ(d.insert(4, -kS - 1), Verdict::kNew);  // bucket [-2s, -1s)
  EXPECT_EQ(d.buckets(), 3u);
}

TEST(Dedup, WatermarkMovingBackwardsIsIgnored) {
  Dedup d(kW, kS);
  d.advance(1000 * kS);
  d.advance(900 * kS);
  EXPECT_EQ(d.forget_before(), 940 * kS);
  EXPECT_EQ(d.insert(1, 850 * kS), Verdict::kExpired);
}

TEST(Dedup, GapsInEventTimeBecomeEmptyBuckets) {
  Dedup d(kW, kS);
  d.advance(100 * kS);
  EXPECT_EQ(d.insert(1, 100 * kS), Verdict::kNew);
  EXPECT_EQ(d.insert(2, 110 * kS), Verdict::kNew);
  EXPECT_EQ(d.buckets(), 11u);
  EXPECT_EQ(d.ids(), 2u);
  EXPECT_EQ(d.insert(3, 105 * kS), Verdict::kNew);  // lands in an empty middle bucket
  EXPECT_EQ(d.insert(3, 105 * kS), Verdict::kDuplicate);
}

TEST(Dedup, RecycledBucketDoesNotRememberExpiredIds) {
  // Expired hash tables are reused for new buckets. The reused table must
  // come back empty, otherwise a fresh id could be called a duplicate.
  Dedup d(kW, kS);
  d.advance(1000 * kS);
  for (uint64_t id = 1; id <= 100; ++id) ASSERT_EQ(d.insert(id, 1000 * kS), Verdict::kNew);
  d.advance(1061 * kS);  // expires bucket 1000s; its table goes to the free list
  ASSERT_EQ(d.ids(), 0u);
  for (uint64_t id = 1; id <= 100; ++id) {
    EXPECT_EQ(d.insert(id, 1061 * kS), Verdict::kNew) << "id=" << id;
  }
  EXPECT_EQ(d.ids(), 100u);
}

TEST(Dedup, RetainedSpanStaysBoundedOnADirtyStream) {
  // Simulates the generator's shape: mostly in-order events, 5% up to 30s
  // late, 1% duplicates, lateness 60s, window 60s. Every id must be
  // classified exactly once as new, and the span must never exceed
  // (lateness + window) / bucket + 2 buckets no matter how long it runs.
  constexpr int64_t kLateness = 60 * kS;
  constexpr int kPerSecond = 200;
  constexpr int kSeconds = 400;
  constexpr size_t kMaxBuckets = static_cast<size_t>((kLateness + kW) / kS) + 2;

  Dedup d(kW, kS);
  std::mt19937_64 rng(42);
  std::uniform_real_distribution<double> u01(0.0, 1.0);
  std::vector<std::pair<uint64_t, int64_t>> recent;  // (id, event_time) to draw dups from
  uint64_t next_id = 1;
  int64_t max_event_time = 0;
  size_t news = 0, dups = 0, expired = 0, max_buckets = 0, max_ids = 0;

  for (int s = 1; s <= kSeconds; ++s) {
    for (int k = 0; k < kPerSecond; ++k) {
      const int64_t ingest = s * kS + k * (kS / kPerSecond);
      uint64_t id;
      int64_t t;
      if (!recent.empty() && u01(rng) < 0.01) {
        const auto& r = recent[static_cast<size_t>(u01(rng) * static_cast<double>(recent.size()))];
        id = r.first;
        t = r.second;
      } else {
        id = next_id++;
        t = u01(rng) < 0.05 ? ingest - static_cast<int64_t>((1 + u01(rng) * 29) * kS)
                            : ingest - static_cast<int64_t>(u01(rng) * 200'000);
        recent.emplace_back(id, t);
        if (recent.size() > 4096) recent.erase(recent.begin());
      }
      if (t > max_event_time) {
        max_event_time = t;
        d.advance(max_event_time - kLateness);
      }
      switch (d.insert(id, t)) {
        case Verdict::kNew: ++news; break;
        case Verdict::kDuplicate: ++dups; break;
        case Verdict::kExpired: ++expired; break;
      }
      max_buckets = std::max(max_buckets, d.buckets());
      max_ids = std::max(max_ids, d.ids());
    }
  }

  EXPECT_EQ(news, next_id - 1);  // every distinct id admitted exactly once
  EXPECT_GT(dups, 0u);
  EXPECT_EQ(expired, 0u);  // nothing in this stream is older than 120s
  EXPECT_LE(max_buckets, kMaxBuckets);
  // Memory is proportional to rate * (lateness + window), not to the stream:
  // at most ~122 buckets of ~200 ids, far below the 80,000 ids generated.
  EXPECT_LE(max_ids, kMaxBuckets * kPerSecond);
  EXPECT_LT(max_ids, static_cast<size_t>(next_id - 1) / 2);
}

TEST(Dedup, RejectsNonPositiveConfig) {
  EXPECT_THROW(Dedup(0, kS), std::invalid_argument);
  EXPECT_THROW(Dedup(kW, 0), std::invalid_argument);
  EXPECT_THROW(Dedup(kW, -kS), std::invalid_argument);
}
