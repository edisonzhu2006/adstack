#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "common/agg.h"

using namespace adstack;

constexpr int64_t kMin = 60'000'000;

TEST(Window, StartFloorsPositive) {
  EXPECT_EQ(window_start(0, kMin), 0);
  EXPECT_EQ(window_start(1, kMin), 0);
  EXPECT_EQ(window_start(kMin - 1, kMin), 0);
  EXPECT_EQ(window_start(kMin, kMin), kMin);
  EXPECT_EQ(window_start(kMin * 7 + 42, kMin), kMin * 7);
}

TEST(Window, StartFloorsNegative) {
  // Plain integer division truncates toward zero; we need the floor.
  EXPECT_EQ(window_start(-1, kMin), -kMin);
  EXPECT_EQ(window_start(-kMin, kMin), -kMin);
  EXPECT_EQ(window_start(-kMin - 1, kMin), -2 * kMin);
}

TEST(Window, KeyOrdering) {
  WindowKey a{0, 5}, b{0, 6}, c{kMin, 0};
  EXPECT_LT(a, b);
  EXPECT_LT(b, c);
  EXPECT_EQ(a, (WindowKey{0, 5}));
}

TEST(Agg, CountsByTypeAndSumsCost) {
  Agg a;
  Event e;
  e.event_type = 0; e.cost_micros = 10; a.add(e);
  e.event_type = 1; e.cost_micros = 20; a.add(e);
  e.event_type = 1; e.cost_micros = 30; a.add(e);
  e.event_type = 2; e.cost_micros = 0;  a.add(e);
  EXPECT_EQ(a.impressions, 1u);
  EXPECT_EQ(a.clicks, 2u);
  EXPECT_EQ(a.conversions, 1u);
  EXPECT_EQ(a.cost_micros, 60);
}

// ---- sliding windows (m3) ---------------------------------------------------

TEST(Sliding, TumblingIsTheSpecialCaseSlideEqualsWindow) {
  for (int64_t t : {0LL, 1LL, kMin - 1, kMin, 7 * kMin + 42, -1LL, -kMin - 1}) {
    const auto starts = window_starts(t, kMin, kMin);
    ASSERT_EQ(starts.size(), 1u) << "t=" << t;
    EXPECT_EQ(starts[0], window_start(t, kMin)) << "t=" << t;
  }
}

TEST(Sliding, EventBelongsToWindowOverSlideWindows) {
  // window 300s, slide 60s: every event is in exactly 5 windows, 60s apart,
  // the last of which starts at floor(t, 60s).
  const int64_t w = 5 * kMin, sl = kMin;
  for (int64_t t : {0LL, 59LL * 1'000'000, kMin, 10 * kMin + 1, 1'000 * kMin - 1}) {
    const auto starts = window_starts(t, w, sl);
    ASSERT_EQ(starts.size(), 5u) << "t=" << t;
    EXPECT_EQ(starts.back(), window_start(t, sl));
    for (size_t i = 0; i < starts.size(); ++i) {
      EXPECT_EQ(starts[i], starts.back() - static_cast<int64_t>(4 - i) * sl);
      EXPECT_LE(starts[i], t);        // window has started
      EXPECT_GT(starts[i] + w, t);    // and not yet ended
      EXPECT_EQ(starts[i] % sl, 0);   // aligned to the slide
    }
  }
}

TEST(Sliding, BoundariesAreHalfOpen) {
  // t exactly on a slide boundary starts a new window and leaves the window
  // that ends there. window 120s, slide 60s.
  const int64_t w = 2 * kMin, sl = kMin;
  EXPECT_EQ(window_starts(2 * kMin - 1, w, sl), (std::vector<int64_t>{0, kMin}));
  EXPECT_EQ(window_starts(2 * kMin, w, sl), (std::vector<int64_t>{kMin, 2 * kMin}));
}

TEST(Sliding, NegativeTimesFloorTowardMinusInfinity) {
  const int64_t w = 2 * kMin, sl = kMin;
  EXPECT_EQ(window_starts(-1, w, sl), (std::vector<int64_t>{-2 * kMin, -kMin}));
  EXPECT_EQ(window_starts(-kMin, w, sl), (std::vector<int64_t>{-2 * kMin, -kMin}));
  EXPECT_EQ(window_starts(-kMin - 1, w, sl), (std::vector<int64_t>{-3 * kMin, -2 * kMin}));
}

TEST(Sliding, ForEachVisitsAscending) {
  std::vector<int64_t> seen;
  for_each_window_start(10 * kMin + 5, 3 * kMin, kMin, [&](int64_t s) { seen.push_back(s); });
  EXPECT_EQ(seen, (std::vector<int64_t>{8 * kMin, 9 * kMin, 10 * kMin}));
}

TEST(Sliding, EveryWindowIsExactlyTheSetOfEventsItContains) {
  // Brute-force cross-check against the definition: for a grid of event
  // times, s is enumerated for t iff s <= t < s + window, for a window that
  // is not a multiple of the slide (100s over 30s) to exercise the general
  // formula and not just the divisible case.
  const int64_t w = 100 * 1'000'000, sl = 30 * 1'000'000;
  for (int64_t t = -400 * 1'000'000; t <= 400 * 1'000'000; t += 7 * 1'000'000 + 13) {
    const auto starts = window_starts(t, w, sl);
    for (int64_t s = -600 * 1'000'000; s <= 600 * 1'000'000; s += sl) {
      const bool contains = s <= t && t < s + w;
      const bool listed = std::find(starts.begin(), starts.end(), s) != starts.end();
      EXPECT_EQ(contains, listed) << "t=" << t << " s=" << s;
    }
  }
}

// ---- review fixes (2026-10-24): supported ranges and checked sums ------------

TEST(Range, SupportedEventTimesAreExactlyPlusMinusTwoToThe62) {
  EXPECT_TRUE(event_time_supported(0));
  EXPECT_TRUE(event_time_supported(kMaxAbsEventTimeUs));
  EXPECT_TRUE(event_time_supported(-kMaxAbsEventTimeUs));
  EXPECT_FALSE(event_time_supported(kMaxAbsEventTimeUs + 1));
  EXPECT_FALSE(event_time_supported(-kMaxAbsEventTimeUs - 1));
  EXPECT_FALSE(event_time_supported(INT64_MIN + 5));
  EXPECT_FALSE(event_time_supported(INT64_MAX - 5));
  Event e;
  e.event_id = 9;
  e.event_time_us = INT64_MIN + 5;
  EXPECT_THROW(check_event_time(e), std::out_of_range);
  e.event_time_us = 1'767'225'600'000'000LL;
  EXPECT_NO_THROW(check_event_time(e));
}

TEST(Range, WindowEnumerationAtTheLimitsTerminatesWithoutOverflow) {
  // Before the fix, t = INT64_MIN + 5 made window_start wrap positive and
  // this loop never ended. At the supported limits, with the largest
  // durations, every start is still exact (run under UBSan to see no report).
  const int64_t slide = kMaxDurationUs / 4;
  for (const int64_t t : {-kMaxAbsEventTimeUs, kMaxAbsEventTimeUs, int64_t{0}}) {
    const auto starts = window_starts(t, kMaxDurationUs, slide);
    ASSERT_EQ(starts.size(), 4u) << t;
    for (size_t i = 0; i < starts.size(); ++i) {
      EXPECT_LE(starts[i], t);
      EXPECT_GT(starts[i] + kMaxDurationUs, t);
      if (i > 0) EXPECT_EQ(starts[i] - starts[i - 1], slide);
    }
  }
}

TEST(Agg, CostSumOverflowThrowsInsteadOfWrapping) {
  // Two events of INT64_MAX micro-dollars used to print a cost of -2.
  Agg a;
  Event e;
  e.event_type = 0;
  e.cost_micros = INT64_MAX;
  a.add(e);
  EXPECT_EQ(a.cost_micros, INT64_MAX);
  EXPECT_THROW(a.add(e), std::overflow_error);
  Agg b;
  e.cost_micros = INT64_MIN;
  b.add(e);
  EXPECT_THROW(b.add(e), std::overflow_error);
  Agg c;  // ordinary negative and positive costs still add exactly
  e.cost_micros = -5;
  c.add(e);
  e.cost_micros = 7;
  c.add(e);
  EXPECT_EQ(c.cost_micros, 2);
}
