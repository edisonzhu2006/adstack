#include <gtest/gtest.h>

#include <algorithm>
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
