// Predicate ranges, the --where parser and zone-map decisions on boundary
// values. No file I/O: everything here is pure functions on integers.

#include <gtest/gtest.h>

#include <limits>

#include "src/encoding.h"
#include "src/predicate.h"

using namespace colstore;

namespace {

Predicate p(ColumnId col, Op op, int64_t v, int64_t hi = 0) {
  return make_predicate(col, op, widen(v), widen(hi));
}

}  // namespace

// ---- order keys and matching ----------------------------------------------------

TEST(Predicate, OrderKeyPreservesSignedOrder) {
  const int64_t vals[] = {std::numeric_limits<int64_t>::min(), -1, 0, 1, std::numeric_limits<int64_t>::max()};
  for (size_t i = 1; i < 5; ++i) {
    EXPECT_LT(order_key(widen(vals[i - 1]), ColumnType::I64), order_key(widen(vals[i]), ColumnType::I64));
  }
  EXPECT_EQ(order_key(widen(std::numeric_limits<int64_t>::min()), ColumnType::I64), 0u);
  EXPECT_EQ(order_key(widen(std::numeric_limits<int64_t>::max()), ColumnType::I64), ~uint64_t{0});
  EXPECT_EQ(order_key(840, ColumnType::U16), 840u);  // identity for unsigned
}

TEST(Predicate, EveryOpMatchesLikeTheObviousComparison) {
  const ColumnId c = ColumnId::CostMicros;  // i64 so negatives are exercised
  for (int64_t x = -5; x <= 5; ++x) {
    const uint64_t bits = widen(x);
    EXPECT_EQ(p(c, Op::Lt, 2).matches(bits), x < 2) << x;
    EXPECT_EQ(p(c, Op::Le, 2).matches(bits), x <= 2) << x;
    EXPECT_EQ(p(c, Op::Eq, -2).matches(bits), x == -2) << x;
    EXPECT_EQ(p(c, Op::Ge, -3).matches(bits), x >= -3) << x;
    EXPECT_EQ(p(c, Op::Gt, 0).matches(bits), x > 0) << x;
    EXPECT_EQ(p(c, Op::Between, -1, 3).matches(bits), x >= -1 && x <= 3) << x;
    EXPECT_FALSE(p(c, Op::Between, 3, -1).matches(bits)) << "lo > hi is empty";
  }
}

TEST(Predicate, TypeEdgesProduceEmptyRanges) {
  // "x < 0" on an unsigned column and "x > max" on any column match nothing
  // and must not wrap around.
  EXPECT_TRUE(p(ColumnId::Country, Op::Lt, 0).empty());
  EXPECT_TRUE(make_predicate(ColumnId::Country, Op::Gt, 65535).empty());
  EXPECT_TRUE(make_predicate(ColumnId::EventId, Op::Gt, ~uint64_t{0}).empty());
  EXPECT_TRUE(p(ColumnId::CostMicros, Op::Lt, std::numeric_limits<int64_t>::min()).empty());
  EXPECT_TRUE(p(ColumnId::CostMicros, Op::Gt, std::numeric_limits<int64_t>::max()).empty());
  // ...while the inclusive forms at the edges are the full range.
  EXPECT_TRUE(make_predicate(ColumnId::Country, Op::Le, 65535).matches(65535));
  EXPECT_TRUE(make_predicate(ColumnId::Country, Op::Ge, 0).matches(0));
  EXPECT_TRUE(p(ColumnId::CostMicros, Op::Ge, std::numeric_limits<int64_t>::min())
                  .matches(widen(std::numeric_limits<int64_t>::max())));
}

TEST(Predicate, EventFieldBitsMatchesWriterWidening) {
  adstack::Event e;
  e.cost_micros = -7;
  e.country = 840;
  e.event_id = ~uint64_t{0};
  EXPECT_EQ(event_field_bits(e, ColumnId::CostMicros), widen(int64_t{-7}));
  EXPECT_EQ(event_field_bits(e, ColumnId::Country), 840u);
  EXPECT_EQ(event_field_bits(e, ColumnId::EventId), ~uint64_t{0});
  EXPECT_TRUE(matches_all({p(ColumnId::CostMicros, Op::Lt, 0), p(ColumnId::Country, Op::Eq, 840)}, e));
  EXPECT_FALSE(matches_all({p(ColumnId::CostMicros, Op::Lt, 0), p(ColumnId::Country, Op::Eq, 356)}, e));
  EXPECT_TRUE(matches_all({}, e));
}

// ---- zone maps ------------------------------------------------------------------

// Chunk [100, 200] on an unsigned column: every operator at every boundary.
TEST(ZoneMap, BoundariesUnsigned) {
  const ColumnId c = ColumnId::CampaignId;
  const uint64_t lo = 100, hi = 200;
  auto z = [&](Op op, int64_t v, int64_t v2 = 0) { return zone_decision(p(c, op, v, v2), lo, hi); };

  EXPECT_EQ(z(Op::Eq, 99), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Eq, 100), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Eq, 200), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Eq, 201), ZoneDecision::Skip);

  EXPECT_EQ(z(Op::Lt, 100), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Lt, 101), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Lt, 200), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Lt, 201), ZoneDecision::All);

  EXPECT_EQ(z(Op::Le, 99), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Le, 100), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Le, 199), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Le, 200), ZoneDecision::All);

  EXPECT_EQ(z(Op::Gt, 200), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Gt, 199), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Gt, 100), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Gt, 99), ZoneDecision::All);

  EXPECT_EQ(z(Op::Ge, 201), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Ge, 200), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Ge, 101), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Ge, 100), ZoneDecision::All);

  EXPECT_EQ(z(Op::Between, 0, 99), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Between, 201, 300), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Between, 0, 100), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Between, 200, 300), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Between, 150, 160), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Between, 100, 200), ZoneDecision::All);
  EXPECT_EQ(z(Op::Between, 99, 201), ZoneDecision::All);
  EXPECT_EQ(z(Op::Between, 300, 200), ZoneDecision::Skip) << "empty predicate";
}

// Signed chunk [-10, 5]: a signed-vs-unsigned mix-up would put -10 above 5.
TEST(ZoneMap, BoundariesSigned) {
  const ColumnId c = ColumnId::CostMicros;
  const uint64_t lo = widen(int64_t{-10}), hi = widen(int64_t{5});
  auto z = [&](Op op, int64_t v, int64_t v2 = 0) { return zone_decision(p(c, op, v, v2), lo, hi); };
  EXPECT_EQ(z(Op::Lt, -10), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Lt, -9), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Lt, 6), ZoneDecision::All);
  EXPECT_EQ(z(Op::Gt, 5), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Gt, -11), ZoneDecision::All);
  EXPECT_EQ(z(Op::Eq, 0), ZoneDecision::Some);
  EXPECT_EQ(z(Op::Eq, -11), ZoneDecision::Skip);
  EXPECT_EQ(z(Op::Between, -10, 5), ZoneDecision::All);
  EXPECT_EQ(z(Op::Between, -100, -11), ZoneDecision::Skip);
}

TEST(ZoneMap, SingleValueChunk) {
  const ColumnId c = ColumnId::Device;
  EXPECT_EQ(zone_decision(p(c, Op::Eq, 1), 1, 1), ZoneDecision::All);
  EXPECT_EQ(zone_decision(p(c, Op::Eq, 0), 1, 1), ZoneDecision::Skip);
  EXPECT_EQ(zone_decision(p(c, Op::Le, 1), 1, 1), ZoneDecision::All);
  EXPECT_EQ(zone_decision(p(c, Op::Lt, 1), 1, 1), ZoneDecision::Skip);
}

// ---- parser -----------------------------------------------------------------

TEST(WhereParser, EveryOperatorAndSpacing) {
  const Conjunction w = parse_where("country=840 AND  campaign_id  <  10 and device>=1 AND cost_micros<=-5 "
                                    "AND event_id > 7 AND video_id == 3");
  ASSERT_EQ(w.size(), 6u);
  EXPECT_EQ(w[0], p(ColumnId::Country, Op::Eq, 840));
  EXPECT_EQ(w[1], p(ColumnId::CampaignId, Op::Lt, 10));
  EXPECT_EQ(w[2], p(ColumnId::Device, Op::Ge, 1));
  EXPECT_EQ(w[3], p(ColumnId::CostMicros, Op::Le, -5));
  EXPECT_EQ(w[4], p(ColumnId::EventId, Op::Gt, 7));
  EXPECT_EQ(w[5], p(ColumnId::VideoId, Op::Eq, 3));
}

TEST(WhereParser, BetweenIsOneTermAndMixesWithAnd) {
  const Conjunction w = parse_where("event_time_us BETWEEN 100 AND 200 AND country = 840 "
                                    "AND cost_micros between -3 and 3");
  ASSERT_EQ(w.size(), 3u);
  EXPECT_EQ(w[0], p(ColumnId::EventTimeUs, Op::Between, 100, 200));
  EXPECT_EQ(w[1], p(ColumnId::Country, Op::Eq, 840));
  EXPECT_EQ(w[2], p(ColumnId::CostMicros, Op::Between, -3, 3));
  EXPECT_EQ(to_string(w[2]), "cost_micros BETWEEN -3 AND 3");
  EXPECT_EQ(to_string(w[1]), "country = 840");
}

TEST(WhereParser, EmptyAndBlank) {
  EXPECT_TRUE(parse_where("").empty());
  EXPECT_TRUE(parse_where("   ").empty());
}

TEST(WhereParser, Unsigned64Literal) {
  const Conjunction w = parse_where("event_id >= 18446744073709551615");
  ASSERT_EQ(w.size(), 1u);
  EXPECT_EQ(w[0].value_bits, ~uint64_t{0});
}

TEST(WhereParser, Errors) {
  EXPECT_THROW(parse_where("nope = 1"), std::invalid_argument);               // unknown column
  EXPECT_THROW(parse_where("country 840"), std::invalid_argument);            // missing operator
  EXPECT_THROW(parse_where("country = "), std::invalid_argument);             // missing literal
  EXPECT_THROW(parse_where("country = 840 country = 356"), std::invalid_argument);  // missing AND
  EXPECT_THROW(parse_where("country = 840 AND"), std::invalid_argument);      // dangling AND
  EXPECT_THROW(parse_where("country BETWEEN 1 2"), std::invalid_argument);    // BETWEEN needs AND
  EXPECT_THROW(parse_where("country BETWEEN 1 OR 2"), std::invalid_argument);
  EXPECT_THROW(parse_where("country != 840"), std::invalid_argument);         // unsupported operator
  EXPECT_THROW(parse_where("country = 70000"), std::invalid_argument);        // does not fit u16
  EXPECT_THROW(parse_where("country = -1"), std::invalid_argument);           // negative for unsigned
  EXPECT_THROW(parse_where("device = 256"), std::invalid_argument);           // does not fit u8
  EXPECT_THROW(parse_where("cost_micros = 9223372036854775808"), std::invalid_argument);  // > INT64_MAX
  EXPECT_THROW(parse_where("country = 8a"), std::invalid_argument);           // garbage literal
  EXPECT_THROW(parse_where("country = (840)"), std::invalid_argument);        // unexpected character
  EXPECT_NO_THROW(parse_where("device = 255"));
  EXPECT_NO_THROW(parse_where("cost_micros = -9223372036854775808"));
}
