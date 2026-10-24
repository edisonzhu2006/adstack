// KeyPacker layout, the --agg parser, and HashAggregate on hand-built
// batches where every expected number can be checked by eye.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <map>

#include "src/aggregate.h"

using namespace colstore;

namespace {

// A Batch whose column vectors point into storage we own. Columns not set
// stay unloaded, which is how the Scanner hands out columns a query did
// not ask for.
struct FakeBatch {
  std::array<std::vector<uint64_t>, kNumEventColumns> store;
  Batch b;

  explicit FakeBatch(uint32_t n) {
    b.n = n;
    b.cols.resize(kNumEventColumns);  // what the Scanner does for the file's schema
  }
  void set(ColumnId c, std::vector<uint64_t> values) {
    EXPECT_EQ(values.size(), b.n);
    auto& v = store[static_cast<size_t>(c)];
    v = std::move(values);
    b.cols[static_cast<size_t>(c)].values = v.data();
  }
  void set_signed(ColumnId c, std::vector<int64_t> values) {
    std::vector<uint64_t> bits;
    for (int64_t x : values) bits.push_back(widen(x));
    set(c, bits);
  }
  void select(std::vector<uint32_t> sel) {
    b.sel = std::move(sel);
    b.filtered = true;
  }
};

uint64_t cell(const ResultTable& t, const char* name, size_t row) {
  for (size_t c = 0; c < t.schema.size(); ++c) {
    if (t.schema[c].name == name) return t.at(c, row);
  }
  ADD_FAILURE() << "no column " << name;
  return 0;
}
int64_t scell(const ResultTable& t, const char* name, size_t row) { return as_signed(cell(t, name, row)); }

// ---- KeyPacker -----------------------------------------------------------------------

TEST(KeyPacker, NarrowColumnsShareOneWord) {
  KeyPacker p({ColumnId::CampaignId, ColumnId::Country, ColumnId::Device});
  const GroupKey k = p.pack_bits({0xAABBCCDD, 840, 2});
  EXPECT_EQ(k.lo, 0xAABBCCDDULL | (840ULL << 32) | (2ULL << 48));
  EXPECT_EQ(k.hi, 0u);
  std::vector<uint64_t> back;
  p.unpack(k, back);
  EXPECT_EQ(back, (std::vector<uint64_t>{0xAABBCCDD, 840, 2}));
}

TEST(KeyPacker, WideColumnStartsTheNextWordInsteadOfStraddling) {
  KeyPacker p({ColumnId::Device, ColumnId::CostMicros});
  const uint64_t neg = widen(int64_t{-7});
  const GroupKey k = p.pack_bits({1, neg});
  EXPECT_EQ(k.lo, 1u);
  EXPECT_EQ(k.hi, neg);  // all 64 bits, sign included
  std::vector<uint64_t> back;
  p.unpack(k, back);
  EXPECT_EQ(back, (std::vector<uint64_t>{1, neg}));
  EXPECT_EQ(as_signed(back[1]), -7);
}

TEST(KeyPacker, ThirdU32GoesToTheHighWord) {
  KeyPacker p({ColumnId::CampaignId, ColumnId::AdvertiserId, ColumnId::EventTimeUs});
  const GroupKey k = p.pack_bits({1, 2, 3});
  EXPECT_EQ(k.lo, 1ULL | (2ULL << 32));
  EXPECT_EQ(k.hi, 3u);
}

TEST(KeyPacker, RejectsKeysWiderThan128Bits) {
  EXPECT_NO_THROW(KeyPacker({ColumnId::EventId, ColumnId::VideoId}));
  EXPECT_THROW(KeyPacker({ColumnId::EventId, ColumnId::VideoId, ColumnId::Device}), std::invalid_argument);
  EXPECT_THROW(KeyPacker({ColumnId::Device, ColumnId::EventId, ColumnId::VideoId}), std::invalid_argument);
  EXPECT_NO_THROW(KeyPacker({}));  // no group columns: the zero key
  try {
    KeyPacker({ColumnId::EventId, ColumnId::VideoId, ColumnId::Device});
    FAIL();
  } catch (const std::invalid_argument& e) {
    EXPECT_NE(std::string(e.what()).find("needs 136 bits"), std::string::npos) << e.what();
  }
}

TEST(KeyPacker, FirstFitMakesColumnOrderIrrelevantToWhetherAKeyFits) {
  // u32 + u64 + u32 = 128 bits. Packing word by word in the given order
  // used to reject this (32 + 64 > 64 moved to word 1, then 64 + 32 > 64);
  // first fit puts advertiser_id back into word 0.
  KeyPacker p({ColumnId::CampaignId, ColumnId::EventId, ColumnId::AdvertiserId});
  const GroupKey k = p.pack_bits({0x11111111, 0x2222222233333333ULL, 0x44444444});
  EXPECT_EQ(k.lo, 0x11111111ULL | (0x44444444ULL << 32));
  EXPECT_EQ(k.hi, 0x2222222233333333ULL);
  std::vector<uint64_t> back;
  p.unpack(k, back);
  EXPECT_EQ(back, (std::vector<uint64_t>{0x11111111, 0x2222222233333333ULL, 0x44444444}));  // column order kept
  // Every order of the same three columns fits and round-trips.
  std::vector<ColumnId> cols = {ColumnId::CampaignId, ColumnId::EventId, ColumnId::AdvertiserId};
  std::sort(cols.begin(), cols.end());
  do {
    KeyPacker q(cols);
    std::vector<uint64_t> bits;
    for (ColumnId c : cols) bits.push_back(c == ColumnId::EventId ? 0xFEDCBA9876543210ULL : 0x89ABCDEF);
    std::vector<uint64_t> out;
    q.unpack(q.pack_bits(bits), out);
    EXPECT_EQ(out, bits);
  } while (std::next_permutation(cols.begin(), cols.end()));
  // u8 + u64 + u32 + u16 = 120 bits, fits; u64 + u8 + u64 = 136 does not.
  EXPECT_NO_THROW(KeyPacker({ColumnId::Device, ColumnId::VideoId, ColumnId::CampaignId, ColumnId::Country}));
  EXPECT_THROW(KeyPacker({ColumnId::VideoId, ColumnId::Device, ColumnId::EventId}), std::invalid_argument);
}

TEST(KeyPacker, PackFromBatchMasksToColumnWidth) {
  FakeBatch fb(2);
  fb.set(ColumnId::Country, {840, 356});
  fb.set(ColumnId::Device, {1, 2});
  KeyPacker p({ColumnId::Country, ColumnId::Device});
  EXPECT_EQ(p.pack(fb.b, 0), p.pack_bits({840, 1}));
  EXPECT_EQ(p.pack(fb.b, 1), p.pack_bits({356, 2}));
  EXPECT_NE(p.pack(fb.b, 0), p.pack(fb.b, 1));
}

// ---- parser ----------------------------------------------------------------------------

TEST(AggParser, EveryFunctionAndDefaultNames) {
  const auto aggs = parse_aggs(
      "count(), sum(cost_micros), MIN(cost_micros), max(campaign_id), count_if(event_type=1), "
      "sum_if(cost_micros, event_type BETWEEN 1 AND 2), count_if(cost_micros < -5)");
  ASSERT_EQ(aggs.size(), 7u);
  EXPECT_EQ(aggs[0].kind, AggKind::Count);
  EXPECT_EQ(aggs[0].name, "count");
  EXPECT_EQ(aggs[1].kind, AggKind::Sum);
  EXPECT_EQ(aggs[1].col, ColumnId::CostMicros);
  EXPECT_EQ(aggs[1].name, "sum_cost_micros");
  EXPECT_EQ(aggs[2].kind, AggKind::Min);
  EXPECT_EQ(aggs[2].name, "min_cost_micros");
  EXPECT_EQ(aggs[3].kind, AggKind::Max);
  EXPECT_EQ(aggs[3].name, "max_campaign_id");
  EXPECT_EQ(aggs[4].kind, AggKind::CountIf);
  ASSERT_TRUE(aggs[4].cond.has_value());
  EXPECT_EQ(aggs[4].cond->col, ColumnId::EventType);
  EXPECT_EQ(aggs[4].name, "count_if_event_type_eq_1");
  EXPECT_EQ(aggs[5].kind, AggKind::SumIf);
  EXPECT_EQ(aggs[5].col, ColumnId::CostMicros);
  EXPECT_EQ(aggs[5].cond->op, Op::Between);
  EXPECT_EQ(aggs[5].name, "sum_if_cost_micros_event_type_between_1_2");
  EXPECT_EQ(aggs[6].name, "count_if_cost_micros_lt_m5");
}

TEST(AggParser, AliasesAndTypes) {
  const auto aggs = parse_aggs("count_if(event_type=0) AS impressions, sum(cost_micros) as cost, count() As n");
  ASSERT_EQ(aggs.size(), 3u);
  EXPECT_EQ(aggs[0].name, "impressions");
  EXPECT_EQ(aggs[1].name, "cost");
  EXPECT_EQ(aggs[2].name, "n");
  EXPECT_EQ(output_type(aggs[0]), ColumnType::U64);
  EXPECT_EQ(output_type(aggs[1]), ColumnType::I64);
  EXPECT_EQ(output_type(parse_aggs("sum(campaign_id)")[0]), ColumnType::U64);
  EXPECT_EQ(output_type(parse_aggs("min(country)")[0]), ColumnType::U16);
  EXPECT_EQ(output_type(parse_aggs("max(cost_micros)")[0]), ColumnType::I64);
}

TEST(AggParser, Errors) {
  for (const char* bad : {"", "   ", "foo(x)", "count(x)", "sum()", "sum(nope)", "sum(cost_micros) extra",
                          "sum(cost_micros) as 1bad", "sum(cost_micros) as a-b", "count_if(event_type=1 AND device=0)",
                          "count_if(event_type)", "sum_if(cost_micros)", "count", "count(),"}) {
    EXPECT_THROW(parse_aggs(bad), std::invalid_argument) << "'" << bad << "' should not parse";
  }
}

TEST(AggParser, OutputColumnsRejectDuplicates) {
  EXPECT_NO_THROW(output_columns({ColumnId::CampaignId}, parse_aggs("count(), sum(cost_micros)")));
  EXPECT_THROW(output_columns({ColumnId::CampaignId}, parse_aggs("count() as campaign_id")), std::invalid_argument);
  EXPECT_THROW(output_columns({}, parse_aggs("count(), count()")), std::invalid_argument);
  const auto cols = output_columns({ColumnId::Country}, parse_aggs("count() as n"));
  ASSERT_EQ(cols.size(), 2u);
  EXPECT_EQ(cols[0], (OutputColumn{"country", ColumnType::U16}));
  EXPECT_EQ(cols[1], (OutputColumn{"n", ColumnType::U64}));
}

// ---- HashAggregate ---------------------------------------------------------------------

// Eight rows, three campaigns. Written out so each expected value below is
// a sum a reader can check by hand.
//   row:        0   1   2   3   4   5   6   7
//   campaign:   1   2   1   2   3   1   2   3
//   cost:      -5  10   7  -3   0 100   1   2
//   type:       0   1   0   1   2   1   0   2
FakeBatch eight_rows() {
  FakeBatch fb(8);
  fb.set(ColumnId::CampaignId, {1, 2, 1, 2, 3, 1, 2, 3});
  fb.set_signed(ColumnId::CostMicros, {-5, 10, 7, -3, 0, 100, 1, 2});
  fb.set(ColumnId::EventType, {0, 1, 0, 1, 2, 1, 0, 2});
  return fb;
}

const char* kAllAggs =
    "count(), sum(cost_micros) as cost, min(cost_micros) as lo, max(cost_micros) as hi, "
    "count_if(event_type=1) as clicks, sum_if(cost_micros, event_type=1) as click_cost";

TEST(HashAggregate, EveryAggregateOnEightRows) {
  HashAggregate agg({ColumnId::CampaignId}, parse_aggs(kAllAggs));
  FakeBatch fb = eight_rows();
  agg.consume(fb.b);
  const ResultTable t = agg.finish();
  ASSERT_EQ(t.rows(), 3u);
  ASSERT_EQ(t.schema.size(), 7u);
  EXPECT_EQ(t.schema[0].name, "campaign_id");

  // Groups come out in first-seen order: 1, 2, 3.
  EXPECT_EQ(cell(t, "campaign_id", 0), 1u);
  EXPECT_EQ(cell(t, "count", 0), 3u);
  EXPECT_EQ(scell(t, "cost", 0), -5 + 7 + 100);
  EXPECT_EQ(scell(t, "lo", 0), -5);
  EXPECT_EQ(scell(t, "hi", 0), 100);
  EXPECT_EQ(cell(t, "clicks", 0), 1u);
  EXPECT_EQ(scell(t, "click_cost", 0), 100);

  EXPECT_EQ(cell(t, "campaign_id", 1), 2u);
  EXPECT_EQ(cell(t, "count", 1), 3u);
  EXPECT_EQ(scell(t, "cost", 1), 10 - 3 + 1);
  EXPECT_EQ(scell(t, "lo", 1), -3);
  EXPECT_EQ(scell(t, "hi", 1), 10);
  EXPECT_EQ(cell(t, "clicks", 1), 2u);
  EXPECT_EQ(scell(t, "click_cost", 1), 7);

  EXPECT_EQ(cell(t, "campaign_id", 2), 3u);
  EXPECT_EQ(cell(t, "count", 2), 2u);
  EXPECT_EQ(scell(t, "cost", 2), 2);
  EXPECT_EQ(scell(t, "lo", 2), 0);
  EXPECT_EQ(scell(t, "hi", 2), 2);
  EXPECT_EQ(cell(t, "clicks", 2), 0u);
  EXPECT_EQ(scell(t, "click_cost", 2), 0);

  const AggStats s = agg.stats();
  EXPECT_EQ(s.rows, 8u);
  EXPECT_EQ(s.batches, 1u);
  EXPECT_EQ(s.groups, 3u);
  EXPECT_GE(s.probes, 8u);
}

TEST(HashAggregate, HonoursTheSelectionVector) {
  HashAggregate agg({ColumnId::CampaignId}, parse_aggs("count(), sum(cost_micros) as cost"));
  FakeBatch fb = eight_rows();
  fb.select({1, 3, 5});  // rows 1 and 3 are campaign 2, row 5 is campaign 1
  agg.consume(fb.b);
  const ResultTable t = agg.finish();
  ASSERT_EQ(t.rows(), 2u);
  EXPECT_EQ(cell(t, "campaign_id", 0), 2u);  // first selected row decides the order
  EXPECT_EQ(cell(t, "count", 0), 2u);
  EXPECT_EQ(scell(t, "cost", 0), 7);
  EXPECT_EQ(cell(t, "campaign_id", 1), 1u);
  EXPECT_EQ(cell(t, "count", 1), 1u);
  EXPECT_EQ(scell(t, "cost", 1), 100);
}

TEST(HashAggregate, EmptySelectionAndUnloadedColumn) {
  HashAggregate agg({ColumnId::CampaignId}, parse_aggs("count()"));
  FakeBatch fb = eight_rows();
  fb.select({});
  agg.consume(fb.b);
  EXPECT_EQ(agg.groups(), 0u);
  EXPECT_EQ(agg.finish().rows(), 0u);

  HashAggregate needs_country({ColumnId::Country}, parse_aggs("count()"));
  FakeBatch fb2 = eight_rows();  // country not set
  EXPECT_THROW(needs_country.consume(fb2.b), std::invalid_argument);
}

TEST(HashAggregate, NoGroupColumnsGivesOneGlobalRow) {
  HashAggregate agg({}, parse_aggs("count(), sum(cost_micros) as cost, count_if(event_type=2) as conv"));
  FakeBatch fb = eight_rows();
  agg.consume(fb.b);
  agg.consume(fb.b);  // twice: counts double
  const ResultTable t = agg.finish();
  ASSERT_EQ(t.rows(), 1u);
  EXPECT_EQ(t.schema.size(), 3u);
  EXPECT_EQ(cell(t, "count", 0), 16u);
  EXPECT_EQ(scell(t, "cost", 0), 2 * 112);
  EXPECT_EQ(cell(t, "conv", 0), 4u);
  EXPECT_EQ(agg.columns_needed(), (std::vector<ColumnId>{ColumnId::CostMicros, ColumnId::EventType}));
}

TEST(HashAggregate, TwoGroupColumns) {
  HashAggregate agg({ColumnId::Country, ColumnId::Device}, parse_aggs("count()"));
  FakeBatch fb(6);
  fb.set(ColumnId::Country, {840, 840, 356, 840, 356, 840});
  fb.set(ColumnId::Device, {0, 1, 0, 0, 0, 1});
  agg.consume(fb.b);
  const ResultTable t = agg.finish();
  ASSERT_EQ(t.rows(), 3u);
  EXPECT_EQ(cell(t, "country", 0), 840u); EXPECT_EQ(cell(t, "device", 0), 0u); EXPECT_EQ(cell(t, "count", 0), 2u);
  EXPECT_EQ(cell(t, "country", 1), 840u); EXPECT_EQ(cell(t, "device", 1), 1u); EXPECT_EQ(cell(t, "count", 1), 2u);
  EXPECT_EQ(cell(t, "country", 2), 356u); EXPECT_EQ(cell(t, "device", 2), 0u); EXPECT_EQ(cell(t, "count", 2), 2u);
}

TEST(HashAggregate, MinMaxAtSignedAndUnsignedExtremes) {
  constexpr int64_t lo = std::numeric_limits<int64_t>::min(), hi = std::numeric_limits<int64_t>::max();
  HashAggregate agg({}, parse_aggs("min(cost_micros) as lo, max(cost_micros) as hi, min(video_id) as vlo, "
                                   "max(video_id) as vhi, sum(cost_micros) as s"));
  FakeBatch fb(4);
  fb.set_signed(ColumnId::CostMicros, {hi, -1, lo, 0});
  fb.set(ColumnId::VideoId, {~uint64_t{0}, 0, 5, 1});
  agg.consume(fb.b);
  const ResultTable t = agg.finish();
  EXPECT_EQ(scell(t, "lo", 0), lo);
  EXPECT_EQ(scell(t, "hi", 0), hi);
  EXPECT_EQ(cell(t, "vlo", 0), 0u);
  EXPECT_EQ(cell(t, "vhi", 0), ~uint64_t{0});
  EXPECT_EQ(scell(t, "s", 0), hi + (-1) + lo + 0);  // = -2, exact under wrapping arithmetic
}

TEST(HashAggregate, MergeEqualsOnePass) {
  const auto aggs = parse_aggs(kAllAggs);
  HashAggregate whole({ColumnId::CampaignId}, aggs);
  HashAggregate first({ColumnId::CampaignId}, aggs);
  HashAggregate second({ColumnId::CampaignId}, aggs);
  FakeBatch fb = eight_rows();
  whole.consume(fb.b);
  FakeBatch a = eight_rows();
  a.select({0, 1, 2, 3});       // campaigns 1 and 2 only
  FakeBatch b = eight_rows();
  b.select({4, 5, 6, 7});       // campaign 3 appears only here
  first.consume(a.b);
  second.consume(b.b);
  first.merge(second);
  const ResultTable w = whole.finish(), m = first.finish();
  ASSERT_EQ(m.rows(), w.rows());
  // Same groups in the same first-seen order (1, 2 from `first`, then 3).
  for (size_t c = 0; c < w.schema.size(); ++c) EXPECT_EQ(m.cols[c], w.cols[c]) << w.schema[c].name;
  EXPECT_EQ(first.stats().rows, 8u);
  EXPECT_EQ(first.stats().batches, 2u);
  EXPECT_EQ(first.stats().probes, whole.stats().probes + 3)
      << "merge probes once per group of the other table";

  HashAggregate other_plan({ColumnId::Country}, aggs);
  EXPECT_THROW(first.merge(other_plan), std::invalid_argument);
}

TEST(HashAggregate, ManyGroupsAcrossManyBatchesMatchAMap) {
  // 50 batches of 1024 rows, campaign = (i * 7919) % 5000, cost = i % 1000 - 500.
  HashAggregate agg({ColumnId::CampaignId}, parse_aggs("count(), sum(cost_micros) as cost, min(cost_micros) as lo"));
  std::map<uint64_t, std::array<int64_t, 3>> ref;
  uint64_t i = 0;
  for (int batch = 0; batch < 50; ++batch) {
    FakeBatch fb(1024);
    std::vector<uint64_t> camp(1024);
    std::vector<int64_t> cost(1024);
    for (uint32_t r = 0; r < 1024; ++r, ++i) {
      camp[r] = (i * 7919) % 5000;
      cost[r] = static_cast<int64_t>(i % 1000) - 500;
      auto& e = ref.try_emplace(camp[r], std::array<int64_t, 3>{0, 0, cost[r]}).first->second;
      e[0] += 1;
      e[1] += cost[r];
      e[2] = std::min(e[2], cost[r]);
    }
    fb.set(ColumnId::CampaignId, camp);
    fb.set_signed(ColumnId::CostMicros, cost);
    agg.consume(fb.b);
  }
  const ResultTable t = agg.finish();
  ASSERT_EQ(t.rows(), ref.size());
  for (size_t r = 0; r < t.rows(); ++r) {
    const auto& e = ref.at(cell(t, "campaign_id", r));
    ASSERT_EQ(static_cast<int64_t>(cell(t, "count", r)), e[0]);
    ASSERT_EQ(scell(t, "cost", r), e[1]);
    ASSERT_EQ(scell(t, "lo", r), e[2]);
  }
  EXPECT_GT(agg.stats().grows, 0u);
  EXPECT_LT(static_cast<double>(agg.stats().probes) / static_cast<double>(agg.stats().rows), 2.5);
}

}  // namespace
