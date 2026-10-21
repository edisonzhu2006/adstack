// ORDER BY / LIMIT: the parser, signed ordering, multi-key order, ties, and
// the partial-sort path agreeing with a full sort for every LIMIT.

#include <gtest/gtest.h>

#include <algorithm>
#include <random>

#include "src/sort.h"

using namespace colstore;

namespace {

// schema: campaign_id u32, cost i64, count u64
ResultTable table(std::vector<uint64_t> campaign, std::vector<int64_t> cost, std::vector<uint64_t> count) {
  ResultTable t;
  t.schema = {{"campaign_id", ColumnType::U32}, {"cost", ColumnType::I64}, {"count", ColumnType::U64}};
  std::vector<uint64_t> cost_bits;
  for (int64_t c : cost) cost_bits.push_back(widen(c));
  t.cols = {std::move(campaign), std::move(cost_bits), std::move(count)};
  return t;
}

TEST(OrderParser, NamesAndDirections) {
  const ResultTable t = table({}, {}, {});
  const auto o = parse_order("cost desc, campaign_id, count ASC", t.schema);
  ASSERT_EQ(o.size(), 3u);
  EXPECT_EQ(o[0], (OrderSpec{1, true}));
  EXPECT_EQ(o[1], (OrderSpec{0, false}));
  EXPECT_EQ(o[2], (OrderSpec{2, false}));
  EXPECT_EQ(to_string(o[0], t.schema), "cost desc");
  EXPECT_TRUE(parse_order("", t.schema).empty());
  EXPECT_TRUE(parse_order("   ", t.schema).empty());
  EXPECT_THROW(parse_order("nope", t.schema), std::invalid_argument);
  EXPECT_THROW(parse_order("cost down", t.schema), std::invalid_argument);
  EXPECT_THROW(parse_order("cost desc,", t.schema), std::invalid_argument);
  EXPECT_THROW(parse_order("cost desc extra", t.schema), std::invalid_argument);
}

TEST(SortLimit, SignedColumnSortsAsSigned) {
  const ResultTable t = table({1, 2, 3, 4, 5}, {5, -100, 0, 7, -1}, {1, 1, 1, 1, 1});
  EXPECT_EQ(sort_limit(t, parse_order("cost", t.schema), 0), (std::vector<uint32_t>{1, 4, 2, 0, 3}));
  EXPECT_EQ(sort_limit(t, parse_order("cost desc", t.schema), 0), (std::vector<uint32_t>{3, 0, 2, 4, 1}));
  EXPECT_EQ(sort_limit(t, parse_order("cost desc", t.schema), 2), (std::vector<uint32_t>{3, 0}));
}

TEST(SortLimit, TiesFallBackToTheNextKeyThenRowIndex) {
  const ResultTable t = table({9, 3, 5, 3, 9}, {1, 1, 2, 1, 1}, {0, 0, 0, 0, 0});
  // cost desc: row 2 first (2); the rest tie on cost and count, then by campaign asc, then row index.
  EXPECT_EQ(sort_limit(t, parse_order("cost desc, count, campaign_id", t.schema), 0),
            (std::vector<uint32_t>{2, 1, 3, 0, 4}));
  // Only cost: every tie broken by row index.
  EXPECT_EQ(sort_limit(t, parse_order("cost desc", t.schema), 0), (std::vector<uint32_t>{2, 0, 1, 3, 4}));
}

TEST(SortLimit, NoOrderMeansIndexOrderTruncated) {
  const ResultTable t = table({1, 2, 3}, {0, 0, 0}, {0, 0, 0});
  EXPECT_EQ(sort_limit(t, {}, 0), (std::vector<uint32_t>{0, 1, 2}));
  EXPECT_EQ(sort_limit(t, {}, 2), (std::vector<uint32_t>{0, 1}));
  EXPECT_EQ(sort_limit(t, {}, 10), (std::vector<uint32_t>{0, 1, 2}));
  EXPECT_TRUE(sort_limit(table({}, {}, {}), parse_order("cost", t.schema), 5).empty());
}

TEST(SortLimit, PartialSortMatchesFullSortPrefixForEveryLimit) {
  // 2,000 rows with heavy ties (cost in -5..5, count in 0..2) so the
  // tiebreak rules matter, in random order.
  std::mt19937 rng(3);
  std::vector<uint64_t> camp(2'000), count(2'000);
  std::vector<int64_t> cost(2'000);
  for (size_t i = 0; i < 2'000; ++i) {
    camp[i] = rng() % 1'000;
    cost[i] = static_cast<int64_t>(rng() % 11) - 5;
    count[i] = rng() % 3;
  }
  const ResultTable t = table(camp, cost, count);
  const auto order = parse_order("cost desc, count, campaign_id desc", t.schema);
  const std::vector<uint32_t> full = sort_limit(t, order, 0);
  ASSERT_EQ(full.size(), 2'000u);
  for (size_t i = 1; i < full.size(); ++i) {
    // Verify the full order independently: (cost desc, count asc, campaign desc, index asc).
    const uint32_t a = full[i - 1], b = full[i];
    const auto key = [&](uint32_t r) {
      return std::make_tuple(-as_signed(t.at(1, r)), t.at(2, r), -static_cast<int64_t>(t.at(0, r)), static_cast<int64_t>(r));
    };
    ASSERT_LT(key(a), key(b)) << "at position " << i;
  }
  for (size_t limit : {1u, 2u, 7u, 20u, 100u, 999u, 1'999u, 2'000u, 5'000u}) {
    const std::vector<uint32_t> part = sort_limit(t, order, limit);
    ASSERT_EQ(part.size(), std::min<size_t>(limit, 2'000));
    ASSERT_TRUE(std::equal(part.begin(), part.end(), full.begin())) << "limit " << limit;
  }
}

}  // namespace
