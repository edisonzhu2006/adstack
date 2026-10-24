// End to end: the three benchmark query shapes through run_aggregate_query
// on a sample file, against an in-test brute force over the same events;
// identical answers at 1..5 threads and at batch sizes 1 and 1024; the
// Scanner stride covering every row group exactly once; check_against_ads
// passing on a correct result and failing on a corrupted one; CSV output.

#include <gtest/gtest.h>

#include <map>

#include "src/query.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

constexpr uint64_t kRows = 7'000;
constexpr uint32_t kGroup = 3'000;

class QueryFiles : public ::testing::Test {
 protected:
  void SetUp() override {
    ColWriter w(col_.path(), kGroup, EncodingPolicy::Auto);
    adstack::EventWriter ads(ads_.path());
    for (uint64_t i = 0; i < kRows; ++i) {
      w.append(sample_event(i));
      ads.write(sample_event(i));
    }
    w.finish();
    ads.flush();
  }
  TempFile col_{"query.col"};
  TempFile ads_{"query.ads"};
};

QueryPlan plan(std::string_view where, std::vector<ColumnId> group, std::string_view aggs, std::string_view order = "",
               size_t limit = 0, unsigned threads = 1) {
  QueryPlan p;
  p.where = parse_where(where);
  p.group = std::move(group);
  p.aggs = parse_aggs(aggs);
  p.order = parse_order(order, output_columns(p.group, p.aggs));
  p.limit = limit;
  p.threads = threads;
  return p;
}

// Brute force in the test itself: map from group values to cells, using
// plain signed arithmetic on sample_event fields.
using Key = std::vector<uint64_t>;
std::map<Key, std::vector<int64_t>> brute(const QueryPlan& p) {
  std::map<Key, std::vector<int64_t>> out;
  for (uint64_t i = 0; i < kRows; ++i) {
    const adstack::Event e = sample_event(i);
    if (!matches_all(p.where, e)) continue;
    Key k;
    for (ColumnId c : p.group) k.push_back(event_field_bits(e, c));
    auto& cells = out.try_emplace(k, std::vector<int64_t>(p.aggs.size(), 0)).first->second;
    for (size_t a = 0; a < p.aggs.size(); ++a) {
      const AggSpec& s = p.aggs[a];
      const int64_t v = s.col ? as_signed(event_field_bits(e, *s.col)) : 0;
      const bool pass = !s.cond || s.cond->matches(event_field_bits(e, s.cond->col));
      switch (s.kind) {
        case AggKind::Count: cells[a] += 1; break;
        case AggKind::CountIf: cells[a] += pass; break;
        case AggKind::Sum: cells[a] += v; break;
        case AggKind::SumIf: cells[a] += pass ? v : 0; break;
        case AggKind::Min: ADD_FAILURE() << "not used here"; break;
        case AggKind::Max: ADD_FAILURE() << "not used here"; break;
      }
    }
  }
  return out;
}

void expect_matches_brute(const QueryResult& r, const QueryPlan& p) {
  const auto want = brute(p);
  ASSERT_EQ(r.table.rows(), want.size());
  const size_t ng = p.group.size();
  for (size_t row = 0; row < r.table.rows(); ++row) {
    Key k;
    for (size_t c = 0; c < ng; ++c) k.push_back(r.table.at(c, row));
    const auto it = want.find(k);
    ASSERT_NE(it, want.end());
    for (size_t a = 0; a < p.aggs.size(); ++a) ASSERT_EQ(as_signed(r.table.at(ng + a, row)), it->second[a]);
  }
}

TEST_F(QueryFiles, Q1PointFilterCount) {
  const QueryPlan p = plan("country=840 AND event_type=1", {}, "count()");
  const QueryResult r = run_aggregate_query(col_.path(), p);
  ASSERT_EQ(r.table.rows(), 1u);
  // sample_event: country 840 when i odd, type i % 3 == 1: i in {1, 7, 13, ...}, i.e. i % 6 == 1.
  EXPECT_EQ(r.table.at(0, 0), (kRows + 4) / 6);
  EXPECT_EQ(r.rows, (std::vector<uint32_t>{0}));
  expect_matches_brute(r, p);
  EXPECT_TRUE(check_against_ads(ads_.path(), p, r).ok);
}

TEST_F(QueryFiles, Q2TimeRangeGroupBy) {
  const int64_t t0 = sample_event(2'500).event_time_us, t1 = sample_event(4'500).event_time_us;
  const QueryPlan p = plan("event_time_us BETWEEN " + std::to_string(t0) + " AND " + std::to_string(t1),
                           {ColumnId::CampaignId}, "sum(cost_micros), count()", "sum_cost_micros desc", 10);
  const QueryResult r = run_aggregate_query(col_.path(), p);
  expect_matches_brute(r, p);
  EXPECT_EQ(r.scan.scan.row_groups_skipped_zone, 1u);  // group 0 (rows 0..2999) is before t0
  EXPECT_EQ(r.rows.size(), 10u);
  for (size_t i = 1; i < r.rows.size(); ++i) {
    EXPECT_GE(as_signed(r.table.at(1, r.rows[i - 1])), as_signed(r.table.at(1, r.rows[i])));
  }
  const CheckReport c = check_against_ads(ads_.path(), p, r);
  EXPECT_TRUE(c.ok) << c.detail;
}

TEST_F(QueryFiles, Q3FullGroupByWithConditionalAggregates) {
  const QueryPlan p = plan("", {ColumnId::CampaignId},
                           "count_if(event_type=0) as impressions, count_if(event_type=1) as clicks, "
                           "count_if(event_type=2) as conversions, sum(cost_micros) as cost, "
                           "sum_if(cost_micros, event_type=1) as click_cost");
  const QueryResult r = run_aggregate_query(col_.path(), p);
  expect_matches_brute(r, p);
  EXPECT_TRUE(r.rows.size() == r.table.rows());
  // impressions + clicks + conversions == rows of the group, for every group.
  uint64_t total = 0;
  for (size_t row = 0; row < r.table.rows(); ++row) {
    total += r.table.at(1, row) + r.table.at(2, row) + r.table.at(3, row);
  }
  EXPECT_EQ(total, kRows);
  EXPECT_TRUE(check_against_ads(ads_.path(), p, r).ok);
}

TEST_F(QueryFiles, ThreadsDoNotChangeTheAnswer) {
  const QueryPlan one = plan("cost_micros > 0", {ColumnId::Country, ColumnId::Device},
                             "count(), sum(cost_micros) as cost, min(cost_micros) as lo, max(cost_micros) as hi",
                             "cost desc", 4, 1);
  const QueryResult r1 = run_aggregate_query(col_.path(), one);
  for (unsigned threads : {2u, 3u, 5u, 17u}) {  // 17 > the file's 3 row groups: idle threads
    QueryPlan p = one;
    p.threads = threads;
    const QueryResult r = run_aggregate_query(col_.path(), p);
    ASSERT_EQ(r.rows.size(), r1.rows.size()) << threads;
    for (size_t i = 0; i < r.rows.size(); ++i) {
      for (size_t c = 0; c < r.table.schema.size(); ++c) {
        ASSERT_EQ(r.table.at(c, r.rows[i]), r1.table.at(c, r1.rows[i])) << "threads " << threads << " row " << i;
      }
    }
    EXPECT_EQ(r.scan.scan.row_groups_total, 3u) << "per-thread totals must add up to the file";
    EXPECT_EQ(r.scan.rows_passed, r1.scan.rows_passed);
    EXPECT_EQ(r.agg.rows, r1.agg.rows);
    EXPECT_GE(r.agg.probes, r.agg.rows) << "merged stats must count every thread's probes";
    const CheckReport c = check_against_ads(ads_.path(), p, r);
    EXPECT_TRUE(c.ok) << c.detail;
  }
}

TEST_F(QueryFiles, BatchSizeDoesNotChangeTheAnswer) {
  QueryPlan p = plan("event_type=1", {ColumnId::CampaignId}, "count(), sum(cost_micros)", "count desc", 7);
  const QueryResult big = run_aggregate_query(col_.path(), p);
  p.batch_rows = 1;
  const QueryResult one = run_aggregate_query(col_.path(), p);
  ASSERT_EQ(one.rows.size(), big.rows.size());
  for (size_t i = 0; i < one.rows.size(); ++i) {
    for (size_t c = 0; c < 3; ++c) ASSERT_EQ(one.table.at(c, one.rows[i]), big.table.at(c, big.rows[i]));
  }
  EXPECT_GT(one.agg.batches, big.agg.batches);
}

TEST_F(QueryFiles, EffectiveOrderAppendsGroupColumns) {
  QueryPlan p = plan("", {ColumnId::Country, ColumnId::Device}, "count()", "count desc");
  const auto schema = output_columns(p.group, p.aggs);
  EXPECT_EQ(effective_order(p, schema), (std::vector<OrderSpec>{{2, true}, {0, false}, {1, false}}));
  p.order = parse_order("device desc", schema);
  EXPECT_EQ(effective_order(p, schema), (std::vector<OrderSpec>{{1, true}, {0, false}}));
  p.order.clear();
  EXPECT_TRUE(effective_order(p, schema).empty());  // no order, no limit: engine order
  p.limit = 3;
  EXPECT_EQ(effective_order(p, schema), (std::vector<OrderSpec>{{0, false}, {1, false}}));
}

TEST_F(QueryFiles, CheckDetectsAWrongResult) {
  const QueryPlan p = plan("", {ColumnId::CampaignId}, "count(), sum(cost_micros) as cost", "cost desc", 5);
  QueryResult r = run_aggregate_query(col_.path(), p);
  ASSERT_TRUE(check_against_ads(ads_.path(), p, r).ok);

  QueryResult wrong_cell = r;
  wrong_cell.table.cols[1][3] += 1;
  const CheckReport c1 = check_against_ads(ads_.path(), p, wrong_cell);
  EXPECT_FALSE(c1.ok);
  EXPECT_NE(c1.detail.find("aggregates differ"), std::string::npos) << c1.detail;

  QueryResult wrong_order = r;
  std::swap(wrong_order.rows[0], wrong_order.rows[1]);
  const CheckReport c2 = check_against_ads(ads_.path(), p, wrong_order);
  EXPECT_FALSE(c2.ok);
  EXPECT_NE(c2.detail.find("output row 0 differs"), std::string::npos) << c2.detail;

  QueryResult missing_group = r;
  for (auto& col : missing_group.table.cols) col.pop_back();
  EXPECT_FALSE(check_against_ads(ads_.path(), p, missing_group).ok);
}

TEST_F(QueryFiles, ScannerStrideCoversEveryGroupOnce) {
  ColReader r(col_.path());
  for (size_t stride : {1u, 2u, 3u, 4u}) {
    uint64_t rows = 0;
    size_t totals = 0;
    for (size_t first = 0; first < stride; ++first) {
      Scanner s(r, {}, {ColumnId::CampaignId}, kVectorSize, first, stride);
      Batch b;
      while (s.next(b)) rows += b.n;
      totals += s.stats().row_groups_total;
    }
    EXPECT_EQ(rows, kRows) << "stride " << stride;
    EXPECT_EQ(totals, 3u) << "stride " << stride;
  }
  EXPECT_THROW(Scanner(r, {}, {}, kVectorSize, 0, 0), std::invalid_argument);
}

TEST_F(QueryFiles, CsvOutput) {
  const QueryPlan p = plan("cost_micros < -900", {ColumnId::Device}, "count(), min(cost_micros) as lo", "device");
  const QueryResult r = run_aggregate_query(col_.path(), p);
  // cost = 3i - 1000 < -900 for i < 34: devices 0 (17 even i, min -1000) and 1 (17 odd i, min -997).
  FILE* f = std::tmpfile();
  ASSERT_NE(f, nullptr);
  write_csv(f, r);
  std::rewind(f);
  char buf[256];
  std::string text;
  while (std::fgets(buf, sizeof buf, f)) text += buf;
  std::fclose(f);
  EXPECT_EQ(text, "device,count,lo\n0,17,-1000\n1,17,-997\n");
}

std::string csv_text(const QueryResult& r) {
  FILE* f = std::tmpfile();
  EXPECT_NE(f, nullptr);
  write_csv(f, r);
  std::rewind(f);
  char buf[256];
  std::string text;
  while (std::fgets(buf, sizeof buf, f)) text += buf;
  std::fclose(f);
  return text;
}

TEST_F(QueryFiles, GlobalAggregateOverZeroRowsIsOneRowWithNulls) {
  // SQL: SELECT COUNT(*), SUM(x), MIN(x) ... WHERE <nothing matches> is one
  // row (0, NULL, NULL), not zero rows. country=1 occurs in no row.
  for (unsigned threads : {1u, 2u, 17u}) {
    const QueryPlan p = plan("country=1", {}, "count(), sum(cost_micros), min(cost_micros), max(cost_micros), "
                             "count_if(event_type=1), sum_if(cost_micros, event_type=1)", "", 0, threads);
    const QueryResult r = run_aggregate_query(col_.path(), p);
    ASSERT_EQ(r.table.rows(), 1u) << threads;
    EXPECT_EQ(r.rows, (std::vector<uint32_t>{0}));
    EXPECT_FALSE(r.table.is_null(0, 0));
    EXPECT_EQ(r.table.at(0, 0), 0u);  // count
    EXPECT_TRUE(r.table.is_null(1, 0));
    EXPECT_TRUE(r.table.is_null(2, 0));
    EXPECT_TRUE(r.table.is_null(3, 0));
    EXPECT_FALSE(r.table.is_null(4, 0));
    EXPECT_EQ(r.table.at(4, 0), 0u);  // count_if
    EXPECT_TRUE(r.table.is_null(5, 0));
    EXPECT_EQ(csv_text(r), "count,sum_cost_micros,min_cost_micros,max_cost_micros,count_if_event_type_eq_1,"
                           "sum_if_cost_micros_event_type_eq_1\n0,,,,0,\n");
    const CheckReport c = check_against_ads(ads_.path(), p, r);
    EXPECT_TRUE(c.ok) << c.detail;
  }
  // The same query with a LIMIT goes through the ordered comparison too.
  const QueryPlan limited = plan("country=1", {}, "count(), sum(cost_micros)", "", 5);
  const QueryResult rl = run_aggregate_query(col_.path(), limited);
  const CheckReport cl = check_against_ads(ads_.path(), limited, rl);
  EXPECT_TRUE(cl.ok) << cl.detail;
}

TEST_F(QueryFiles, CheckRejectsAMissingOrWrongEmptyGlobalRow) {
  const QueryPlan p = plan("country=1", {}, "count(), sum(cost_micros)");
  const QueryResult r = run_aggregate_query(col_.path(), p);
  ASSERT_TRUE(check_against_ads(ads_.path(), p, r).ok);
  // The old engine behaviour (zero rows) must now fail the check.
  QueryResult no_row = r;
  for (auto& col : no_row.table.cols) col.clear();
  no_row.table.nulls.clear();
  no_row.rows.clear();
  EXPECT_FALSE(check_against_ads(ads_.path(), p, no_row).ok);
  // SUM reported as 0 instead of NULL must fail too.
  QueryResult zero_sum = r;
  zero_sum.table.nulls.clear();
  EXPECT_FALSE(check_against_ads(ads_.path(), p, zero_sum).ok);
  // A NULL where a group saw rows must fail.
  const QueryPlan some = plan("country=840", {}, "count(), sum(cost_micros)");
  QueryResult r2 = run_aggregate_query(col_.path(), some);
  ASSERT_TRUE(check_against_ads(ads_.path(), some, r2).ok);
  EXPECT_FALSE(r2.table.is_null(1, 0));
  r2.table.nulls.assign(r2.table.schema.size(), std::vector<uint8_t>(1, 0));
  r2.table.nulls[1][0] = 1;
  r2.table.cols[1][0] = 0;
  EXPECT_FALSE(check_against_ads(ads_.path(), some, r2).ok);
}

TEST_F(QueryFiles, GlobalAggregateSkipsTheHashTableAndMatchesBruteForce) {
  const QueryPlan p = plan("event_type=1", {}, "count(), sum(cost_micros), count_if(country=840)");
  const QueryResult r = run_aggregate_query(col_.path(), p);
  ASSERT_EQ(r.table.rows(), 1u);
  EXPECT_TRUE(r.table.nulls.empty());
  expect_matches_brute(r, p);
  EXPECT_TRUE(check_against_ads(ads_.path(), p, r).ok);
  // One probe in total: creating group 0. Rows are not hashed.
  EXPECT_EQ(r.agg.probes, 1u);
  EXPECT_GT(r.agg.rows, 0u);
}

TEST_F(QueryFiles, GroupByOverZeroRowsIsStillZeroRows) {
  const QueryPlan p = plan("country=1", {ColumnId::CampaignId}, "count(), sum(cost_micros)", "count desc", 3);
  const QueryResult r = run_aggregate_query(col_.path(), p);
  EXPECT_EQ(r.table.rows(), 0u);
  EXPECT_EQ(csv_text(r), "campaign_id,count,sum_cost_micros\n");
  const CheckReport c = check_against_ads(ads_.path(), p, r);
  EXPECT_TRUE(c.ok) << c.detail;
}

TEST_F(QueryFiles, GroupKeyFitDoesNotDependOnColumnOrder) {
  // 32 + 64 + 32 bits: fits in 128 whichever order the columns are named.
  for (const std::vector<ColumnId>& g : {std::vector<ColumnId>{ColumnId::CampaignId, ColumnId::EventId, ColumnId::AdvertiserId},
                                         std::vector<ColumnId>{ColumnId::EventId, ColumnId::CampaignId, ColumnId::AdvertiserId},
                                         std::vector<ColumnId>{ColumnId::AdvertiserId, ColumnId::CampaignId, ColumnId::EventId}}) {
    const QueryPlan p = plan("event_type=2", g, "count()", "count desc", 5);
    const QueryResult r = run_aggregate_query(col_.path(), p);
    EXPECT_EQ(r.table.schema[0].name, spec_of(event_schema(), g[0]).name) << "output keeps the order given";
    const CheckReport c = check_against_ads(ads_.path(), p, r);
    EXPECT_TRUE(c.ok) << c.detail;
  }
}

TEST_F(QueryFiles, BadPlansAreRejected) {
  QueryPlan p = plan("", {ColumnId::CampaignId}, "count()");
  p.threads = 0;
  EXPECT_THROW(run_aggregate_query(col_.path(), p), std::invalid_argument);
  p.threads = 1;
  p.aggs = parse_aggs("count() as campaign_id");
  EXPECT_THROW(run_aggregate_query(col_.path(), p), std::invalid_argument);
  EXPECT_THROW(run_aggregate_query("/nonexistent/file.col", plan("", {}, "count()")), std::runtime_error);
}

}  // namespace
