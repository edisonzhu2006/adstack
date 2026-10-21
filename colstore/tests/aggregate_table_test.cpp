// The second table kind: streamkit's aggregate CSV through CsvRowSource,
// ColWriter::append_row, the reader, the scanner and the hash aggregate,
// with the brute-force check reading the CSV back. Also the guards that
// keep a plan or a predicate parsed for one table from running on the
// other, and the byte-level helpers the generic writer added.

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <limits>

#include "common/agg.h"
#include "src/encoding.h"
#include "src/query.h"
#include "src/reader.h"
#include "src/row_source.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

// Writes `text` verbatim to a temp file.
void write_text(const std::string& path, const std::string& text) {
  std::ofstream out(path, std::ios::binary);
  out << text;
}

const char* kHeader = "window_start_us,campaign_id,impressions,clicks,conversions,cost_micros\n";

// Deterministic aggregate rows: 3 campaigns x `windows` one-minute windows,
// in the interleaved order a multi-worker streamkit run produces. Written
// with common/agg.h's own CSV writer, so the format is streamkit's by
// construction, not by imitation.
std::vector<std::pair<adstack::WindowKey, adstack::Agg>> sample_aggregates(int windows) {
  std::vector<std::pair<adstack::WindowKey, adstack::Agg>> rows;
  for (int w = 0; w < windows; ++w) {
    for (uint32_t campaign : {7u, 3u, 11u}) {
      adstack::WindowKey k{1'767'225'600'000'000LL + int64_t{w} * 60'000'000, campaign};
      adstack::Agg a;
      a.impressions = static_cast<uint64_t>(10 + w + campaign);
      a.clicks = static_cast<uint64_t>(w % 3);
      a.conversions = static_cast<uint64_t>(campaign == 11 ? 1 : 0);
      a.cost_micros = int64_t{1000} * w * campaign - 5000;  // negative for small w
      rows.emplace_back(k, a);
    }
  }
  return rows;
}

void write_aggregates_csv(const std::string& path, int windows) {
  FILE* f = std::fopen(path.c_str(), "wb");
  ASSERT_NE(f, nullptr);
  adstack::write_csv_header(f);
  for (const auto& [k, a] : sample_aggregates(windows)) adstack::write_csv_row(f, k, a);
  std::fclose(f);
}

std::vector<uint64_t> row_bits(const adstack::WindowKey& k, const adstack::Agg& a) {
  return {widen(k.window_start_us), widen(k.campaign_id), widen(a.impressions),
          widen(a.clicks),          widen(a.conversions), widen(a.cost_micros)};
}

}  // namespace

// ---- CsvRowSource ------------------------------------------------------------------

TEST(CsvRowSource, ReadsStreamkitOutputExactly) {
  TempFile csv("agg_ok.csv");
  write_aggregates_csv(csv.path(), 4);
  CsvRowSource src(csv.path(), aggregate_schema());
  EXPECT_EQ(src.schema(), aggregate_schema());
  std::vector<uint64_t> row;
  size_t n = 0;
  for (const auto& [k, a] : sample_aggregates(4)) {
    ASSERT_TRUE(src.next(row)) << "row " << n;
    EXPECT_EQ(row, row_bits(k, a)) << "row " << n;
    ++n;
  }
  EXPECT_FALSE(src.next(row));
  EXPECT_EQ(src.rows_read(), 12u);
  EXPECT_EQ(csv_header(aggregate_schema()), std::string(kHeader).substr(0, std::string(kHeader).size() - 1));
}

TEST(CsvRowSource, RejectsWrongHeaderAndMalformedRows) {
  TempFile csv("agg_bad.csv");
  auto parse_all = [&](const std::string& text) {
    write_text(csv.path(), text);
    CsvRowSource src(csv.path(), aggregate_schema());
    std::vector<uint64_t> row;
    while (src.next(row)) {}
  };
  const std::string h = kHeader;
  EXPECT_THROW(parse_all(""), std::runtime_error);                                   // no header
  EXPECT_THROW(parse_all("a,b,c\n"), std::runtime_error);                            // wrong header
  EXPECT_THROW(parse_all("window_start_us,campaign_id\n"), std::runtime_error);      // too few columns
  EXPECT_THROW(parse_all(h + "1,2,3,4,5\n"), std::runtime_error);                    // five fields
  EXPECT_THROW(parse_all(h + "1,2,3,4,5,6,7\n"), std::runtime_error);                // seven fields
  EXPECT_THROW(parse_all(h + "1,2,3,x,5,6\n"), std::runtime_error);                  // not a number
  EXPECT_THROW(parse_all(h + "1,2,,4,5,6\n"), std::runtime_error);                   // empty field
  EXPECT_THROW(parse_all(h + "1,-2,3,4,5,6\n"), std::runtime_error);                 // negative u32
  EXPECT_THROW(parse_all(h + "1,4294967296,3,4,5,6\n"), std::runtime_error);         // u32 overflow
  EXPECT_THROW(parse_all(h + "1,2,3,4,5,9223372036854775808\n"), std::runtime_error);  // i64 overflow
  EXPECT_THROW(parse_all(h + "1,2,3,4,5,6\n\n1,2,3,4,5,6\n"), std::runtime_error);   // blank line inside
  EXPECT_NO_THROW(parse_all(h + "-1,4294967295,0,0,0,-9223372036854775808\n"));      // every edge value
  EXPECT_NO_THROW(parse_all(h + "1,2,3,4,5,6\r\n"));                                 // CRLF
  EXPECT_NO_THROW(parse_all(h + "1,2,3,4,5,6\n\n"));                                 // one trailing blank line
  EXPECT_NO_THROW(parse_all(h + "1,2,3,4,5,6"));                                     // no final newline
  EXPECT_NO_THROW(parse_all(h));                                                     // header only: zero rows
}

TEST(CsvRowSource, ReportsTheLineNumber) {
  TempFile csv("agg_line.csv");
  write_text(csv.path(), std::string(kHeader) + "1,2,3,4,5,6\n1,2,3,4,5,oops\n");
  CsvRowSource src(csv.path(), aggregate_schema());
  std::vector<uint64_t> row;
  ASSERT_TRUE(src.next(row));
  try {
    src.next(row);
    FAIL() << "expected an exception";
  } catch (const std::runtime_error& ex) {
    EXPECT_NE(std::string(ex.what()).find(":3: "), std::string::npos) << ex.what();  // header is line 1
    EXPECT_NE(std::string(ex.what()).find("cost_micros"), std::string::npos) << ex.what();
  }
}

// ---- writer and reader on the aggregate table -------------------------------------

TEST(AggregateTable, WriteReadRoundTrip) {
  TempFile col("agg_rt.col");
  const auto rows = sample_aggregates(300);  // 900 rows, groups of 256: 4 row groups
  {
    ColWriter w(col.path(), aggregate_schema(), 256, EncodingPolicy::Auto);
    EXPECT_EQ(w.schema(), aggregate_schema());
    for (const auto& [k, a] : rows) w.append_row(row_bits(k, a));
    EXPECT_EQ(w.rows_written(), 900u);
    w.finish();
  }
  ColReader r(col.path());
  EXPECT_FALSE(r.is_event_table());
  EXPECT_EQ(r.schema(), aggregate_schema());
  EXPECT_EQ(r.num_columns(), 6u);
  ASSERT_EQ(r.num_row_groups(), 4u);
  EXPECT_EQ(r.total_rows(), 900u);
  EXPECT_THROW(r.read_events(0), std::logic_error);
  EXPECT_THROW(r.chunk(0, ColumnId::CostMicros), std::out_of_range) << "index 9 is not in a 6-column table";

  size_t i = 0;
  for (size_t g = 0; g < r.num_row_groups(); ++g) {
    std::vector<std::vector<uint64_t>> cols;
    for (size_t c = 0; c < 6; ++c) cols.push_back(r.read_column_bits(g, column_id(c)));
    const auto starts = r.read_column<int64_t>(g, column_id(0));  // typed read works on any schema
    for (size_t k = 0; k < cols[0].size(); ++k, ++i) {
      const std::vector<uint64_t> want = row_bits(rows[i].first, rows[i].second);
      for (size_t c = 0; c < 6; ++c) ASSERT_EQ(cols[c][k], want[c]) << "row " << i << " col " << c;
      ASSERT_EQ(starts[k], rows[i].first.window_start_us);
    }
  }
  EXPECT_EQ(i, 900u);
  // Zone map of the time column is signed and per group.
  const ChunkMeta& t = r.chunk(0, column_id(0));
  EXPECT_EQ(as_signed(t.min_bits), rows[0].first.window_start_us);
  EXPECT_EQ(as_signed(t.max_bits), rows[255].first.window_start_us);
  const ChunkMeta& cost = r.chunk(0, column_id(5));
  EXPECT_LT(as_signed(cost.min_bits), 0);
}

TEST(AggregateTable, WriterGuards) {
  TempFile col("agg_guard.col");
  ColWriter w(col.path(), aggregate_schema(), 256, EncodingPolicy::PlainOnly);
  EXPECT_THROW(w.append(sample_event(0)), std::logic_error) << "an Event is not an aggregate row";
  EXPECT_THROW(w.append_row(std::vector<uint64_t>(5, 0)), std::invalid_argument) << "five values for six columns";
  // campaign_id is u32: a pattern with high bits set must not be truncated silently.
  EXPECT_THROW(w.append_row(std::vector<uint64_t>{0, uint64_t{1} << 32, 0, 0, 0, 0}), std::invalid_argument);
  EXPECT_NO_THROW(w.append_row(std::vector<uint64_t>{0, 0xFFFFFFFFu, 0, 0, 0, widen(int64_t{-1})}));
  EXPECT_THROW(ColWriter(col.path(), Schema{}, 256, EncodingPolicy::Auto), std::invalid_argument);
}

TEST(AggregateTable, GenericRowPathWritesTheSameBytesAsAppendEvent) {
  // append(Event) is append_row() on the widened fields; the two must
  // produce byte-identical files, which also pins the generic writer to
  // the m1/m2 files every earlier number was measured on.
  TempFile a("agg_same_a.col"), b("agg_same_b.col");
  {
    ColWriter wa(a.path(), 300, EncodingPolicy::Auto);
    ColWriter wb(b.path(), event_schema(), 300, EncodingPolicy::Auto);
    std::vector<uint64_t> row(kNumEventColumns);
    for (uint64_t i = 0; i < 1'000; ++i) {
      wa.append(sample_event(i));
      event_row_bits(sample_event(i), row.data());
      wb.append_row(row);
    }
    wa.finish();
    wb.finish();
  }
  std::ifstream fa(a.path(), std::ios::binary), fb(b.path(), std::ios::binary);
  const std::string sa((std::istreambuf_iterator<char>(fa)), {}), sb((std::istreambuf_iterator<char>(fb)), {});
  EXPECT_EQ(sa.size(), sb.size());
  EXPECT_TRUE(sa == sb);
}

TEST(Widen, BytesAndMinMax) {
  const int64_t vals[] = {5, -3, std::numeric_limits<int64_t>::min(), 7};
  const uint8_t* bytes = reinterpret_cast<const uint8_t*>(vals);
  std::vector<uint64_t> wide;
  widen_bytes(ColumnType::I64, bytes, 4, wide);
  ASSERT_EQ(wide.size(), 4u);
  EXPECT_EQ(as_signed(wide[1]), -3);
  uint64_t lo = 1, hi = 1;
  min_max_bytes(ColumnType::I64, bytes, 4, lo, hi);
  EXPECT_EQ(as_signed(lo), std::numeric_limits<int64_t>::min());
  EXPECT_EQ(as_signed(hi), 7);
  // The same bytes as u64 patterns: unsigned order, so -3 (0xFF..FD) is the largest.
  widen_bytes(ColumnType::U64, bytes, 4, wide);
  EXPECT_EQ(wide[1], widen(int64_t{-3}));
  min_max_bytes(ColumnType::U64, bytes, 4, lo, hi);
  EXPECT_EQ(lo, 5u);
  EXPECT_EQ(hi, widen(int64_t{-3}));
  const uint8_t narrow[] = {200, 1, 255};
  widen_bytes(ColumnType::U8, narrow, 3, wide);
  EXPECT_EQ(wide, (std::vector<uint64_t>{200, 1, 255}));
  min_max_bytes(ColumnType::U8, narrow, 3, lo, hi);
  EXPECT_EQ(lo, 1u);
  EXPECT_EQ(hi, 255u);
  min_max_bytes(ColumnType::U8, narrow, 0, lo, hi);
  EXPECT_EQ(lo, 0u);
  EXPECT_EQ(hi, 0u);
}

// ---- queries on the aggregate table ----------------------------------------------

class AggregateFiles : public ::testing::Test {
 protected:
  void SetUp() override {
    write_aggregates_csv(csv_.path(), 500);  // 1500 rows, 3 campaigns, windows 0..499
    CsvRowSource src(csv_.path(), aggregate_schema());
    ColWriter w(col_.path(), aggregate_schema(), 400, EncodingPolicy::Auto);
    std::vector<uint64_t> row;
    while (src.next(row)) w.append_row(row);
    w.finish();
  }
  TempFile csv_{"agg_q.csv"};
  TempFile col_{"agg_q.col"};
};

TEST_F(AggregateFiles, TopCampaignsBySpendInAnHourChecksAgainstTheCsv) {
  // The e2e query: spend per campaign over one hour of windows, top N.
  const int64_t h0 = 1'767'225'600'000'000LL, h1 = h0 + 3'600'000'000LL;
  QueryPlan p;
  p.schema = aggregate_schema();
  p.where = parse_where("window_start_us >= " + std::to_string(h0) + " AND window_start_us < " + std::to_string(h1),
                        aggregate_schema());
  p.group = {column_id(1)};  // campaign_id
  p.aggs = parse_aggs("sum(cost_micros) AS spend, sum(impressions) AS impressions, count()", aggregate_schema());
  p.order = parse_order("spend desc", output_columns(p.group, p.aggs, aggregate_schema()));
  p.limit = 2;
  const QueryResult r = run_aggregate_query(col_.path(), p);
  ASSERT_EQ(r.table.rows(), 3u);
  ASSERT_EQ(r.rows.size(), 2u);
  // By hand: windows 0..59 of the hour; spend(c) = sum_w (1000 w c - 5000) = 1000 c * 1770 - 300000.
  auto spend = [](int64_t c) { return 1000 * c * 1770 - 300'000; };
  EXPECT_EQ(r.table.at(0, r.rows[0]), 11u);
  EXPECT_EQ(as_signed(r.table.at(1, r.rows[0])), spend(11));
  EXPECT_EQ(r.table.at(0, r.rows[1]), 7u);
  EXPECT_EQ(as_signed(r.table.at(1, r.rows[1])), spend(7));
  EXPECT_EQ(r.table.at(3, r.rows[0]), 60u);
  EXPECT_GT(r.scan.scan.row_groups_skipped_zone, 0u) << "windows are written in time order: later groups skip";
  const CheckReport c = check_against_csv(csv_.path(), p, r);
  EXPECT_TRUE(c.ok) << c.detail;
  CsvRowSource reopened(csv_.path(), aggregate_schema());
  EXPECT_TRUE(check_against_rows(reopened, p, r).ok);

  // CSV output carries the aggregate table's names.
  FILE* f = std::tmpfile();
  ASSERT_NE(f, nullptr);
  write_csv(f, r);
  std::rewind(f);
  char buf[256];
  ASSERT_NE(std::fgets(buf, sizeof buf, f), nullptr);
  std::fclose(f);
  EXPECT_EQ(std::string(buf), "campaign_id,spend,impressions,count\n");
}

TEST_F(AggregateFiles, CheckSeesACorruptedResult) {
  QueryPlan p;
  p.schema = aggregate_schema();
  p.group = {column_id(1)};
  p.aggs = parse_aggs("sum(clicks) AS clicks", aggregate_schema());
  QueryResult r = run_aggregate_query(col_.path(), p);
  ASSERT_TRUE(check_against_csv(csv_.path(), p, r).ok);
  r.table.cols[1][0] += 1;
  const CheckReport c = check_against_csv(csv_.path(), p, r);
  EXPECT_FALSE(c.ok);
  EXPECT_NE(c.detail.find("aggregates differ"), std::string::npos) << c.detail;
  // A CSV that is not the plan's table is refused, not silently compared.
  TempFile other("agg_other.csv");
  write_text(other.path(), "a,b\n1,2\n");
  EXPECT_THROW(check_against_csv(other.path(), p, r), std::runtime_error);
}

TEST_F(AggregateFiles, PlansAndPredicatesCannotCrossTables) {
  // Parsed for the event table, run on the aggregate file: refused at
  // every layer rather than comparing the wrong column's bytes.
  ColReader r(col_.path());
  const Conjunction event_where = parse_where("campaign_id = 7");  // event schema: index 5, u32
  EXPECT_THROW(Scanner(r, event_where, {column_id(1)}), std::invalid_argument);
  EXPECT_THROW(Scanner(r, {}, {ColumnId::CostMicros}), std::invalid_argument) << "index 9 is outside the table";
  const Conjunction agg_where = parse_where("campaign_id = 7", aggregate_schema());  // index 1, u32
  EXPECT_NO_THROW(Scanner(r, agg_where, {column_id(1)}));
  EXPECT_EQ(agg_where[0].col, column_id(1));
  EXPECT_EQ(to_string(agg_where[0]), "campaign_id = 7");
  EXPECT_THROW(parse_where("event_type = 1", aggregate_schema()), std::invalid_argument);

  QueryPlan p;  // schema defaults to the event table
  p.aggs = parse_aggs("count()");
  EXPECT_THROW(run_aggregate_query(col_.path(), p), std::invalid_argument);
  EXPECT_THROW(HashAggregate({column_id(1)}, parse_aggs("sum(cost_micros)"), aggregate_schema()), std::out_of_range)
      << "sum(cost_micros) parsed for the event table names index 9";
  EXPECT_THROW(HashAggregate({column_id(1)}, parse_aggs("sum(campaign_id)"), aggregate_schema()), std::invalid_argument)
      << "event index 5 (campaign_id, u32) exists in the aggregate table but is cost_micros (i64)";
  EXPECT_NO_THROW(HashAggregate({column_id(1)}, parse_aggs("sum(cost_micros)", aggregate_schema()), aggregate_schema()));
  EXPECT_THROW(KeyPacker({ColumnId::CostMicros}, aggregate_schema()), std::out_of_range);
}

TEST_F(AggregateFiles, ScanWithSelectionVectorOnTheAggregateTable) {
  ColReader r(col_.path());
  const Conjunction where = parse_where("campaign_id = 3 AND cost_micros < 0", aggregate_schema());
  uint64_t rows = 0, clicks = 0;
  run_query(r, where, {column_id(3)}, [&](const Batch& b) {
    rows += b.selected();
    for (uint32_t k = 0; k < b.selected(); ++k) clicks += b.cols[3].at(b.row(k));
    return true;
  });
  // cost = 3000 w - 5000 < 0 for w = 0, 1: two rows, clicks w % 3 = 0 + 1.
  EXPECT_EQ(rows, 2u);
  EXPECT_EQ(clicks, 1u);
}
