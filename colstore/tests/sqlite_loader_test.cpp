// Tests for the m5 SQLite loader (src/sqlite_loader.h): the schema follows
// the column table, every row and value survives the load (including
// negative costs and u64 values above INT64_MAX), the benchmark indexes and
// statistics exist, a GROUP BY answers the same as a brute force over the
// events, and the CSV writer matches colstore's shape. Compiled only when
// CMake found sqlite3 (see colstore/CMakeLists.txt).

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/event.h"
#include "src/format.h"
#include "src/sqlite_loader.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::sqlite_bench;
using colstore::test::TempFile;
using colstore::test::sample_event;

namespace {

// Writes n sample events to an .ads file and returns them.
std::vector<adstack::Event> write_sample_ads(const std::string& path, uint64_t n) {
  std::vector<adstack::Event> events;
  adstack::EventWriter w(path);
  for (uint64_t i = 0; i < n; ++i) {
    events.push_back(sample_event(i));
    w.write(events.back());
  }
  w.flush();
  return events;
}

struct LoadedDb {
  TempFile ads{"sqlite_loader.ads"};
  TempFile db{"sqlite_loader.sqlite"};
  std::vector<adstack::Event> events;
  LoadReport report;

  explicit LoadedDb(uint64_t n, LoadOptions opt = LoadOptions{}) {
    events = write_sample_ads(ads.path(), n);
    report = load_ads(ads.path(), db.path(), opt);
  }
};

}  // namespace

TEST(SqliteLoader, SchemaFollowsTheColumnTable) {
  const std::string sql = create_table_sql();
  size_t last = 0;
  for (const ColumnSpec& spec : event_schema()) {
    const size_t pos = sql.find(spec.name + " INTEGER NOT NULL", last);
    ASSERT_NE(pos, std::string::npos) << spec.name << " missing from " << sql;
    EXPECT_GT(pos, last) << spec.name << " out of order";  // columns appear in ColumnId order
    last = pos;
  }
  EXPECT_EQ(create_index_sqls().size(), 3u);
}

TEST(SqliteLoader, LoadsEveryRowWithIndexesAndStatistics) {
  LoadedDb loaded(2000);
  EXPECT_EQ(loaded.report.rows, 2000u);
  EXPECT_GT(loaded.report.bytes_table_only, 0u);
  EXPECT_GE(loaded.report.bytes_total, loaded.report.bytes_table_only);  // indexes only add pages

  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  // Brute-force sums over the events, including negative costs.
  int64_t sum_cost = 0, min_cost = INT64_MAX;
  for (const adstack::Event& e : loaded.events) {
    sum_cost += e.cost_micros;
    min_cost = std::min(min_cost, e.cost_micros);
  }
  const auto rows = db.fetch_all("SELECT COUNT(*), SUM(cost_micros), MIN(cost_micros) FROM events");
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(rows[0][0], 2000);
  EXPECT_EQ(rows[0][1], sum_cost);
  EXPECT_EQ(rows[0][2], min_cost);
  EXPECT_LT(min_cost, 0) << "sample events are meant to include negative costs";

  const auto indexes = db.fetch_all("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index'");
  EXPECT_EQ(indexes[0][0], 3);
  // ANALYZE writes its statistics to sqlite_stat1; its presence proves it ran.
  const auto stats = db.fetch_all("SELECT COUNT(*) FROM sqlite_master WHERE name = 'sqlite_stat1'");
  EXPECT_EQ(stats[0][0], 1);
}

TEST(SqliteLoader, NoIndexAndNoAnalyzeOptionsAreHonoured) {
  LoadOptions opt;
  opt.create_indexes = false;
  opt.analyze = false;
  LoadedDb loaded(500, opt);
  EXPECT_EQ(loaded.report.index_seconds, 0.0);
  EXPECT_EQ(loaded.report.analyze_seconds, 0.0);
  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  EXPECT_EQ(db.fetch_all("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index'")[0][0], 0);
  EXPECT_EQ(db.fetch_all("SELECT COUNT(*) FROM sqlite_master WHERE name = 'sqlite_stat1'")[0][0], 0);
}

TEST(SqliteLoader, U64AboveInt64MaxRoundTripsThroughReinterpretation) {
  TempFile ads("sqlite_loader_u64.ads");
  TempFile dbfile("sqlite_loader_u64.sqlite");
  adstack::Event e = sample_event(7);
  e.event_id = UINT64_MAX;
  e.video_id = (1ULL << 63) + 5;
  {
    adstack::EventWriter w(ads.path());
    w.write(e);
    w.flush();
  }
  load_ads(ads.path(), dbfile.path(), LoadOptions{});
  Db db(dbfile.path(), Db::Mode::kReadOnly);
  const auto rows = db.fetch_all("SELECT event_id, video_id FROM events");
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_LT(rows[0][0], 0) << "stored as the signed reinterpretation, as documented";
  EXPECT_EQ(static_cast<uint64_t>(rows[0][0]), UINT64_MAX);
  EXPECT_EQ(static_cast<uint64_t>(rows[0][1]), (1ULL << 63) + 5);
}

TEST(SqliteLoader, GroupByMatchesBruteForce) {
  LoadedDb loaded(3000);
  // Q3's shape: per campaign, counts by type and the cost sum.
  struct Agg { int64_t imp = 0, clk = 0, conv = 0, cost = 0; };
  std::map<uint32_t, Agg> want;
  for (const adstack::Event& e : loaded.events) {
    Agg& a = want[e.campaign_id];
    if (e.event_type == 0) ++a.imp;
    if (e.event_type == 1) ++a.clk;
    if (e.event_type == 2) ++a.conv;
    a.cost += e.cost_micros;
  }
  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  const auto rows = db.fetch_all(
      "SELECT campaign_id, COUNT(*) FILTER (WHERE event_type = 0), COUNT(*) FILTER (WHERE event_type = 1), "
      "COUNT(*) FILTER (WHERE event_type = 2), SUM(cost_micros) FROM events GROUP BY campaign_id ORDER BY campaign_id");
  ASSERT_EQ(rows.size(), want.size());
  size_t i = 0;
  for (const auto& [campaign, a] : want) {
    EXPECT_EQ(rows[i][0], static_cast<int64_t>(campaign));
    EXPECT_EQ(rows[i][1], a.imp);
    EXPECT_EQ(rows[i][2], a.clk);
    EXPECT_EQ(rows[i][3], a.conv);
    EXPECT_EQ(rows[i][4], a.cost);
    ++i;
  }
}

TEST(SqliteLoader, CsvHasHeaderThenRowsInColstoreShape) {
  LoadedDb loaded(1000);
  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  FILE* tmp = std::tmpfile();
  ASSERT_NE(tmp, nullptr);
  const Db::RunReport r = write_query_csv(
      db, "SELECT country, COUNT(*) AS count FROM events GROUP BY country ORDER BY country", tmp);
  EXPECT_EQ(r.rows, 2u);
  ASSERT_EQ(r.columns.size(), 2u);
  EXPECT_EQ(r.columns[0], "country");
  EXPECT_EQ(r.columns[1], "count");
  std::rewind(tmp);
  std::string text;
  char buf[256];
  while (std::fgets(buf, sizeof buf, tmp)) text += buf;
  std::fclose(tmp);
  // sample_event alternates 356 and 840, so 500 each.
  EXPECT_EQ(text, "country,count\n356,500\n840,500\n");
}

TEST(SqliteLoader, CsvPrintsNullAsAnEmptyField) {
  // SUM and MIN over zero rows are NULL in SQL; colstore prints a NULL cell
  // as an empty field, so this side must too or the bench diff would hide
  // the difference between NULL and 0.
  LoadedDb loaded(100);
  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  FILE* tmp = std::tmpfile();
  ASSERT_NE(tmp, nullptr);
  const Db::RunReport r = write_query_csv(
      db, "SELECT COUNT(*) AS count, SUM(cost_micros) AS s, MIN(cost_micros) AS m FROM events WHERE country = 1", tmp);
  EXPECT_EQ(r.rows, 1u);
  std::rewind(tmp);
  std::string text;
  char buf[256];
  while (std::fgets(buf, sizeof buf, tmp)) text += buf;
  std::fclose(tmp);
  EXPECT_EQ(text, "count,s,m\n0,,\n");
}

TEST(SqliteLoader, PlanUsesTheCoveringIndexForThePointCount) {
  LoadedDb loaded(2000);
  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  const auto plan = db.explain_query_plan("SELECT COUNT(*) FROM events WHERE country = 840 AND event_type = 1");
  ASSERT_FALSE(plan.empty());
  bool uses_index = false;
  for (const std::string& line : plan) uses_index = uses_index || line.find("idx_events_country_type") != std::string::npos;
  EXPECT_TRUE(uses_index) << plan[0];
}

TEST(SqliteLoader, ObjectSizesCoverTableAndIndexesWithinTheFile) {
  LoadedDb loaded(2000);
  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  const auto sizes = db.object_sizes();
  if (sizes.empty()) GTEST_SKIP() << "this SQLite has no dbstat virtual table";
  uint64_t total = 0;
  bool table = false, idx_time = false, idx_ct = false, idx_campaign = false;
  for (const auto& [name, bytes] : sizes) {
    total += bytes;
    table = table || name == "events";
    idx_time = idx_time || name == "idx_events_time";
    idx_ct = idx_ct || name == "idx_events_country_type";
    idx_campaign = idx_campaign || name == "idx_events_campaign";
  }
  EXPECT_TRUE(table && idx_time && idx_ct && idx_campaign);
  EXPECT_LE(total, loaded.report.bytes_total);  // dbstat counts pages in use; the file holds all of them
}

TEST(SqliteLoader, RepeatedRunsReturnTheSameRowCountAndATime) {
  LoadedDb loaded(1000);
  Db db(loaded.db.path(), Db::Mode::kReadOnly);
  for (const std::string& p : query_pragmas(QueryOptions{})) db.exec(p);
  const std::string sql = "SELECT campaign_id, SUM(cost_micros) FROM events GROUP BY campaign_id";
  const Db::RunReport first = db.query(sql, nullptr);
  for (int i = 0; i < 3; ++i) {
    const Db::RunReport r = db.query(sql, nullptr);
    EXPECT_EQ(r.rows, first.rows);
    EXPECT_GE(r.seconds, 0.0);
    EXPECT_EQ(r.columns.size(), 2u);
  }
  EXPECT_THROW(db.query("SELECT nope FROM events", nullptr), std::runtime_error);
  EXPECT_THROW(db.exec("INSERT INTO events VALUES (1,2,3,4,5,6,7,8,9,10)"), std::runtime_error)
      << "query_only = 1 and a read-only connection must refuse writes";
}
