// Scanner / apply_filter / gather against a brute-force loop over the same
// events, on both an auto-encoded file (dictionary columns are filtered on
// codes) and a PLAIN-only file (everything is filtered on values), so the
// two paths check each other. Plus zone-map skip counts on a time-sorted
// column versus an unsorted one.

#include <gtest/gtest.h>

#include <numeric>

#include "src/exec.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

constexpr uint64_t kRows = 7'000;
constexpr uint32_t kGroup = 3'000;  // groups of 3000, 3000, 1000 -> batches 1024, 1024, 952, ...

void write_sample(const std::string& path, EncodingPolicy policy, uint64_t rows = kRows, uint32_t group = kGroup) {
  ColWriter w(path, group, policy);
  for (uint64_t i = 0; i < rows; ++i) w.append(sample_event(i));
  w.finish();
}

// Global row indices that pass, plus per-column sums, through the scanner.
struct Result {
  std::vector<uint64_t> rows;
  std::vector<uint64_t> sums;
  QueryStats stats;
};

Result run(const ColReader& r, const Conjunction& where, const std::vector<ColumnId>& cols,
           uint32_t batch_rows = kVectorSize) {
  Result out;
  out.sums.assign(cols.size(), 0);
  std::vector<uint64_t> dense;
  out.stats = run_query(r, where, cols, [&](const Batch& b) {
    EXPECT_LE(b.n, batch_rows);
    EXPECT_LE(b.first_row + b.n, r.footer().row_groups[b.row_group].rows) << "batch crosses a row group";
    uint64_t base = 0;
    for (size_t g = 0; g < b.row_group; ++g) base += r.footer().row_groups[g].rows;
    uint32_t prev = 0;
    for (uint32_t k = 0; k < b.selected(); ++k) {
      const uint32_t i = b.row(k);
      EXPECT_LT(i, b.n);
      if (k > 0) {
        EXPECT_GT(i, prev) << "selection vector must be strictly ascending";
      }
      prev = i;
      out.rows.push_back(base + b.first_row + i);
    }
    for (size_t c = 0; c < cols.size(); ++c) {
      gather(b, cols[c], dense);
      EXPECT_EQ(dense.size(), b.selected());
      for (uint32_t k = 0; k < b.selected(); ++k) {
        out.sums[c] += dense[k];
        EXPECT_EQ(dense[k], b.cols[static_cast<size_t>(cols[c])].at(b.row(k)));
      }
    }
    return true;
  }, batch_rows);
  return out;
}

Result brute(const Conjunction& where, const std::vector<ColumnId>& cols, uint64_t rows = kRows) {
  Result out;
  out.sums.assign(cols.size(), 0);
  for (uint64_t i = 0; i < rows; ++i) {
    const adstack::Event e = sample_event(i);
    if (!matches_all(where, e)) continue;
    out.rows.push_back(i);
    for (size_t c = 0; c < cols.size(); ++c) out.sums[c] += event_field_bits(e, cols[c]);
  }
  return out;
}

Predicate p(ColumnId col, Op op, int64_t v, int64_t hi = 0) { return make_predicate(col, op, widen(v), widen(hi)); }

int64_t t(uint64_t i) { return sample_event(i).event_time_us; }

class ExecFiles : public ::testing::Test {
 protected:
  ExecFiles() : auto_file("exec_auto.col"), plain_file("exec_plain.col") {
    write_sample(auto_file.path(), EncodingPolicy::Auto);
    write_sample(plain_file.path(), EncodingPolicy::PlainOnly);
  }
  TempFile auto_file, plain_file;
};

}  // namespace

TEST_F(ExecFiles, BatchesTileRowGroupsWithoutFilter) {
  ColReader r(auto_file.path());
  Scanner s(r, {}, {ColumnId::EventId});
  Batch b;
  std::vector<uint32_t> sizes;
  uint64_t row = 0;
  while (s.next(b)) {
    apply_filter(b);
    EXPECT_FALSE(b.filtered) << "no predicates: nothing to filter";
    EXPECT_EQ(b.selected(), b.n);
    for (uint32_t i = 0; i < b.n; ++i, ++row) EXPECT_EQ(b.cols[0].at(i), sample_event(row).event_id);
    sizes.push_back(b.n);
  }
  EXPECT_EQ(row, kRows);
  EXPECT_EQ(sizes, (std::vector<uint32_t>{1024, 1024, 952, 1024, 1024, 952, 1000}));
  EXPECT_EQ(s.stats().row_groups_scanned, 3u);
  EXPECT_EQ(s.stats().chunks_decoded, 3u) << "only the projected column is decoded";
}

TEST_F(ExecFiles, SelectionVectorMatchesBruteForceOnBothEncodings) {
  const std::vector<ColumnId> cols = {ColumnId::CampaignId, ColumnId::CostMicros, ColumnId::Country};
  const std::vector<std::pair<const char*, Conjunction>> cases = {
      {"dict equality", {p(ColumnId::Country, Op::Eq, 840)}},
      {"dict equality, value absent", {p(ColumnId::Country, Op::Eq, 500)}},
      {"dict range", {p(ColumnId::Device, Op::Ge, 1)}},
      {"dict between", {p(ColumnId::EventType, Op::Between, 1, 2)}},
      {"signed negative", {p(ColumnId::CostMicros, Op::Lt, 0)}},
      {"value range", {p(ColumnId::CampaignId, Op::Between, 1000, 2000)}},
      {"time range inside one group", {p(ColumnId::EventTimeUs, Op::Between, t(3100), t(3300))}},
      {"everything", {p(ColumnId::EventId, Op::Ge, 1)}},
      {"nothing", {p(ColumnId::EventId, Op::Lt, 1)}},
      {"and of four",
       {p(ColumnId::Country, Op::Eq, 840), p(ColumnId::Device, Op::Le, 1), p(ColumnId::CostMicros, Op::Gt, -500),
        p(ColumnId::CampaignId, Op::Lt, 50000)}},
  };
  ColReader auto_r(auto_file.path()), plain_r(plain_file.path());
  for (const auto& [name, where] : cases) {
    const Result want = brute(where, cols);
    const Result a = run(auto_r, where, cols);
    const Result q = run(plain_r, where, cols);
    EXPECT_EQ(a.rows, want.rows) << name << " (auto)";
    EXPECT_EQ(q.rows, want.rows) << name << " (plain)";
    EXPECT_EQ(a.sums, want.sums) << name << " (auto)";
    EXPECT_EQ(q.sums, want.sums) << name << " (plain)";
    EXPECT_EQ(a.stats.rows_passed, want.rows.size()) << name;
    EXPECT_EQ(q.stats.scan.predicates_on_codes, 0u) << name << ": PLAIN file has no dictionaries";
  }
}

TEST_F(ExecFiles, DictionaryPredicatesRunOnCodes) {
  ColReader r(auto_file.path());
  ASSERT_EQ(r.chunk(0, ColumnId::Country).encoding, static_cast<uint32_t>(Encoding::Dict));
  const Result a = run(r, {p(ColumnId::Country, Op::Eq, 840)}, {ColumnId::Country});
  EXPECT_EQ(a.stats.scan.predicates_on_codes, 3u) << "one per row group";
  EXPECT_EQ(a.stats.scan.predicates_on_values, 0u);
  EXPECT_EQ(a.stats.rows_passed, kRows / 2);

  // A constant absent from every chunk's dictionary: the zone map cannot
  // tell (356 < 500 < 840) but the code mapping can, before any projected
  // column is read.
  const Result none = run(r, {p(ColumnId::Country, Op::Eq, 500)}, {ColumnId::CostMicros});
  EXPECT_EQ(none.stats.rows_passed, 0u);
  EXPECT_EQ(none.stats.scan.row_groups_skipped_zone, 0u);
  EXPECT_EQ(none.stats.scan.row_groups_skipped_dict, 3u);
  EXPECT_EQ(none.stats.scan.chunks_decoded, 3u) << "country only; cost_micros never decoded";
}

TEST_F(ExecFiles, BatchSizeDoesNotChangeTheAnswer) {
  ColReader r(auto_file.path());
  const Conjunction where = {p(ColumnId::Country, Op::Eq, 840), p(ColumnId::CampaignId, Op::Lt, 30000)};
  const std::vector<ColumnId> cols = {ColumnId::CostMicros};
  const Result want = brute(where, cols);
  for (uint32_t batch : {1u, 7u, 1023u, 1024u, 1025u, 100'000u}) {
    const Result got = run(r, where, cols, batch);
    EXPECT_EQ(got.rows, want.rows) << "batch " << batch;
    EXPECT_EQ(got.sums, want.sums) << "batch " << batch;
  }
  EXPECT_THROW(Scanner(r, where, cols, 0), std::invalid_argument);
}

TEST_F(ExecFiles, GatherNeedsAProjectedColumn) {
  ColReader r(auto_file.path());
  Scanner s(r, {}, {ColumnId::EventId});
  Batch b;
  ASSERT_TRUE(s.next(b));
  std::vector<uint64_t> out;
  EXPECT_NO_THROW(gather(b, ColumnId::EventId, out));
  EXPECT_THROW(gather(b, ColumnId::CostMicros, out), std::invalid_argument);
}

TEST(Exec, EmptyFileYieldsNoBatches) {
  TempFile f("exec_empty.col");
  write_sample(f.path(), EncodingPolicy::Auto, 0);
  ColReader r(f.path());
  Scanner s(r, {p(ColumnId::Country, Op::Eq, 840)}, {ColumnId::EventId});
  Batch b;
  EXPECT_FALSE(s.next(b));
  EXPECT_EQ(s.stats().row_groups_total, 0u);
}

// ---- zone maps on real row groups ------------------------------------------------

TEST(ZoneMapScan, SortedColumnSkipsEveryOtherGroup) {
  // 10 groups of 1000 rows; event_time_us is strictly increasing, so the
  // groups' [min, max] are disjoint and a range inside group 3 touches
  // exactly one group.
  TempFile f("exec_zone.col");
  write_sample(f.path(), EncodingPolicy::Auto, 10'000, 1'000);
  ColReader r(f.path());

  const Conjunction in_group_3 = {p(ColumnId::EventTimeUs, Op::Between, t(3'500), t(3'600))};
  const Result a = run(r, in_group_3, {ColumnId::CostMicros});
  EXPECT_EQ(a.rows, brute(in_group_3, {ColumnId::CostMicros}, 10'000).rows);
  EXPECT_EQ(a.stats.rows_passed, 101u);
  EXPECT_EQ(a.stats.scan.row_groups_skipped_zone, 9u);
  EXPECT_EQ(a.stats.scan.row_groups_scanned, 1u);
  EXPECT_EQ(a.stats.scan.rows_scanned, 1'000u);
  EXPECT_EQ(a.stats.scan.chunks_decoded, 2u) << "event_time_us + cost_micros of one group";

  // A range spanning the boundary of groups 2 and 3 scans exactly two.
  const Conjunction across = {p(ColumnId::EventTimeUs, Op::Between, t(2'999), t(3'000))};
  const Result b = run(r, across, {ColumnId::CostMicros});
  EXPECT_EQ(b.stats.rows_passed, 2u);
  EXPECT_EQ(b.stats.scan.row_groups_scanned, 2u);
  EXPECT_EQ(b.stats.scan.row_groups_skipped_zone, 8u);

  // Boundary values: the group's own min and max must not be skipped.
  EXPECT_EQ(run(r, {p(ColumnId::EventTimeUs, Op::Eq, t(3'000))}, {ColumnId::CostMicros}).stats.rows_passed, 1u);
  EXPECT_EQ(run(r, {p(ColumnId::EventTimeUs, Op::Eq, t(3'999))}, {ColumnId::CostMicros}).stats.rows_passed, 1u);
  EXPECT_EQ(run(r, {p(ColumnId::EventTimeUs, Op::Lt, t(0))}, {ColumnId::CostMicros}).stats.scan.row_groups_skipped_zone,
            10u);
}

// sample_event() is monotone in every column at this row count, so an
// unsorted column has to be built here: campaign_id = i * 2654435761 mod
// 2^32 wraps around inside every group of 1000.
adstack::Event unsorted_event(uint64_t i) {
  adstack::Event e = sample_event(i);
  e.campaign_id = static_cast<uint32_t>(i * 2654435761ULL);
  return e;
}

TEST(ZoneMapScan, UnsortedColumnSkipsNothing) {
  // Every group's [min, max] on campaign_id spans nearly all of u32, so the
  // zone map is consulted (it costs nothing) but can never skip: a narrow
  // range that matches one row still decodes all ten groups. This is the
  // case for sorting or clustering data on the column you filter by.
  TempFile f("exec_zone_unsorted.col");
  {
    ColWriter w(f.path(), 1'000);
    for (uint64_t i = 0; i < 10'000; ++i) w.append(unsorted_event(i));
    w.finish();
  }
  ColReader r(f.path());
  const uint32_t v = unsorted_event(4'321).campaign_id;
  const Conjunction narrow = {make_predicate(ColumnId::CampaignId, Op::Between, v, v + 1'000)};
  const Result a = run(r, narrow, {ColumnId::CostMicros});

  std::vector<uint64_t> want_rows;
  for (uint64_t i = 0; i < 10'000; ++i) {
    if (matches_all(narrow, unsorted_event(i))) want_rows.push_back(i);
  }
  EXPECT_EQ(a.rows, want_rows);
  EXPECT_GE(a.stats.rows_passed, 1u);
  EXPECT_EQ(a.stats.scan.row_groups_skipped_zone, 0u);
  EXPECT_EQ(a.stats.scan.row_groups_scanned, 10u);
  EXPECT_EQ(a.stats.scan.rows_scanned, 10'000u);
}

TEST(ZoneMapScan, AlwaysTruePredicateIsDroppedAndItsColumnNotDecoded) {
  TempFile f("exec_zone_all.col");
  write_sample(f.path(), EncodingPolicy::Auto, 10'000, 1'000);
  ColReader r(f.path());
  // event_id = i + 1 >= 1 everywhere: every group's min satisfies it.
  const Result a = run(r, {p(ColumnId::EventId, Op::Ge, 1)}, {ColumnId::CostMicros});
  EXPECT_EQ(a.stats.rows_passed, 10'000u);
  EXPECT_EQ(a.stats.scan.predicates_dropped, 10u);
  EXPECT_EQ(a.stats.scan.predicates_on_values + a.stats.scan.predicates_on_codes, 0u);
  EXPECT_EQ(a.stats.scan.chunks_decoded, 10u) << "cost_micros only; event_id never read";
}
