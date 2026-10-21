// Row-group boundaries: 0 rows, exactly one full group, one full group plus
// one row, and a short custom group size. Every case reads back identically.

#include <gtest/gtest.h>

#include "src/reader.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

// Writes n sample events with the given group size, reopens, and checks
// group shapes plus a full field-by-field read-back.
void round_trip(const char* name, uint64_t n, uint32_t rows_per_group,
                const std::vector<uint32_t>& expected_group_rows) {
  TempFile tmp(name);
  {
    ColWriter w(tmp.path(), rows_per_group);
    for (uint64_t i = 0; i < n; ++i) w.append(sample_event(i));
    EXPECT_EQ(w.rows_written(), n);
    w.finish();
  }
  ColReader r(tmp.path());
  ASSERT_EQ(r.num_row_groups(), expected_group_rows.size());
  EXPECT_EQ(r.total_rows(), n);
  EXPECT_EQ(r.footer().rows_per_group, rows_per_group);

  uint64_t row = 0;
  for (size_t g = 0; g < r.num_row_groups(); ++g) {
    EXPECT_EQ(r.footer().row_groups[g].rows, expected_group_rows[g]);
    const std::vector<adstack::Event> events = r.read_events(g);
    ASSERT_EQ(events.size(), expected_group_rows[g]);
    for (const adstack::Event& e : events) {
      ASSERT_EQ(e, sample_event(row)) << "row " << row;
      ++row;
    }
  }
  EXPECT_EQ(row, n);
}

}  // namespace

TEST(RowGroup, ZeroRows) { round_trip("rg0.col", 0, kDefaultRowsPerGroup, {}); }

TEST(RowGroup, ExactlyOneFullGroup) {
  round_trip("rg65536.col", 65'536, kDefaultRowsPerGroup, {65'536});
}

TEST(RowGroup, OneFullGroupPlusOne) {
  round_trip("rg65537.col", 65'537, kDefaultRowsPerGroup, {65'536, 1});
}

TEST(RowGroup, CustomGroupSize) {
  round_trip("rg1000.col", 2'500, 1'000, {1'000, 1'000, 500});
}

TEST(RowGroup, RejectsZeroGroupSize) {
  TempFile tmp("rgzero.col");
  EXPECT_THROW(ColWriter w(tmp.path(), 0), std::invalid_argument);
}
