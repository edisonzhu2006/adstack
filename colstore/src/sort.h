#pragma once
// ORDER BY ... LIMIT N over a ResultTable (m4).
//
// Sorting happens AFTER aggregation, on the result rows (one per group),
// never on the scanned events: with 10M events and 100K groups the sort
// input is 100x smaller than the scan input, and no query here needs
// event-level order.
//
// The operator produces a permutation (row indices in output order) rather
// than moving rows: the table is columnar, so swapping a row means swapping
// one cell per column, and the CSV writer can just follow the index list.
//
// LIMIT N with N smaller than the row count uses std::partial_sort, which
// is a heap of N elements: the first N rows are heapified, then each of the
// remaining rows is compared against the heap's top (the worst of the
// current best N) and only rows that beat it do a log N sift. That is
// O(rows * log N) worst case and close to O(rows) when the input is in
// random order, against O(rows * log rows) for a full sort; for a top-20
// over 100K groups the full sort does roughly 17 compares per row, the
// heap about one. The sorted tail that a full sort would produce is never
// needed because the LIMIT throws it away. Without a LIMIT (or with one
// larger than the row count) it is a plain std::sort. std::nth_element plus
// a sort of the first N is the other option (O(rows) average) but it
// reorders the whole index array for a gain that is microseconds at this
// size; the heap is the simpler story.
//
// Ties: rows that compare equal on every ORDER BY column are ordered by
// row index, so the output is a deterministic function of the table. The
// query layer additionally appends the group columns to the ORDER BY so the
// result does not depend on which thread saw a group first.

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "src/aggregate.h"

namespace colstore {

struct OrderSpec {
  size_t column = 0;  // index into ResultTable::schema
  bool desc = false;
  bool operator==(const OrderSpec&) const = default;
};

// "sum_cost_micros desc, campaign_id [asc]" resolved against the schema.
// Names are output column names (group columns or aggregate names / AS
// aliases). Throws std::invalid_argument on an unknown name or direction.
std::vector<OrderSpec> parse_order(std::string_view text, const std::vector<OutputColumn>& schema);
std::string to_string(const OrderSpec& o, const std::vector<OutputColumn>& schema);

// Row indices of `table` in ORDER BY order, truncated to `limit` rows when
// limit > 0. Values are compared in their column's natural order (signed
// columns as signed). With an empty `order` the rows are in index order,
// just truncated.
std::vector<uint32_t> sort_limit(const ResultTable& table, const std::vector<OrderSpec>& order, size_t limit);

}  // namespace colstore
