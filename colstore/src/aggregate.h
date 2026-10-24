#pragma once
// Hash aggregation for colstore (m4): GROUP BY one or more integer columns
// with COUNT(*), SUM, MIN, MAX and their conditional forms COUNT_IF /
// SUM_IF, consuming the filtered batches that Scanner + apply_filter
// produce.
//
// Pipeline position:
//
//   Scanner -> Batch -> apply_filter -> Batch(sel) -> HashAggregate::consume
//                                                      ... finish() -> ResultTable -> sort/limit -> CSV
//
// How one batch is aggregated (HashAggregate::consume):
//   1. For every selected row, pack the group-by column values into a
//      GroupKey and look it up in the GroupTable, which hands back a dense
//      group index. The indices are kept in a per-batch vector.
//   2. For every aggregate, walk the batch once more and update
//      state[group * n_aggs + agg]. Each aggregate is its own tight loop
//      over one column, instead of one loop that switches on the aggregate
//      kind per row; the switch happens once per (batch, aggregate).
// The state is a flat vector of uint64_t, n_aggs cells per group, so a
// group's cells are adjacent and a new group is a resize.
//
// Value representation inside the state, all uint64_t:
//   count, count_if : the count
//   sum, sum_if     : sum of the raw 64-bit patterns, modulo 2^64. For an
//                     i64 column the patterns are two's complement, so the
//                     result reinterpreted as int64 is the exact signed sum
//                     whenever that sum fits in int64; for an unsigned
//                     column it is exact whenever the sum fits in uint64.
//                     Outside those ranges the result wraps SILENTLY: there
//                     is no overflow check (SQLite raises "integer
//                     overflow" instead), and the --check brute force wraps
//                     the same way, so it cannot see it. No column of either
//                     table gets near that today (10M events sum cost_micros
//                     to about 10^13); a u64 id summed over many rows would.
//                     sum_if adds 0 for a row whose condition is false, i.e.
//                     SQL's SUM(CASE WHEN cond THEN col ELSE 0 END).
//   min, max        : the ORDER KEY of the value (predicate.h), so one
//                     unsigned compare works for signed and unsigned
//                     columns alike; converted back to a value in finish().
//
// Group keys are packed by KeyPacker: each group column takes exactly its
// width in bits (u8: 8, u16: 16, u32: 32, u64/i64: 64) and goes, first fit,
// into whichever of two 64-bit words still has room for all of it, so no
// column straddles a word. For these widths first fit succeeds exactly when
// the widths add up to at most 128 bits, whatever order the columns are
// named in: campaign_id + country + device fit in one word, any two columns
// fit in two, and three 64-bit columns are rejected at plan time with the
// bit count.
//
// GROUP BY with no columns (a global aggregate) skips the hash table: every
// row belongs to group 0, which exists from construction, so the answer has
// exactly one row even when no row matched, as in SQL. That row's count and
// count_if are 0 and its sum, sum_if, min and max are NULL (no value
// exists); ResultTable::is_null says which cells, and the CSV leaves them
// empty, the way SQLite's NULL prints. A GROUP BY with columns has no NULLs:
// every group it emits saw at least one row. Packing instead of hashing the columns
// separately makes "same group" a 128-bit compare and lets the table stay
// key-agnostic.

#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "src/exec.h"
#include "src/format.h"
#include "src/hash_table.h"
#include "src/predicate.h"

namespace colstore {

// ---- key packing --------------------------------------------------------------

class KeyPacker {
 public:
  // Throws std::invalid_argument when the columns do not fit in 128 bits,
  // std::out_of_range when one is not in the schema.
  explicit KeyPacker(std::vector<ColumnId> cols, const Schema& schema = event_schema());

  const std::vector<ColumnId>& columns() const { return cols_; }
  GroupKey pack(const Batch& b, uint32_t row) const;
  // Reverse of pack for a key built from raw 64-bit patterns, one per column.
  GroupKey pack_bits(const std::vector<uint64_t>& bits) const;
  // Raw 64-bit patterns of the columns, in column order.
  void unpack(const GroupKey& k, std::vector<uint64_t>& bits_out) const;

 private:
  struct Field {
    ColumnId col;
    unsigned word;   // 0 = lo, 1 = hi
    unsigned shift;  // bit offset inside the word
    uint64_t mask;   // width bits of ones
  };
  std::vector<ColumnId> cols_;
  std::vector<Field> fields_;
};

// ---- aggregate specs ------------------------------------------------------------

enum class AggKind { Count, Sum, Min, Max, CountIf, SumIf };
const char* agg_kind_name(AggKind k);

struct AggSpec {
  AggKind kind = AggKind::Count;
  std::optional<ColumnId> col;     // Sum, Min, Max, SumIf: the column aggregated
  ColumnType col_type = ColumnType::U64;  // its type and name, copied from the schema the spec was
  std::string col_name;                   //   parsed against (same reason as Predicate); unset without col
  std::optional<Predicate> cond;   // CountIf, SumIf: counted/summed only when true
  std::string name;                // output column name (default or AS alias)

  bool operator==(const AggSpec&) const = default;
};

// Grammar (keywords case-insensitive, whitespace optional):
//
//   aggs := agg ( ',' agg )*
//   agg  := func [ AS name ]
//   func := count() | sum(col) | min(col) | max(col)
//         | count_if(pred) | sum_if(col, pred)
//   pred := one --where term, e.g. event_type=1 or cost_micros BETWEEN 1 AND 9
//
// Default names: count, sum_<col>, min_<col>, max_<col>,
// count_if_<col>_<op>_<value>, sum_if_<col>_<cond>. Column names resolve
// against `schema`. Throws std::invalid_argument naming the offending token.
std::vector<AggSpec> parse_aggs(std::string_view text, const Schema& schema = event_schema());
std::string to_string(const AggSpec& a);

// Type of an aggregate's output: count -> u64; sum -> i64 for a signed
// column else u64; min/max -> the column's type.
ColumnType output_type(const AggSpec& a);

// ---- result table --------------------------------------------------------------

struct OutputColumn {
  std::string name;
  ColumnType type;
  bool operator==(const OutputColumn&) const = default;
};

// Group columns first (their own names and types, from `schema`), then one
// column per aggregate. Throws std::invalid_argument on a duplicate name,
// because ORDER BY resolves names against this list.
std::vector<OutputColumn> output_columns(const std::vector<ColumnId>& group, const std::vector<AggSpec>& aggs,
                                         const Schema& schema = event_schema());

// Columnar result: cols[c][r] is the raw 64-bit pattern of row r in output
// column c, formatted with schema[c].type. `nulls` is empty unless some
// cell is NULL, which only the one row of a global aggregate over zero
// rows has; then nulls[c][r] != 0 marks a NULL cell (its pattern is 0).
struct ResultTable {
  std::vector<OutputColumn> schema;
  std::vector<std::vector<uint64_t>> cols;
  std::vector<std::vector<uint8_t>> nulls;

  size_t rows() const { return cols.empty() ? 0 : cols[0].size(); }
  uint64_t at(size_t col, size_t row) const { return cols[col][row]; }
  bool is_null(size_t col, size_t row) const { return !nulls.empty() && nulls[col][row] != 0; }
};

// Decimal text of a value of the given type (signed types print negative).
std::string format_value(ColumnType type, uint64_t bits);
// The text of one result cell: format_value, or "" for a NULL cell.
std::string format_cell(const ResultTable& t, size_t col, size_t row);
// Whether an aggregate of this kind is NULL over zero input rows (SQL:
// COUNT is 0, SUM / MIN / MAX are NULL).
bool null_when_empty(AggKind k);

// ---- the operator --------------------------------------------------------------

struct AggStats {
  uint64_t rows = 0;       // selected rows consumed
  uint64_t batches = 0;
  size_t groups = 0;
  size_t slots = 0;        // hash table capacity at the end
  size_t grows = 0;        // number of doublings
  uint64_t probes = 0;     // slots visited, so probes / rows is the mean probe length
};

class HashAggregate {
 public:
  // An empty `group` means one global group (plain COUNT / SUM), which
  // exists from construction and is filled without hashing. `schema` is
  // the table the batches come from; group columns are resolved in it.
  HashAggregate(std::vector<ColumnId> group, std::vector<AggSpec> aggs, const Schema& schema = event_schema());

  // Every column the batches must have loaded: group columns, aggregated
  // columns and condition columns. This is the Scanner's project list.
  std::vector<ColumnId> columns_needed() const;

  void consume(const Batch& b);
  // Folds another aggregate over the same plan into this one (thread merge).
  void merge(const HashAggregate& other);
  ResultTable finish() const;

  size_t groups() const { return table_.size(); }
  AggStats stats() const;

 private:
  // Per-aggregate constants hoisted out of the row loops.
  struct Column {
    size_t col_index = 0;      // into Batch::cols
    ColumnType type = ColumnType::U64;
    size_t cond_index = 0;     // into Batch::cols, CountIf / SumIf only
  };

  uint64_t& cell(uint32_t group, size_t agg) { return state_[static_cast<size_t>(group) * aggs_.size() + agg]; }
  uint64_t cell(uint32_t group, size_t agg) const { return state_[static_cast<size_t>(group) * aggs_.size() + agg]; }
  void assign_groups(const Batch& b);           // fills group_of_, grows state_
  void update(size_t a, const Batch& b);        // one aggregate over the batch
  void combine(uint32_t group, size_t a, uint64_t other);  // merge one cell
  static uint64_t initial_state(const AggSpec& a);

  Schema schema_;
  KeyPacker packer_;
  std::vector<AggSpec> aggs_;
  std::vector<Column> agg_cols_;
  GroupTable table_;
  std::vector<uint64_t> state_;      // table_.size() * aggs_.size() cells
  std::vector<uint32_t> group_of_;   // group index per selected row of the current batch
  bool global_ = false;              // no group columns: one group, index 0, no hashing
  uint64_t rows_ = 0;
  uint64_t batches_ = 0;
  uint64_t merged_probes_ = 0;  // probes and doublings of tables merged into this one,
  size_t merged_grows_ = 0;     // so stats() cover every thread's work
};

}  // namespace colstore
