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
//   sum, sum_if     : wrapping sum of the raw 64-bit patterns. For an i64
//                     column the patterns are two's complement, so the
//                     wrapped sum reinterpreted as int64 is the exact signed
//                     sum (as long as it fits in 64 bits); unsigned columns
//                     are zero-extended, so the sum is exact.
//   min, max        : the ORDER KEY of the value (predicate.h), so one
//                     unsigned compare works for signed and unsigned
//                     columns alike; converted back to a value in finish().
//
// Group keys are packed by KeyPacker: each group column takes exactly its
// width in bits (u8: 8, u16: 16, u32: 32, u64/i64: 64) and the columns are
// laid out back to back in two 64-bit words, each column kept inside one
// word (a column that does not fit in the rest of the current word starts
// the next one). So campaign_id + country + device fit in one word and any
// two columns fit in two; three wide columns do not and are rejected at
// plan time with a clear message. Packing instead of hashing the columns
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
  // Throws std::invalid_argument when the columns do not fit in 128 bits.
  explicit KeyPacker(std::vector<ColumnId> cols);

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
// count_if_<col>_<op>_<value>, sum_if_<col>_<cond>. Throws
// std::invalid_argument naming the offending token.
std::vector<AggSpec> parse_aggs(std::string_view text);
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

// Group columns first (their own names and types), then one column per
// aggregate. Throws std::invalid_argument on a duplicate name, because
// ORDER BY resolves names against this list.
std::vector<OutputColumn> output_columns(const std::vector<ColumnId>& group, const std::vector<AggSpec>& aggs);

// Columnar result: cols[c][r] is the raw 64-bit pattern of row r in output
// column c, formatted with schema[c].type.
struct ResultTable {
  std::vector<OutputColumn> schema;
  std::vector<std::vector<uint64_t>> cols;

  size_t rows() const { return cols.empty() ? 0 : cols[0].size(); }
  uint64_t at(size_t col, size_t row) const { return cols[col][row]; }
};

// Decimal text of a value of the given type (signed types print negative).
std::string format_value(ColumnType type, uint64_t bits);

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
  // An empty `group` means one global group (plain COUNT / SUM).
  HashAggregate(std::vector<ColumnId> group, std::vector<AggSpec> aggs);

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

  KeyPacker packer_;
  std::vector<AggSpec> aggs_;
  std::vector<Column> agg_cols_;
  GroupTable table_;
  std::vector<uint64_t> state_;      // table_.size() * aggs_.size() cells
  std::vector<uint32_t> group_of_;   // group index per selected row of the current batch
  uint64_t rows_ = 0;
  uint64_t batches_ = 0;
  uint64_t merged_probes_ = 0;  // probes and doublings of tables merged into this one,
  size_t merged_grows_ = 0;     // so stats() cover every thread's work
};

}  // namespace colstore
