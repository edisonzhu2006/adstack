#pragma once
// Query execution for colstore (m3): scan, filter, project over batches of
// up to kVectorSize rows, with zone-map row-group skipping. m4 adds the
// row-group stride so several Scanners (one per thread, each with its own
// ColReader) can split one file.
//
// Shape of a query:
//
//   Scanner  -> Batch -> apply_filter() -> Batch (with selection vector) -> gather()
//
// Scanner walks the row groups. For each one it first asks the zone maps
// (the per-chunk min/max in the footer) whether the predicates can match
// at all; if not, the group is skipped without a single byte read. For a
// group that survives, only the columns the query touches (filter columns
// plus projected columns) are read and decoded, once, for the whole group;
// the scanner then hands out that group as consecutive Batches of 1024
// rows whose column vectors are pointers into the decoded storage.
//
// apply_filter() evaluates the predicates on a batch and produces a
// SELECTION VECTOR: the indices of passing rows. Nothing is copied; the
// batch's column vectors are untouched and the consumer reads
// cols[c].at(sel[k]). gather() is the "project" step for consumers that
// want a dense column of the survivors.
//
// Why batches ("vectors") of 1024 and not one row at a time: per-row
// execution pays a virtual call / switch per row per operator and keeps no
// value in a register across rows; per-batch execution amortizes the
// dispatch over 1024 rows and runs tight loops the compiler can unroll and
// vectorize. 1024 * 8 bytes = 8 KiB per column, so a handful of columns
// stay in L1 while the loop runs. Why not whole row groups at a time: 64K
// rows * 8 bytes * several columns leaves L1/L2 and the selection vector
// grows accordingly. These claims are measured in NOTES.md.
//
// Why a selection vector and not a bitmap or a compacted copy: the copy is
// the expensive thing we are trying to avoid (selectivity in ads queries is
// usually low, so most of a copy would be wasted); a bitmap needs a popcount
// / scan to iterate survivors and branches on every bit. A selection vector
// is written branch-free (sel[k] = i; k += pass) and iterated with a
// straight loop.
//
// Dictionary columns are filtered on CODES: the chunk's dictionary is
// sorted, so a value range maps to a code range with two binary searches,
// done once per row group, and the per-row test becomes a compare of small
// integers with no table lookup. The constant is mapped per row group
// because every chunk carries its own dictionary.

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "src/predicate.h"
#include "src/reader.h"

namespace colstore {

constexpr uint32_t kVectorSize = 1024;

// One column's view of a batch: either raw 64-bit value patterns, or
// dictionary codes plus the dictionary they index. Pointers borrow from
// the Scanner's storage for the current row group and are valid until the
// next Scanner::next() that moves to a new row group.
struct ColumnVector {
  const uint64_t* values = nullptr;  // n patterns, or null when codes is set
  const uint32_t* codes = nullptr;   // n codes into dict
  const uint64_t* dict = nullptr;    // the chunk's sorted dictionary

  bool loaded() const { return values != nullptr || codes != nullptr; }
  uint64_t at(uint32_t i) const { return values ? values[i] : dict[codes[i]]; }
};

// A predicate specialised to one row group: a closed [lo, hi] range over
// either order keys (values ^ flip) or dictionary codes.
struct CompiledPredicate {
  ColumnId col = ColumnId::EventId;
  bool on_codes = false;
  uint64_t flip = 0;  // 1<<63 for i64 columns, 0 otherwise (see order_key)
  uint64_t lo = 0;
  uint64_t hi = 0;
};

struct Batch {
  size_t row_group = 0;
  uint32_t first_row = 0;  // row index inside the row group
  uint32_t n = 0;          // rows in this batch, <= kVectorSize
  std::vector<ColumnVector> cols;  // one per schema column (the Scanner sizes it); unloaded unless needed
  const std::vector<CompiledPredicate>* predicates = nullptr;  // this row group's, for apply_filter

  // After apply_filter(): sel holds the indices (0..n-1) of passing rows in
  // ascending order. Before it, every row passes and sel is unused.
  bool filtered = false;
  std::vector<uint32_t> sel;

  uint32_t selected() const { return filtered ? static_cast<uint32_t>(sel.size()) : n; }
  uint32_t row(uint32_t k) const { return filtered ? sel[k] : k; }
};

struct ScanStats {
  size_t row_groups_total = 0;
  size_t row_groups_skipped_zone = 0;  // zone map said no row can match
  size_t row_groups_skipped_dict = 0;  // a constant was absent from a chunk's dictionary
  size_t row_groups_scanned = 0;
  uint64_t rows_scanned = 0;           // rows handed out in batches
  size_t chunks_decoded = 0;
  uint64_t bytes_read = 0;
  size_t predicates_dropped = 0;       // (group, predicate) pairs the zone map proved always true
  size_t predicates_on_codes = 0;      // (group, predicate) pairs evaluated on dictionary codes
  size_t predicates_on_values = 0;
};

class Scanner {
 public:
  // `where` is ANDed; `project` lists the columns the consumer will read.
  // Columns in `where` but not in `project` are decoded for filtering only.
  // Both are checked against the file's schema (reader.schema()): a
  // predicate built for another table's column throws std::invalid_argument.
  // `batch_rows` exists so the batch-size tradeoff can be measured
  // (`scan --vector N`); 1 is row-at-a-time through the same operators.
  // `first_group` / `group_stride` restrict the scan to row groups
  // first_group, first_group + stride, ...: thread t of T scans (t, T), so
  // a time-range query whose matching groups are contiguous still spreads
  // over every thread (m4).
  Scanner(const ColReader& reader, Conjunction where, std::vector<ColumnId> project,
          uint32_t batch_rows = kVectorSize, size_t first_group = 0, size_t group_stride = 1);

  // Fills `out` with the next batch; false at end of file. Batches never
  // span row groups, so the last batch of a group may be short.
  bool next(Batch& out);
  const ScanStats& stats() const { return stats_; }

 private:
  // Decoded chunk of one column for the current row group.
  struct DecodedColumn {
    std::vector<uint64_t> values;
    std::vector<uint32_t> codes;
    std::vector<uint64_t> dict;
    bool on_codes = false;
    bool loaded = false;
    void clear() { values.clear(); codes.clear(); dict.clear(); on_codes = false; loaded = false; }
  };

  bool load_row_group(size_t g);  // false when the group is skipped
  void decode_column(size_t g, ColumnId col);
  bool compile_predicate(const Predicate& p, CompiledPredicate& out);  // false: no code matches
  ColumnVector slice(ColumnId col, uint32_t first) const;

  const ColReader& reader_;
  Conjunction where_;
  std::vector<uint8_t> projected_;    // per schema column (uint8_t: vector<bool> has no data())
  std::vector<DecodedColumn> cols_;   // per schema column
  std::vector<CompiledPredicate> compiled_;  // predicates still needed for the current group
  ScanStats stats_;

  uint32_t batch_rows_ = kVectorSize;
  size_t next_group_ = 0;
  size_t group_stride_ = 1;
  bool group_loaded_ = false;
  uint32_t group_rows_ = 0;
  uint32_t next_row_ = 0;
};

// Evaluates batch.predicates and fills batch.sel. With no predicates the
// batch is left unfiltered (every row passes) and nothing is written.
void apply_filter(Batch& batch);

// Project: the values of `col` for the selected rows, densely packed.
void gather(const Batch& batch, ColumnId col, std::vector<uint64_t>& out);

// ---- the whole pipeline ------------------------------------------------------

struct QueryStats {
  ScanStats scan;
  uint64_t rows_passed = 0;
  double seconds = 0;
};

// Scan -> filter -> on_batch for every batch with at least one passing row.
// on_batch(const Batch&) returns false to stop early (LIMIT).
template <typename OnBatch>
QueryStats run_query(const ColReader& reader, const Conjunction& where, const std::vector<ColumnId>& project,
                     OnBatch&& on_batch, uint32_t batch_rows = kVectorSize, size_t first_group = 0,
                     size_t group_stride = 1) {
  const auto t0 = std::chrono::steady_clock::now();
  Scanner scan(reader, where, project, batch_rows, first_group, group_stride);
  QueryStats q;
  Batch b;
  while (scan.next(b)) {
    apply_filter(b);
    if (b.selected() == 0) continue;
    q.rows_passed += b.selected();
    if (!on_batch(static_cast<const Batch&>(b))) break;
  }
  q.scan = scan.stats();
  q.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  return q;
}

// One-line summary for --stats output.
std::string describe(const QueryStats& q);

}  // namespace colstore
