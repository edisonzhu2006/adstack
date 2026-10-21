#pragma once
// The aggregate query driver (m4): plan -> scan/filter/aggregate -> sort/limit
// -> CSV, plus the brute-force check that proves a result against the
// source .ads file.
//
//   colstore q --in events.col --where ... --group ... --agg ... --order ... --limit N
//
// Execution:
//   1. One HashAggregate per thread. Thread t opens its own ColReader (a
//      ColReader is a FILE* with fseek+fread, not shareable) and scans row
//      groups t, t+T, t+2T, ... through the same Scanner / apply_filter path
//      the single-threaded query uses. Nothing is shared between threads
//      while they run; each thread's table is merged into thread 0's after
//      join. This is the simplest parallel hash aggregation: no locks, no
//      concurrent table, and the merge cost is one pass over each thread's
//      groups (100K keys, a millisecond), paid once.
//   2. finish() turns the table into a ResultTable (one row per group).
//   3. If there is an ORDER BY or a LIMIT, sort_limit() produces the output
//      permutation; the group columns are appended to the ORDER BY as a
//      tiebreak so the output is identical at any thread count. Without
//      either, rows come out in the order the groups were first seen.
//
// Verification (--check events.ads): the same plan is evaluated by a loop
// over every event of the .ads file with a std::map keyed by the group
// values, nothing shared with the hash table or the scanner, and the two
// result sets are compared group by group; with an ORDER BY / LIMIT the
// emitted rows are also compared with the brute-force rows under a plain
// std::sort.

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "src/aggregate.h"
#include "src/exec.h"
#include "src/predicate.h"
#include "src/sort.h"

namespace colstore {

struct QueryPlan {
  Conjunction where;
  std::vector<ColumnId> group;     // empty: one global group
  std::vector<AggSpec> aggs;
  std::vector<OrderSpec> order;    // resolved against output_columns(group, aggs)
  size_t limit = 0;                // 0 = no limit
  unsigned threads = 1;
  uint32_t batch_rows = kVectorSize;
};

struct QueryResult {
  ResultTable table;              // one row per group, engine order
  std::vector<uint32_t> rows;     // output order (indices into table), after ORDER BY / LIMIT
  QueryStats scan;                // summed over threads
  AggStats agg;                   // of the merged aggregate
  double scan_agg_seconds = 0;    // wall time of scan + filter + aggregate, all threads joined
  double merge_seconds = 0;       // folding the other threads' tables into thread 0's (0 at 1 thread)
  double finish_seconds = 0;      // table -> ResultTable
  double sort_seconds = 0;
  double total_seconds = 0;
};

// Throws std::invalid_argument on a bad plan, std::runtime_error on I/O.
QueryResult run_aggregate_query(const std::string& col_path, const QueryPlan& plan);

// The ORDER BY actually applied: plan.order plus every group column not
// already in it, ascending. Empty when the plan has neither order nor limit.
std::vector<OrderSpec> effective_order(const QueryPlan& plan, const std::vector<OutputColumn>& schema);

// Header line, then one line per row of r.rows.
void write_csv(FILE* out, const QueryResult& r);

// Multi-line summary for --stats.
std::string describe(const QueryResult& r);

struct CheckReport {
  bool ok = false;
  std::string detail;   // what was compared, and the first difference if any
  double seconds = 0;   // brute-force time
};
CheckReport check_against_ads(const std::string& ads_path, const QueryPlan& plan, const QueryResult& r);

}  // namespace colstore
