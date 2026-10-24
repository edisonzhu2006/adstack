#pragma once
// SQLite side of the m5 benchmark: load an .ads event file into a SQLite
// database with the benchmark's schema, indexes and PRAGMAs, and run timed
// SQL against it. This exists so the "colstore vs SQLite" comparison in
// colstore/BENCHMARKS.md is produced by one program with the settings
// written down in code, not by hand in the sqlite3 shell.
//
// It is the ONLY part of colstore that touches a third-party library, so it
// is a separate static library (colstore_sqlite_loader) and a separate
// binary (colstore_sqlite); colstore_core and the colstore CLI build without
// sqlite3, and CMake skips these targets when the library is not found.
//
// Schema: one rowid table `events` with the ten columns of adstack::Event,
// in declaration order, all INTEGER NOT NULL. SQLite integers are signed
// 64-bit, so the two u64 columns (event_id, video_id) are stored as the
// same 64 bits reinterpreted as int64: a value above INT64_MAX comes back
// negative from SQL but round-trips exactly through static_cast<uint64_t>.
// The benchmark queries never sum or compare those two columns, so the
// reinterpretation changes no answer. The alternative, storing them as
// 8-byte BLOBs, would make SQLite's indexes and comparisons on them
// bytewise and slower, which would be unfair to SQLite.

#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

struct sqlite3;       // from <sqlite3.h>; kept out of this header so that
struct sqlite3_stmt;  // only sqlite_loader.cpp depends on it

namespace colstore::sqlite_bench {

// ---- settings, as data so the CLI can print exactly what was used --------

struct LoadOptions {
  bool create_indexes = true;      // (event_time_us), (country, event_type), (campaign_id)
  bool analyze = true;             // ANALYZE after the indexes, so the planner has statistics
  int64_t cache_mib = 1024;        // PRAGMA cache_size = -(cache_mib * 1024)
};

struct QueryOptions {
  int64_t cache_mib = 1024;        // SQLite's own page cache (on top of the OS page cache)
  int64_t mmap_mib = 1024;         // PRAGMA mmap_size; 0 disables. macOS caps it at 1 GiB
                                   // (SQLITE_MAX_MMAP_SIZE); whatever is left is read()
};

// The PRAGMAs applied before loading / before querying, in order.
std::vector<std::string> load_pragmas(const LoadOptions& opt);
std::vector<std::string> query_pragmas(const QueryOptions& opt);

// CREATE TABLE text derived from colstore's event schema (event_schema()), so
// a schema change in common/event.h cannot leave this file behind silently.
std::string create_table_sql();
// The three CREATE INDEX statements of the benchmark.
std::vector<std::string> create_index_sqls();

// ---- a small RAII connection ---------------------------------------------

class Db {
 public:
  enum class Mode { kCreate, kReadOnly };
  Db(const std::string& path, Mode mode);   // throws std::runtime_error on failure
  ~Db();
  Db(const Db&) = delete;
  Db& operator=(const Db&) = delete;

  // Statements that return no rows (PRAGMA, CREATE, BEGIN, ...). Throws.
  void exec(const std::string& sql);

  struct RunReport {
    std::vector<std::string> columns;  // result column names, in order
    uint64_t rows = 0;
    double seconds = 0;                // prepare + step to SQLITE_DONE + finalize
  };
  // Runs one SELECT. on_row (optional) sees every row's integer values; NULL
  // reads as 0. The time includes the callback, so pass nullptr for a pure
  // engine timing.
  RunReport query(const std::string& sql,
                  const std::function<void(const std::vector<int64_t>&)>& on_row);
  // query() collecting every row; for tests and small results.
  std::vector<std::vector<int64_t>> fetch_all(const std::string& sql);
  // The text column of EXPLAIN QUERY PLAN, one entry per plan line.
  std::vector<std::string> explain_query_plan(const std::string& sql);
  // Bytes used by each table and index (dbstat virtual table), name -> bytes.
  // Empty when this SQLite was built without SQLITE_ENABLE_DBSTAT_VTAB.
  std::vector<std::pair<std::string, uint64_t>> object_sizes();

  sqlite3* raw() const { return db_; }

 private:
  sqlite3* db_ = nullptr;
};

// ---- load ----------------------------------------------------------------

struct LoadReport {
  uint64_t rows = 0;
  double insert_seconds = 0;      // CREATE TABLE, BEGIN, every INSERT, COMMIT
  double index_seconds = 0;       // the three CREATE INDEX statements
  double analyze_seconds = 0;     // ANALYZE
  uint64_t bytes_table_only = 0;  // file size after COMMIT, before any index
  uint64_t bytes_total = 0;       // file size after indexes + ANALYZE + close
  std::string sqlite_version;
};

// Deletes any existing database at db_path (and its journal) first, so the
// load always measures a fresh file. Throws on any SQLite or I/O error.
LoadReport load_ads(const std::string& ads_path, const std::string& db_path, const LoadOptions& opt);

// Writes the result of one query as CSV: header from the column names, then
// one line per row, integers in decimal and NULL as an empty field. Matches
// colstore's write_csv() shape (including its empty NULL cells) so the bench
// script can diff the two engines' answers.
Db::RunReport write_query_csv(Db& db, const std::string& sql, FILE* out);

}  // namespace colstore::sqlite_bench
