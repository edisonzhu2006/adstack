// SQLite loader and query runner for the m5 benchmark; see sqlite_loader.h
// for what it is and why it is separate from colstore_core.

#include "src/sqlite_loader.h"

#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <optional>
#include <stdexcept>

#include "common/event.h"
#include "src/format.h"
#include "src/predicate.h"  // event_field_bits(): the same per-column view verify/--check use

namespace colstore::sqlite_bench {

namespace {

double seconds_since(std::chrono::steady_clock::time_point t0) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}

[[noreturn]] void fail(sqlite3* db, const std::string& what) {
  throw std::runtime_error("sqlite: " + what + ": " + (db ? sqlite3_errmsg(db) : "(no connection)"));
}

uint64_t file_bytes(const std::string& path) {
  return std::filesystem::exists(path) ? std::filesystem::file_size(path) : 0;
}

// A prepared statement that finalizes itself, so an exception in the middle
// of a load or a query cannot leak one (sqlite3_close would then fail).
class Stmt {
 public:
  Stmt(sqlite3* db, const std::string& sql) {
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK) fail(db, "prepare `" + sql + "`");
  }
  ~Stmt() { sqlite3_finalize(stmt_); }
  Stmt(const Stmt&) = delete;
  Stmt& operator=(const Stmt&) = delete;
  sqlite3_stmt* get() const { return stmt_; }

 private:
  sqlite3_stmt* stmt_ = nullptr;
};

std::string text_column(sqlite3_stmt* stmt, int col) {
  const unsigned char* text = sqlite3_column_text(stmt, col);
  return text ? reinterpret_cast<const char*>(text) : "";
}

}  // namespace

// ---- settings --------------------------------------------------------------

std::vector<std::string> load_pragmas(const LoadOptions& opt) {
  return {
    // No rollback journal and no fsync: a bulk load into a fresh file has
    // nothing to protect (if it dies, rerun it), and these two are the
    // settings that make SQLite's bulk insert fast. Stating them is part
    // of the fairness argument: SQLite is not being timed with durability
    // on while colstore's writer has none.
    "PRAGMA journal_mode = OFF",
    "PRAGMA synchronous = OFF",
    "PRAGMA cache_size = -" + std::to_string(opt.cache_mib * 1024),  // negative = KiB
    "PRAGMA temp_store = MEMORY",       // index builds sort in memory, not in temp files
    "PRAGMA locking_mode = EXCLUSIVE",  // one lock for the whole load, not one per statement
  };
}

std::vector<std::string> query_pragmas(const QueryOptions& opt) {
  return {
    "PRAGMA query_only = 1",
    "PRAGMA cache_size = -" + std::to_string(opt.cache_mib * 1024),
    "PRAGMA mmap_size = " + std::to_string(opt.mmap_mib * 1024 * 1024),
    "PRAGMA temp_store = MEMORY",  // GROUP BY's temporary b-tree, if the planner needs one
  };
}

std::string create_table_sql() {
  // Column list straight from colstore's column table, so the SQLite schema
  // and the .col schema cannot drift apart. All INTEGER: SQLite stores each
  // as a 1..9-byte varint, which is its own (row-level) compression.
  std::string sql = "CREATE TABLE events (";
  bool first = true;
  for (const ColumnSpec& spec : event_schema()) {
    if (!first) sql += ", ";
    first = false;
    sql += spec.name + " INTEGER NOT NULL";
  }
  return sql + ")";
}

std::vector<std::string> create_index_sqls() {
  // One index per benchmark access path: Q2's time range, Q1's
  // (country, event_type) point filter (a covering index for COUNT(*)),
  // and the GROUP BY column of Q2 / Q3.
  return {
    "CREATE INDEX idx_events_time ON events(event_time_us)",
    "CREATE INDEX idx_events_country_type ON events(country, event_type)",
    "CREATE INDEX idx_events_campaign ON events(campaign_id)",
  };
}

// ---- Db ----------------------------------------------------------------------

Db::Db(const std::string& path, Mode mode) {
  // NOMUTEX: this program never shares a connection between threads, so
  // SQLite's per-call mutex is pure overhead on the query path.
  const int flags = (mode == Mode::kCreate ? (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE) : SQLITE_OPEN_READONLY) |
                    SQLITE_OPEN_NOMUTEX;
  if (sqlite3_open_v2(path.c_str(), &db_, flags, nullptr) != SQLITE_OK) {
    const std::string msg = db_ ? sqlite3_errmsg(db_) : "out of memory";
    sqlite3_close(db_);
    db_ = nullptr;
    throw std::runtime_error("sqlite: open " + path + ": " + msg);
  }
}

Db::~Db() { sqlite3_close(db_); }

void Db::exec(const std::string& sql) {
  char* err = nullptr;
  if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
    const std::string msg = err ? err : "unknown error";
    sqlite3_free(err);
    throw std::runtime_error("sqlite: exec `" + sql + "`: " + msg);
  }
}

Db::RunReport Db::query(const std::string& sql, const std::function<void(const std::vector<int64_t>&)>& on_row) {
  RunReport r;
  const auto t0 = std::chrono::steady_clock::now();
  {
    Stmt stmt(db_, sql);
    const int ncol = sqlite3_column_count(stmt.get());
    for (int c = 0; c < ncol; ++c) r.columns.emplace_back(sqlite3_column_name(stmt.get(), c));
    std::vector<int64_t> values(static_cast<size_t>(ncol));
    for (;;) {
      const int rc = sqlite3_step(stmt.get());
      if (rc == SQLITE_DONE) break;
      if (rc != SQLITE_ROW) fail(db_, "step `" + sql + "`");
      ++r.rows;
      if (on_row) {
        for (int c = 0; c < ncol; ++c) values[static_cast<size_t>(c)] = sqlite3_column_int64(stmt.get(), c);
        on_row(values);
      }
    }
  }  // finalize inside the timed region: it is part of running a statement
  r.seconds = seconds_since(t0);
  return r;
}

std::vector<std::vector<int64_t>> Db::fetch_all(const std::string& sql) {
  std::vector<std::vector<int64_t>> rows;
  query(sql, [&](const std::vector<int64_t>& v) { rows.push_back(v); });
  return rows;
}

std::vector<std::string> Db::explain_query_plan(const std::string& sql) {
  // The plan rows are (id, parent, notused, detail); only detail is text,
  // so this cannot go through query(), which reads integers.
  std::vector<std::string> lines;
  Stmt stmt(db_, "EXPLAIN QUERY PLAN " + sql);
  for (;;) {
    const int rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) break;
    if (rc != SQLITE_ROW) fail(db_, "explain `" + sql + "`");
    lines.push_back(text_column(stmt.get(), 3));
  }
  return lines;
}

std::vector<std::pair<std::string, uint64_t>> Db::object_sizes() {
  std::vector<std::pair<std::string, uint64_t>> out;
  try {
    Stmt stmt(db_, "SELECT name, SUM(pgsize) FROM dbstat GROUP BY name ORDER BY name");
    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
      out.emplace_back(text_column(stmt.get(), 0), static_cast<uint64_t>(sqlite3_column_int64(stmt.get(), 1)));
    }
  } catch (const std::runtime_error&) {
    // "no such table: dbstat": this SQLite was built without the virtual
    // table. The caller then only has the file size; that is not an error.
    out.clear();
  }
  return out;
}

// ---- load ----------------------------------------------------------------------

LoadReport load_ads(const std::string& ads_path, const std::string& db_path, const LoadOptions& opt) {
  std::filesystem::remove(db_path);
  std::filesystem::remove(db_path + "-journal");
  std::filesystem::remove(db_path + "-wal");

  LoadReport report;
  report.sqlite_version = sqlite3_libversion();
  {
    Db db(db_path, Db::Mode::kCreate);
    for (const std::string& p : load_pragmas(opt)) db.exec(p);

    // One transaction for the whole table: per-row transactions would pay a
    // commit per row. One prepared INSERT, bound and reset per row: no SQL
    // parsing on the hot path. This is the standard fast-load recipe.
    const auto t0 = std::chrono::steady_clock::now();
    db.exec(create_table_sql());
    db.exec("BEGIN");
    {
      std::string placeholders;
      for (uint32_t i = 0; i < kNumEventColumns; ++i) placeholders += i ? ",?" : "?";
      Stmt insert(db.raw(), "INSERT INTO events VALUES (" + placeholders + ")");
      adstack::EventReader reader(ads_path);
      adstack::Event e;
      while (reader.next(e)) {
        for (uint32_t c = 0; c < kNumEventColumns; ++c) {
          // Same 64-bit pattern the .col side stores; see the header for u64.
          const int64_t v = static_cast<int64_t>(event_field_bits(e, column_id(c)));
          sqlite3_bind_int64(insert.get(), static_cast<int>(c) + 1, v);
        }
        if (sqlite3_step(insert.get()) != SQLITE_DONE) fail(db.raw(), "insert");
        sqlite3_reset(insert.get());
        ++report.rows;
      }
    }
    db.exec("COMMIT");
    report.insert_seconds = seconds_since(t0);
    report.bytes_table_only = file_bytes(db_path);

    if (opt.create_indexes) {
      const auto t1 = std::chrono::steady_clock::now();
      for (const std::string& sql : create_index_sqls()) db.exec(sql);
      report.index_seconds = seconds_since(t1);
    }
    if (opt.analyze) {
      const auto t2 = std::chrono::steady_clock::now();
      db.exec("ANALYZE");
      report.analyze_seconds = seconds_since(t2);
    }
  }  // connection closed here, so the size below includes every page written
  report.bytes_total = file_bytes(db_path);
  return report;
}

// ---- CSV --------------------------------------------------------------------------

Db::RunReport write_query_csv(Db& db, const std::string& sql, FILE* out) {
  // Its own step loop rather than query(), because query() reads every
  // value as an integer and a NULL would print as 0. NULL is an empty
  // field here, the same text colstore's write_csv() gives a NULL cell
  // (a global aggregate over zero rows), so the cross-engine diff can tell
  // "SUM over nothing" from "SUM = 0". Rows are collected first and written
  // after the header: the results are at most 100K short rows, and the
  // formatting stays outside the measured time, as it does on the colstore
  // side (q --stats excludes write_csv).
  Db::RunReport r;
  std::vector<std::vector<std::optional<int64_t>>> rows;
  const auto t0 = std::chrono::steady_clock::now();
  {
    Stmt stmt(db.raw(), sql);
    const int ncol = sqlite3_column_count(stmt.get());
    for (int c = 0; c < ncol; ++c) r.columns.emplace_back(sqlite3_column_name(stmt.get(), c));
    for (;;) {
      const int rc = sqlite3_step(stmt.get());
      if (rc == SQLITE_DONE) break;
      if (rc != SQLITE_ROW) fail(db.raw(), "step `" + sql + "`");
      ++r.rows;
      std::vector<std::optional<int64_t>>& v = rows.emplace_back(static_cast<size_t>(ncol));
      for (int c = 0; c < ncol; ++c) {
        if (sqlite3_column_type(stmt.get(), c) != SQLITE_NULL) v[static_cast<size_t>(c)] = sqlite3_column_int64(stmt.get(), c);
      }
    }
  }
  r.seconds = seconds_since(t0);
  for (size_t i = 0; i < r.columns.size(); ++i) std::fprintf(out, "%s%s", i ? "," : "", r.columns[i].c_str());
  std::fputc('\n', out);
  for (const auto& values : rows) {
    for (size_t i = 0; i < values.size(); ++i) {
      if (i) std::fputc(',', out);
      if (values[i]) std::fprintf(out, "%lld", static_cast<long long>(*values[i]));
    }
    std::fputc('\n', out);
  }
  return r;
}

}  // namespace colstore::sqlite_bench
