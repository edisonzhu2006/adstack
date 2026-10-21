// colstore_sqlite: the SQLite half of the m5 benchmark (bench/colstore_bench.sh).
// A tiny CLI over src/sqlite_loader.h so that every SQLite number in
// colstore/BENCHMARKS.md comes from a command with its settings printed.
//
//   colstore_sqlite load  --in events.ads --out events.sqlite [--no-index] [--no-analyze] [--cache-mib 1024]
//   colstore_sqlite query --db events.sqlite --sql "SELECT ..." [--runs 3] [--plan] [--reopen]
//                         [--cache-mib 1024] [--mmap-mib 1024]
//   colstore_sqlite sizes --db events.sqlite
//
// `query` runs the statement once with its rows written as CSV to stdout
// (header + rows, the same shape as `colstore q`, so the bench script can
// diff the two engines' answers), then --runs more times with no output
// and prints each time and the median to stderr. By default all runs share
// one connection, so from the second run on SQLite serves pages from its
// own cache; --reopen opens a fresh connection per run (OS page cache
// only), which is closer to how the colstore numbers are taken (one process
// per run). Both are reported in BENCHMARKS.md.

#include <algorithm>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/args.h"
#include "src/sqlite_loader.h"

using namespace colstore::sqlite_bench;

namespace {

void usage() {
  std::fprintf(stderr,
    "colstore_sqlite load  --in events.ads --out events.sqlite [--no-index] [--no-analyze] [--cache-mib 1024]\n"
    "colstore_sqlite query --db events.sqlite --sql \"SELECT ...\" [--runs 3] [--plan] [--reopen]\n"
    "                      [--cache-mib 1024] [--mmap-mib 1024]\n"
    "colstore_sqlite sizes --db events.sqlite\n");
}

void print_pragmas(const std::vector<std::string>& pragmas) {
  std::fprintf(stderr, "  pragmas:");
  for (const std::string& p : pragmas) std::fprintf(stderr, " [%s]", p.c_str() + 7);  // drop "PRAGMA "
  std::fprintf(stderr, "\n");
}

void print_object_sizes(Db& db) {
  const auto sizes = db.object_sizes();
  if (sizes.empty()) {
    std::fprintf(stderr, "  object sizes: not available (SQLite built without dbstat)\n");
    return;
  }
  for (const auto& [name, bytes] : sizes) {
    std::fprintf(stderr, "  object %-28s %14llu bytes\n", name.c_str(), static_cast<unsigned long long>(bytes));
  }
}

int cmd_load(const adstack::Args& args) {
  if (!args.has("--in") || !args.has("--out")) { usage(); return 2; }
  LoadOptions opt;
  opt.create_indexes = !args.has("--no-index");
  opt.analyze = !args.has("--no-analyze");
  opt.cache_mib = args.i64("--cache-mib", opt.cache_mib);
  if (opt.cache_mib <= 0) { std::fprintf(stderr, "--cache-mib must be > 0\n"); return 2; }

  const LoadReport r = load_ads(args.str("--in", ""), args.str("--out", ""), opt);
  const double total = r.insert_seconds + r.index_seconds + r.analyze_seconds;
  std::fprintf(stderr,
    "sqlite load: %llu rows; insert %.3f s, indexes %.3f s, analyze %.3f s, total %.3f s (%.2f M rows/s)\n"
    "  table only %llu bytes; with indexes %llu bytes; sqlite %s\n",
    static_cast<unsigned long long>(r.rows), r.insert_seconds, r.index_seconds, r.analyze_seconds, total,
    total > 0 ? static_cast<double>(r.rows) / total / 1e6 : 0.0,
    static_cast<unsigned long long>(r.bytes_table_only), static_cast<unsigned long long>(r.bytes_total),
    r.sqlite_version.c_str());
  print_pragmas(load_pragmas(opt));
  std::fprintf(stderr, "  schema: %s\n", create_table_sql().c_str());
  if (opt.create_indexes) {
    for (const std::string& sql : create_index_sqls()) std::fprintf(stderr, "  index:  %s\n", sql.c_str());
  }
  Db db(args.str("--out", ""), Db::Mode::kReadOnly);
  print_object_sizes(db);
  return 0;
}

double median(std::vector<double> v) {
  std::sort(v.begin(), v.end());
  return v[v.size() / 2];  // for an even count this is the upper middle; the script uses 3 runs
}

int cmd_query(const adstack::Args& args) {
  if (!args.has("--db") || !args.has("--sql")) { usage(); return 2; }
  const std::string path = args.str("--db", "");
  const std::string sql = args.str("--sql", "");
  const int64_t runs = args.i64("--runs", 3);
  if (runs < 1 || runs > 1000) { std::fprintf(stderr, "--runs must be 1..1000\n"); return 2; }
  QueryOptions opt;
  opt.cache_mib = args.i64("--cache-mib", opt.cache_mib);
  opt.mmap_mib = args.i64("--mmap-mib", opt.mmap_mib);
  if (opt.cache_mib <= 0 || opt.mmap_mib < 0) { std::fprintf(stderr, "--cache-mib must be > 0, --mmap-mib >= 0\n"); return 2; }
  const bool reopen = args.has("--reopen");

  auto open = [&] {
    auto db = std::make_unique<Db>(path, Db::Mode::kReadOnly);
    for (const std::string& p : query_pragmas(opt)) db->exec(p);
    return db;
  };

  // Run 0: the answer, as CSV on stdout. Not timed as a benchmark run: it
  // also pays for the connection's first touch of every page it needs.
  std::unique_ptr<Db> db = open();
  if (args.has("--plan")) {
    for (const std::string& line : db->explain_query_plan(sql)) std::fprintf(stderr, "  plan: %s\n", line.c_str());
  }
  const Db::RunReport first = write_query_csv(*db, sql, stdout);
  std::fflush(stdout);

  std::vector<double> times;
  for (int64_t i = 0; i < runs; ++i) {
    if (reopen) db = open();
    const Db::RunReport r = db->query(sql, nullptr);
    if (r.rows != first.rows) throw std::runtime_error("row count changed between runs");
    times.push_back(r.seconds);
  }
  std::fprintf(stderr, "sqlite query: %llu rows; first run (with csv) %.4f s; %s runs:",
               static_cast<unsigned long long>(first.rows), first.seconds,
               reopen ? "fresh-connection" : "same-connection");
  for (double t : times) std::fprintf(stderr, " %.4f", t);
  std::fprintf(stderr, " | median %.4f s\n", median(times));
  print_pragmas(query_pragmas(opt));
  return 0;
}

int cmd_sizes(const adstack::Args& args) {
  if (!args.has("--db")) { usage(); return 2; }
  Db db(args.str("--db", ""), Db::Mode::kReadOnly);
  print_object_sizes(db);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) { usage(); return 2; }
  const std::string cmd = argv[1];
  adstack::Args args(argc, argv);
  try {
    if (cmd == "load") return cmd_load(args);
    if (cmd == "query") return cmd_query(args);
    if (cmd == "sizes") return cmd_sizes(args);
    usage();
    return cmd == "--help" ? 0 : 2;
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "colstore_sqlite %s: %s\n", cmd.c_str(), ex.what());
    return 1;
  }
}
