// colstore CLI: ingest tables into the .col columnar format, inspect and
// query the result, and prove every answer against the original rows.
// Two input kinds share every path below: raw events (.ads, the event
// table) and streamkit's per-window aggregate CSV (the aggregate table).
// Subcommands:
//
//   colstore ingest --in events.ads | --in-csv aggregates.csv  --out table.col
//                   [--row-group 65536] [--encoding auto|plain]
//   colstore info   --in table.col [--groups [--col <name>]]
//   colstore dump   --in table.col --col <name> [--n 20]
//   colstore scan   --in table.col [--where "..."] [--cols a,b] [--limit 20] [--count] [--stats] [--vector 1024]
//   colstore q      --in table.col [--where "..."] [--group a,b] --agg "sum(x),count()" [--order "sum_x desc"]
//                   [--limit N] [--threads T] [--stats] [--check events.ads | --check-csv aggregates.csv]
//   colstore verify --col table.col --ads events.ads | --csv aggregates.csv [--where "..." [--cols a,b]]
//
// `verify` is the correctness proof: without --where it walks the original
// rows and the .col columns side by side and asserts every value of every
// row is identical (m1); with --where it runs the predicate through the
// vectorized scan on the .col file and through a brute-force loop over the
// original rows, and asserts the row counts and per-column sums agree (m3).
// `q --check` / `--check-csv` does the same for an aggregate query: the
// whole GROUP BY is recomputed from the original rows with a std::map and
// diffed against the hash aggregate's output (m4). Column names in --where,
// --cols, --group and --agg are the file's own (`info` lists them).

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/args.h"
#include "common/event.h"
#include "src/aggregate.h"
#include "src/exec.h"
#include "src/format.h"
#include "src/predicate.h"
#include "src/query.h"
#include "src/reader.h"
#include "src/row_source.h"
#include "src/writer.h"

using namespace colstore;

namespace {

void usage() {
  std::fprintf(stderr,
    "colstore ingest --in events.ads | --in-csv aggregates.csv  --out table.col\n"
    "                [--row-group 65536] [--encoding auto|plain]\n"
    "colstore info   --in table.col [--groups [--col <name>]]\n"
    "colstore dump   --in table.col --col <name> [--n 20]\n"
    "colstore scan   --in table.col [--where \"country=840 AND event_time_us BETWEEN a AND b\"]\n"
    "                [--cols campaign_id,cost_micros] [--limit 20 (0 = all)] [--count] [--stats] [--vector 1024]\n"
    "colstore q      --in table.col [--where \"...\"] [--group campaign_id,country]\n"
    "                --agg \"sum(cost_micros),count(),min(x),max(x),count_if(event_type=1) AS clicks,sum_if(x,event_type=1)\"\n"
    "                [--order \"sum_cost_micros desc\"] [--limit N] [--threads T] [--stats] [--vector 1024]\n"
    "                [--check events.ads | --check-csv aggregates.csv]   recompute from the original rows and diff\n"
    "colstore verify --col table.col --ads events.ads | --csv aggregates.csv [--where \"...\" [--cols a,b]]\n"
    "event table columns:     event_id event_time_us ingest_time_us event_type advertiser_id\n"
    "                         campaign_id video_id country device cost_micros\n"
    "aggregate table columns: window_start_us campaign_id impressions clicks conversions cost_micros\n");
}

double seconds_since(std::chrono::steady_clock::time_point t0) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}

std::string format_bits(ColumnType type, uint64_t bits) { return format_value(type, bits); }

// The original rows a .col file was built from: --ads for the event table,
// --csv for the aggregate table (the flag names are the subcommand's).
std::unique_ptr<RowSource> open_source(const adstack::Args& args, const char* ads_flag, const char* csv_flag) {
  if (args.has(ads_flag) && args.has(csv_flag)) throw std::invalid_argument(std::string("give ") + ads_flag + " or " + csv_flag + ", not both");
  if (args.has(ads_flag)) return std::make_unique<AdsRowSource>(args.str(ads_flag, ""));
  if (args.has(csv_flag)) return std::make_unique<CsvRowSource>(args.str(csv_flag, ""), aggregate_schema());
  throw std::invalid_argument(std::string("need ") + ads_flag + " or " + csv_flag);
}

// "--cols a,b,c" -> column ids; empty or missing means every column.
std::vector<ColumnId> parse_cols(const std::string& text, const Schema& schema) {
  std::vector<ColumnId> out;
  if (text.empty()) {
    for (size_t i = 0; i < schema.size(); ++i) out.push_back(column_id(i));
    return out;
  }
  size_t start = 0;
  while (start <= text.size()) {
    size_t comma = text.find(',', start);
    if (comma == std::string::npos) comma = text.size();
    const std::string name = text.substr(start, comma - start);
    const auto col = find_column(schema, name);
    if (!col) throw std::invalid_argument("unknown column: " + name);
    out.push_back(*col);
    start = comma + 1;
  }
  return out;
}

// ---- ingest ---------------------------------------------------------------

int cmd_ingest(const adstack::Args& args) {
  if (!args.has("--out") || !(args.has("--in") || args.has("--in-csv"))) { usage(); return 2; }
  const std::string out = args.str("--out", "");
  const int64_t rg = args.i64("--row-group", kDefaultRowsPerGroup);
  if (rg <= 0 || rg > 0xFFFFFFFFLL) { std::fprintf(stderr, "--row-group out of range\n"); return 2; }
  const std::string enc = args.str("--encoding", "auto");
  EncodingPolicy policy = EncodingPolicy::Auto;
  if (enc == "plain") policy = EncodingPolicy::PlainOnly;
  else if (enc != "auto") { std::fprintf(stderr, "--encoding must be auto or plain\n"); return 2; }

  const auto t0 = std::chrono::steady_clock::now();
  const std::unique_ptr<RowSource> source = open_source(args, "--in", "--in-csv");
  const std::string in = args.has("--in") ? args.str("--in", "") : args.str("--in-csv", "");
  ColWriter writer(out, source->schema(), static_cast<uint32_t>(rg), policy);
  std::vector<uint64_t> row;
  while (source->next(row)) writer.append_row(row.data());
  writer.finish();
  const double secs = seconds_since(t0);

  const uint64_t rows = writer.rows_written();
  const uint64_t in_bytes = in == "-" ? 0 : std::filesystem::file_size(in);
  std::fprintf(stderr,
    "ingest: %llu rows in %.3f s (%.2f M rows/s), %zu row groups of %u, %zu columns (%s)\n"
    "        in  %llu bytes  out %llu bytes  (footer %zu bytes, ratio %.3f)\n",
    static_cast<unsigned long long>(rows), secs, secs > 0 ? static_cast<double>(rows) / secs / 1e6 : 0.0,
    writer.footer().row_groups.size(), writer.footer().rows_per_group, writer.schema().size(),
    args.has("--in") ? "event table from .ads" : "aggregate table from CSV",
    static_cast<unsigned long long>(in_bytes), static_cast<unsigned long long>(writer.bytes_written()),
    encode_footer(writer.footer()).size(),
    in_bytes > 0 ? static_cast<double>(writer.bytes_written()) / static_cast<double>(in_bytes) : 0.0);
  return 0;
}

// ---- info -----------------------------------------------------------------

int cmd_info(const adstack::Args& args) {
  if (!args.has("--in")) { usage(); return 2; }
  ColReader r(args.str("--in", ""));
  const Footer& f = r.footer();
  const Schema& schema = r.schema();

  const uint64_t data_bytes = r.file_size() - kHeaderSize - r.footer_bytes() - kTrailerSize;
  std::printf("file          %s\n", args.str("--in", "").c_str());
  std::printf("version       %u\n", kColVersion);
  std::printf("table         %s (%zu columns)\n",
              r.is_event_table() ? "events" : schema == aggregate_schema() ? "aggregates" : "custom", schema.size());
  std::printf("rows          %llu\n", static_cast<unsigned long long>(f.total_rows));
  std::printf("row groups    %zu (up to %u rows each)\n", f.row_groups.size(), f.rows_per_group);
  std::printf("file bytes    %llu  (data %llu, footer %llu, header+trailer %zu)\n",
              static_cast<unsigned long long>(r.file_size()), static_cast<unsigned long long>(data_bytes),
              static_cast<unsigned long long>(r.footer_bytes()), kHeaderSize + kTrailerSize);
  // Per column: bytes actually stored, bytes PLAIN would take (rows * width,
  // the m1 baseline), and which encodings the chunks ended up with. The
  // selector works per chunk, so one column can mix encodings.
  std::printf("\n%-16s %-5s %14s %14s %7s  %s\n", "column", "type", "plain bytes", "bytes", "ratio",
              "encodings (chunks)");
  uint64_t total_plain = 0, total_bytes = 0;
  for (size_t c = 0; c < schema.size(); ++c) {
    const ColumnSpec& spec = schema[c];
    uint64_t bytes = 0;
    std::vector<uint32_t> chunks_by_encoding(kNumEncodings, 0);
    for (const auto& rg : f.row_groups) {
      const ChunkMeta& m = rg.chunks[c];
      bytes += m.length;
      if (m.encoding < kNumEncodings) ++chunks_by_encoding[m.encoding];
    }
    const uint64_t plain = f.total_rows * value_width(spec.type);
    total_plain += plain;
    total_bytes += bytes;
    std::string encodings;
    for (uint32_t e = 0; e < kNumEncodings; ++e) {
      if (chunks_by_encoding[e] == 0) continue;
      if (!encodings.empty()) encodings += " ";
      encodings += std::string(encoding_name(e)) + "(" + std::to_string(chunks_by_encoding[e]) + ")";
    }
    std::printf("%-16s %-5s %14llu %14llu %7.2fx  %s\n", spec.name.c_str(), column_type_name(spec.type),
                static_cast<unsigned long long>(plain), static_cast<unsigned long long>(bytes),
                bytes ? static_cast<double>(plain) / static_cast<double>(bytes) : 0.0,
                encodings.empty() ? "-" : encodings.c_str());
  }
  std::printf("%-16s %-5s %14llu %14llu %7.2fx\n", "total", "", static_cast<unsigned long long>(total_plain),
              static_cast<unsigned long long>(total_bytes),
              total_bytes ? static_cast<double>(total_plain) / static_cast<double>(total_bytes) : 0.0);
  if (args.has("--groups")) {
    // Per row group, with the zone map of one column: --col, else the
    // first column of the table.
    const std::string name = args.str("--col", schema[0].name);
    const auto col = find_column(schema, name);
    if (!col) { std::fprintf(stderr, "unknown column: %s\n", name.c_str()); return 2; }
    const ColumnType type = spec_of(schema, *col).type;
    std::printf("\n%-6s %8s %12s %12s  %s min..max\n", "group", "rows", "offset", "bytes", name.c_str());
    for (size_t i = 0; i < f.row_groups.size(); ++i) {
      const auto& rg = f.row_groups[i];
      uint64_t bytes = 0;
      for (const auto& m : rg.chunks) bytes += m.length;
      const ChunkMeta& t = rg.chunks[column_index(*col)];
      std::printf("%-6zu %8u %12llu %12llu  %s..%s\n", i, rg.rows,
                  static_cast<unsigned long long>(rg.chunks[0].offset), static_cast<unsigned long long>(bytes),
                  format_bits(type, t.min_bits).c_str(), format_bits(type, t.max_bits).c_str());
    }
  }
  return 0;
}

// ---- dump -----------------------------------------------------------------

int cmd_dump(const adstack::Args& args) {
  if (!args.has("--in") || !args.has("--col")) { usage(); return 2; }
  ColReader r(args.str("--in", ""));
  const auto col = find_column(r.schema(), args.str("--col", ""));
  if (!col) { std::fprintf(stderr, "unknown column: %s\n", args.str("--col", "").c_str()); return 2; }
  const int64_t n = args.i64("--n", 20);
  const ColumnType type = spec_of(r.schema(), *col).type;

  int64_t printed = 0;
  for (size_t rg = 0; rg < r.num_row_groups() && printed < n; ++rg) {
    const std::vector<uint64_t> values = r.read_column_bits(rg, *col);  // decodes whatever encoding the chunk has
    for (size_t i = 0; i < values.size() && printed < n; ++i, ++printed) {
      std::printf("%s\n", format_bits(type, values[i]).c_str());
    }
  }
  return 0;
}

// ---- scan -----------------------------------------------------------------

int cmd_scan(const adstack::Args& args) {
  if (!args.has("--in")) { usage(); return 2; }
  ColReader r(args.str("--in", ""));
  const Schema& schema = r.schema();
  const Conjunction where = parse_where(args.str("--where", ""), schema);
  const std::vector<ColumnId> cols = parse_cols(args.str("--cols", ""), schema);
  const int64_t limit = args.i64("--limit", 20);
  if (limit < 0) { std::fprintf(stderr, "--limit must be >= 0 (0 = no limit)\n"); return 2; }
  const bool stats = args.has("--stats");
  const bool count_only = args.has("--count");  // SELECT COUNT(*): no row output, so timings measure the engine
  const int64_t vec = args.i64("--vector", kVectorSize);  // batch size, for the measurement in NOTES.md
  if (vec <= 0 || vec > 0xFFFFFFFFLL) { std::fprintf(stderr, "--vector out of range\n"); return 2; }

  if (!count_only) {
    for (size_t i = 0; i < cols.size(); ++i) std::printf("%s%s", i ? "," : "", spec_of(schema, cols[i]).name.c_str());
    std::printf("\n");
  }

  // Print straight from the batch through the selection vector; nothing
  // is copied out of the decoded columns.
  int64_t printed = 0;
  const QueryStats q = run_query(r, where, cols, [&](const Batch& b) {
    if (count_only) return true;
    for (uint32_t k = 0; k < b.selected() && (limit == 0 || printed < limit); ++k, ++printed) {
      const uint32_t row = b.row(k);
      for (size_t i = 0; i < cols.size(); ++i) {
        const ColumnSpec& spec = spec_of(schema, cols[i]);
        std::printf("%s%s", i ? "," : "", format_bits(spec.type, b.cols[column_index(cols[i])].at(row)).c_str());
      }
      std::printf("\n");
    }
    return limit == 0 || printed < limit;
  }, static_cast<uint32_t>(vec));
  if (count_only) std::printf("%llu\n", static_cast<unsigned long long>(q.rows_passed));
  if (stats) {
    std::fprintf(stderr, "scan: %s\n", describe(q).c_str());
    for (const Predicate& p : where) std::fprintf(stderr, "  where %s\n", to_string(p).c_str());
    if (!count_only && limit != 0 && printed >= limit) std::fprintf(stderr, "  (stopped at --limit %lld; rows passed counts only batches seen)\n",
                                                   static_cast<long long>(limit));
  }
  return 0;
}

// ---- q: aggregate query ---------------------------------------------------

int cmd_q(const adstack::Args& args) {
  if (!args.has("--in") || !args.has("--agg")) { usage(); return 2; }
  if (args.has("--check") && args.has("--check-csv")) { std::fprintf(stderr, "give --check or --check-csv, not both\n"); return 2; }
  QueryPlan plan;
  plan.schema = ColReader(args.str("--in", "")).schema();  // the plan names this file's columns
  plan.where = parse_where(args.str("--where", ""), plan.schema);
  plan.group = args.has("--group") ? parse_cols(args.str("--group", ""), plan.schema) : std::vector<ColumnId>{};
  plan.aggs = parse_aggs(args.str("--agg", ""), plan.schema);
  plan.order = parse_order(args.str("--order", ""), output_columns(plan.group, plan.aggs, plan.schema));
  const int64_t limit = args.i64("--limit", 0);
  if (limit < 0) { std::fprintf(stderr, "--limit must be >= 0 (0 = no limit)\n"); return 2; }
  plan.limit = static_cast<size_t>(limit);
  const int64_t threads = args.i64("--threads", 1);
  if (threads < 1 || threads > 1024) { std::fprintf(stderr, "--threads must be 1..1024\n"); return 2; }
  plan.threads = static_cast<unsigned>(threads);
  const int64_t vec = args.i64("--vector", kVectorSize);
  if (vec <= 0 || vec > 0xFFFFFFFFLL) { std::fprintf(stderr, "--vector out of range\n"); return 2; }
  plan.batch_rows = static_cast<uint32_t>(vec);

  const QueryResult r = run_aggregate_query(args.str("--in", ""), plan);
  write_csv(stdout, r);
  std::fflush(stdout);
  if (args.has("--stats")) {
    std::fprintf(stderr, "q: %s\n", describe(r).c_str());
    for (const Predicate& p : plan.where) std::fprintf(stderr, "  where %s\n", to_string(p).c_str());
    for (ColumnId c : plan.group) std::fprintf(stderr, "  group %s\n", spec_of(plan.schema, c).name.c_str());
    for (const AggSpec& a : plan.aggs) std::fprintf(stderr, "  agg   %s\n", to_string(a).c_str());
    for (const OrderSpec& o : effective_order(plan, r.table.schema)) {
      std::fprintf(stderr, "  order %s\n", to_string(o, r.table.schema).c_str());
    }
    if (plan.limit) std::fprintf(stderr, "  limit %zu\n", plan.limit);
    std::fprintf(stderr, "  threads %u\n", plan.threads);
  }
  if (args.has("--check") || args.has("--check-csv")) {
    const std::unique_ptr<RowSource> rows = open_source(args, "--check", "--check-csv");
    const CheckReport c = check_against_rows(*rows, plan, r);
    std::fprintf(stderr, "q --check: %s (%s; brute force %.3f s, engine %.3f s)\n", c.ok ? "PASS" : "FAIL",
                 c.detail.c_str(), c.seconds, r.total_seconds);
    return c.ok ? 0 : 1;
  }
  return 0;
}

// ---- verify ---------------------------------------------------------------

// Filtered verify: the same predicate evaluated two ways. The scan path
// uses zone maps, dictionary codes and selection vectors; the brute-force
// path reads every original row and tests it with Predicate::matches().
// Row counts and per-column sums (wrapping uint64 of the raw patterns,
// printed as signed for i64 columns) must agree exactly.
int cmd_verify_where(const adstack::Args& args, const ColReader& r, RowSource& source) {
  const Schema& schema = r.schema();
  const Conjunction where = parse_where(args.str("--where", ""), schema);
  const std::vector<ColumnId> cols = parse_cols(args.str("--cols", ""), schema);

  const auto t0 = std::chrono::steady_clock::now();
  uint64_t scan_rows = 0;
  std::vector<uint64_t> scan_sums(cols.size(), 0);
  const QueryStats q = run_query(r, where, cols, [&](const Batch& b) {
    scan_rows += b.selected();
    for (size_t i = 0; i < cols.size(); ++i) {
      const ColumnVector& cv = b.cols[column_index(cols[i])];
      for (uint32_t k = 0; k < b.selected(); ++k) scan_sums[i] += cv.at(b.row(k));
    }
    return true;
  });
  const double scan_secs = seconds_since(t0);

  const auto t1 = std::chrono::steady_clock::now();
  uint64_t brute_rows = 0;
  std::vector<uint64_t> brute_sums(cols.size(), 0);
  std::vector<uint64_t> row;
  while (source.next(row)) {
    if (!matches_row(where, row.data())) continue;
    ++brute_rows;
    for (size_t i = 0; i < cols.size(); ++i) brute_sums[i] += row[column_index(cols[i])];
  }
  const double brute_secs = seconds_since(t1);

  bool ok = scan_rows == brute_rows;
  std::fprintf(stderr, "verify --where: rows scan=%llu brute=%llu\n", static_cast<unsigned long long>(scan_rows),
               static_cast<unsigned long long>(brute_rows));
  for (size_t i = 0; i < cols.size(); ++i) {
    const ColumnSpec& spec = spec_of(schema, cols[i]);
    std::fprintf(stderr, "  sum(%s) scan=%s brute=%s\n", spec.name.c_str(), format_bits(spec.type, scan_sums[i]).c_str(),
                 format_bits(spec.type, brute_sums[i]).c_str());
    ok = ok && scan_sums[i] == brute_sums[i];
  }
  std::fprintf(stderr, "  scan %.3f s, brute force %.3f s\n  %s\n", scan_secs, brute_secs, describe(q).c_str());
  std::fprintf(stderr, "verify --where: %s\n", ok ? "PASS" : "FAIL");
  return ok ? 0 : 1;
}

// Full verify: every value of every row, .col columns against the source
// rows, in order. The .col side is read a row group at a time (all columns
// decoded, whatever their encoding); the source side one row at a time.
int cmd_verify(const adstack::Args& args) {
  if (!args.has("--col") || !(args.has("--ads") || args.has("--csv"))) { usage(); return 2; }
  ColReader r(args.str("--col", ""));
  const std::unique_ptr<RowSource> source = open_source(args, "--ads", "--csv");
  if (source->schema() != r.schema()) {
    std::fprintf(stderr, "verify: FAIL the .col file's columns are not the %s table's\n",
                 args.has("--ads") ? "event" : "aggregate");
    return 1;
  }
  if (args.has("--where")) return cmd_verify_where(args, r, *source);
  const auto t0 = std::chrono::steady_clock::now();
  const Schema& schema = r.schema();

  uint64_t row = 0;
  std::vector<uint64_t> want;
  std::vector<std::vector<uint64_t>> cols(schema.size());
  for (size_t rg = 0; rg < r.num_row_groups(); ++rg) {
    for (size_t c = 0; c < schema.size(); ++c) cols[c] = r.read_column_bits(rg, column_id(c));
    const size_t rows = r.footer().row_groups[rg].rows;
    for (size_t i = 0; i < rows; ++i, ++row) {
      if (!source->next(want)) {
        std::fprintf(stderr, "verify: FAIL source ended at row %llu but .col has %llu rows\n",
                     static_cast<unsigned long long>(row), static_cast<unsigned long long>(r.total_rows()));
        return 1;
      }
      for (size_t c = 0; c < schema.size(); ++c) {
        if (cols[c][i] != want[c]) {
          std::fprintf(stderr, "verify: FAIL at row %llu\n  %s: col=%s source=%s\n", static_cast<unsigned long long>(row),
                       schema[c].name.c_str(), format_bits(schema[c].type, cols[c][i]).c_str(),
                       format_bits(schema[c].type, want[c]).c_str());
          return 1;
        }
      }
    }
  }
  if (source->next(want)) {
    std::fprintf(stderr, "verify: FAIL .col ended at row %llu but the source has more rows\n",
                 static_cast<unsigned long long>(row));
    return 1;
  }
  std::fprintf(stderr, "verify: PASS %llu rows, %zu row groups, all %zu columns identical (%.3f s)\n",
               static_cast<unsigned long long>(row), r.num_row_groups(), schema.size(), seconds_since(t0));
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) { usage(); return 2; }
  const std::string cmd = argv[1];
  adstack::Args args(argc, argv);  // skips argv[1]: it does not start with "--"
  try {
    if (cmd == "ingest") return cmd_ingest(args);
    if (cmd == "info") return cmd_info(args);
    if (cmd == "dump") return cmd_dump(args);
    if (cmd == "scan") return cmd_scan(args);
    if (cmd == "q") return cmd_q(args);
    if (cmd == "verify") return cmd_verify(args);
    usage();
    return cmd == "--help" ? 0 : 2;
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "colstore %s: %s\n", cmd.c_str(), ex.what());
    return 1;
  }
}
