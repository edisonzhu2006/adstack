// Aggregate query driver, CSV output and the brute-force check. See query.h.

#include "src/query.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <map>
#include <stdexcept>
#include <thread>

#include "common/event.h"
#include "src/encoding.h"  // less_in_type
#include "src/reader.h"

namespace colstore {

namespace {

double seconds_since(std::chrono::steady_clock::time_point t0) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}

void add_stats(QueryStats& into, const QueryStats& s) {
  into.rows_passed += s.rows_passed;
  into.scan.row_groups_total += s.scan.row_groups_total;
  into.scan.row_groups_skipped_zone += s.scan.row_groups_skipped_zone;
  into.scan.row_groups_skipped_dict += s.scan.row_groups_skipped_dict;
  into.scan.row_groups_scanned += s.scan.row_groups_scanned;
  into.scan.rows_scanned += s.scan.rows_scanned;
  into.scan.chunks_decoded += s.scan.chunks_decoded;
  into.scan.bytes_read += s.scan.bytes_read;
  into.scan.predicates_dropped += s.scan.predicates_dropped;
  into.scan.predicates_on_codes += s.scan.predicates_on_codes;
  into.scan.predicates_on_values += s.scan.predicates_on_values;
}

// Scan the row groups (first, first + stride, ...) of the file at `path`
// into `agg`. Each call opens its own reader, so it is safe to run one per
// thread.
QueryStats scan_into(const std::string& path, const QueryPlan& plan, HashAggregate& agg, size_t first,
                     size_t stride) {
  ColReader reader(path);
  return run_query(reader, plan.where, agg.columns_needed(),
                   [&](const Batch& b) { agg.consume(b); return true; }, plan.batch_rows, first, stride);
}

}  // namespace

std::vector<OrderSpec> effective_order(const QueryPlan& plan, const std::vector<OutputColumn>& schema) {
  if (plan.order.empty() && plan.limit == 0) return {};
  std::vector<OrderSpec> out = plan.order;
  // Group columns are schema[0 .. group.size()); add the missing ones so
  // ties are broken by the key, not by insertion order.
  for (size_t c = 0; c < plan.group.size(); ++c) {
    const bool present = std::any_of(out.begin(), out.end(), [c](const OrderSpec& o) { return o.column == c; });
    if (!present) out.push_back(OrderSpec{c, false});
  }
  (void)schema;
  return out;
}

QueryResult run_aggregate_query(const std::string& col_path, const QueryPlan& plan) {
  if (plan.threads == 0) throw std::invalid_argument("threads must be >= 1");
  const auto t0 = std::chrono::steady_clock::now();
  const std::vector<OutputColumn> schema = output_columns(plan.group, plan.aggs);  // validates names
  for (const OrderSpec& o : plan.order) {
    if (o.column >= schema.size()) throw std::invalid_argument("order column out of range");
  }

  // One aggregate per thread; thread 0 is the one everything merges into.
  const unsigned T = plan.threads;
  std::vector<HashAggregate> aggs;
  aggs.reserve(T);
  for (unsigned t = 0; t < T; ++t) aggs.emplace_back(plan.group, plan.aggs);
  std::vector<QueryStats> stats(T);

  QueryResult r;
  if (T == 1) {
    stats[0] = scan_into(col_path, plan, aggs[0], 0, 1);
  } else {
    // Each thread writes only its own aggs[t] / stats[t] / errors[t] slot
    // and reads only the shared, immutable plan and path; join() publishes
    // everything to the main thread.
    std::vector<std::exception_ptr> errors(T);
    std::vector<std::thread> workers;
    for (unsigned t = 0; t < T; ++t) {
      workers.emplace_back([&, t] {
        try {
          stats[t] = scan_into(col_path, plan, aggs[t], t, T);
        } catch (...) {
          errors[t] = std::current_exception();
        }
      });
    }
    for (std::thread& w : workers) w.join();
    for (const std::exception_ptr& e : errors) {
      if (e) std::rethrow_exception(e);
    }
  }
  r.scan_agg_seconds = seconds_since(t0);
  const auto t_merge = std::chrono::steady_clock::now();
  for (unsigned t = 1; t < T; ++t) aggs[0].merge(aggs[t]);
  r.merge_seconds = seconds_since(t_merge);
  for (const QueryStats& s : stats) add_stats(r.scan, s);
  r.agg = aggs[0].stats();
  const auto t_finish = std::chrono::steady_clock::now();
  r.table = aggs[0].finish();
  r.finish_seconds = seconds_since(t_finish);
  r.scan.seconds = r.scan_agg_seconds;  // the per-thread times overlap; report the wall time

  const auto t1 = std::chrono::steady_clock::now();
  r.rows = sort_limit(r.table, effective_order(plan, schema), plan.limit);
  r.sort_seconds = seconds_since(t1);
  r.total_seconds = seconds_since(t0);
  return r;
}

void write_csv(FILE* out, const QueryResult& r) {
  const ResultTable& t = r.table;
  for (size_t c = 0; c < t.schema.size(); ++c) std::fprintf(out, "%s%s", c ? "," : "", t.schema[c].name.c_str());
  std::fputc('\n', out);
  std::string line;
  for (uint32_t row : r.rows) {
    line.clear();
    for (size_t c = 0; c < t.schema.size(); ++c) {
      if (c) line += ',';
      line += format_value(t.schema[c].type, t.at(c, row));
    }
    line += '\n';
    std::fputs(line.c_str(), out);
  }
}

std::string describe(const QueryResult& r) {
  char buf[256];
  std::string s = describe(r.scan);  // the scan lines (groups skipped, rows, chunks, scan time)
  std::snprintf(buf, sizeof buf,
                "\naggregate: %zu groups, %llu rows in %llu batches; table %zu slots, %zu doublings, "
                "%.2f probes per row",
                r.agg.groups, static_cast<unsigned long long>(r.agg.rows),
                static_cast<unsigned long long>(r.agg.batches), r.agg.slots, r.agg.grows,
                r.agg.rows ? static_cast<double>(r.agg.probes) / static_cast<double>(r.agg.rows) : 0.0);
  s += buf;
  std::snprintf(buf, sizeof buf,
                "\noutput: %zu rows; scan+aggregate %.3f s, merge %.4f s, finish %.4f s, sort %.4f s, total %.3f s",
                r.rows.size(), r.scan_agg_seconds, r.merge_seconds, r.finish_seconds, r.sort_seconds, r.total_seconds);
  s += buf;
  return s;
}

// ---- brute force ----------------------------------------------------------------------

namespace {

using KeyBits = std::vector<uint64_t>;   // group column patterns
using Cells = std::vector<uint64_t>;     // one per aggregate, same representation as the table

// Plain per-event update written independently of HashAggregate: signed
// compares through less_in_type, no order keys, no packing.
void brute_update(const QueryPlan& plan, const adstack::Event& e, Cells& cells) {
  for (size_t a = 0; a < plan.aggs.size(); ++a) {
    const AggSpec& s = plan.aggs[a];
    const uint64_t v = s.col ? event_field_bits(e, *s.col) : 0;
    const bool pass = s.cond ? s.cond->matches(event_field_bits(e, s.cond->col)) : true;
    switch (s.kind) {
      case AggKind::Count: cells[a] += 1; break;
      case AggKind::CountIf: cells[a] += pass ? 1 : 0; break;
      case AggKind::Sum: cells[a] += v; break;
      case AggKind::SumIf: cells[a] += pass ? v : 0; break;
      case AggKind::Min: if (less_in_type(v, cells[a], column_spec(*s.col).type)) cells[a] = v; break;
      case AggKind::Max: if (less_in_type(cells[a], v, column_spec(*s.col).type)) cells[a] = v; break;
    }
  }
}

Cells brute_initial(const QueryPlan& plan, const adstack::Event& first) {
  Cells c(plan.aggs.size(), 0);
  for (size_t a = 0; a < plan.aggs.size(); ++a) {
    const AggSpec& s = plan.aggs[a];
    // MIN / MAX start at the first value of the group, the textbook way.
    if (s.kind == AggKind::Min || s.kind == AggKind::Max) c[a] = event_field_bits(first, *s.col);
  }
  return c;
}

std::string row_text(const ResultTable& t, const KeyBits& key, const Cells& cells) {
  std::string s;
  for (size_t c = 0; c < t.schema.size(); ++c) {
    const uint64_t v = c < key.size() ? key[c] : cells[c - key.size()];
    s += (c ? "," : "") + format_value(t.schema[c].type, v);
  }
  return s;
}

std::string engine_row_text(const ResultTable& t, uint32_t row) {
  std::string s;
  for (size_t c = 0; c < t.schema.size(); ++c) s += (c ? "," : "") + format_value(t.schema[c].type, t.at(c, row));
  return s;
}

}  // namespace

CheckReport check_against_ads(const std::string& ads_path, const QueryPlan& plan, const QueryResult& r) {
  const auto t0 = std::chrono::steady_clock::now();
  CheckReport rep;
  const ResultTable& t = r.table;
  const size_t ng = plan.group.size();

  // 1. Brute force: every event, std::map keyed by the group values.
  std::map<KeyBits, Cells> truth;
  {
    adstack::EventReader ads(ads_path);
    adstack::Event e;
    KeyBits key(ng);
    while (ads.next(e)) {
      if (!matches_all(plan.where, e)) continue;
      for (size_t c = 0; c < ng; ++c) key[c] = event_field_bits(e, plan.group[c]);
      auto it = truth.find(key);
      if (it == truth.end()) it = truth.emplace(key, brute_initial(plan, e)).first;
      brute_update(plan, e, it->second);
    }
  }

  // 2. Every engine group must be in the truth with identical cells, and
  //    the counts must match (so nothing is missing either).
  if (t.rows() != truth.size()) {
    rep.detail = "group count differs: engine " + std::to_string(t.rows()) + ", brute force " + std::to_string(truth.size());
    rep.seconds = seconds_since(t0);
    return rep;
  }
  KeyBits key(ng);
  Cells cells(plan.aggs.size());
  for (size_t row = 0; row < t.rows(); ++row) {
    for (size_t c = 0; c < ng; ++c) key[c] = t.at(c, row);
    for (size_t a = 0; a < plan.aggs.size(); ++a) cells[a] = t.at(ng + a, row);
    const auto it = truth.find(key);
    if (it == truth.end()) {
      rep.detail = "engine group not found by brute force: " + row_text(t, key, cells);
      rep.seconds = seconds_since(t0);
      return rep;
    }
    if (it->second != cells) {
      rep.detail = "aggregates differ for group: engine " + row_text(t, key, cells) + " / brute force " +
                   row_text(t, key, it->second);
      rep.seconds = seconds_since(t0);
      return rep;
    }
  }
  rep.detail = std::to_string(truth.size()) + " groups identical";

  // 3. ORDER BY / LIMIT: the emitted rows must equal the first N rows of
  //    the brute-force table under a plain std::stable_sort with a
  //    straightforward comparator (no heap, no partial sort).
  const std::vector<OrderSpec> order = effective_order(plan, t.schema);
  if (!order.empty()) {
    std::vector<std::pair<KeyBits, Cells>> rows(truth.begin(), truth.end());
    auto value = [&](const std::pair<KeyBits, Cells>& x, size_t c) { return c < ng ? x.first[c] : x.second[c - ng]; };
    std::stable_sort(rows.begin(), rows.end(), [&](const auto& a, const auto& b) {
      for (const OrderSpec& o : order) {
        const uint64_t va = value(a, o.column), vb = value(b, o.column);
        if (va == vb) continue;
        const bool a_less = less_in_type(va, vb, t.schema[o.column].type);
        return o.desc ? !a_less : a_less;
      }
      return false;
    });
    const size_t want = (plan.limit == 0 || plan.limit > rows.size()) ? rows.size() : plan.limit;
    if (r.rows.size() != want) {
      rep.detail += "; emitted " + std::to_string(r.rows.size()) + " rows, expected " + std::to_string(want);
      rep.ok = false;
      rep.seconds = seconds_since(t0);
      return rep;
    }
    for (size_t i = 0; i < want; ++i) {
      const uint32_t row = r.rows[i];
      for (size_t c = 0; c < t.schema.size(); ++c) {
        if (t.at(c, row) != value(rows[i], c)) {
          rep.detail += "; output row " + std::to_string(i) + " differs: engine " + engine_row_text(t, row) +
                        " vs brute force " + row_text(t, rows[i].first, rows[i].second);
          rep.seconds = seconds_since(t0);
          return rep;
        }
      }
    }
    rep.detail += "; " + std::to_string(want) + " ordered output rows identical";
  }
  rep.ok = true;
  rep.seconds = seconds_since(t0);
  return rep;
}

}  // namespace colstore
