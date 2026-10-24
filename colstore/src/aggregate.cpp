// Key packing, the aggregate-spec parser and the HashAggregate operator.
// See aggregate.h for the model.

#include "src/aggregate.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace colstore {

// ---- KeyPacker ----------------------------------------------------------------------

KeyPacker::KeyPacker(std::vector<ColumnId> cols, const Schema& schema) : cols_(std::move(cols)) {
  // First fit: each column goes into the first word with room for it, so
  // the order the columns are named in does not decide whether they fit.
  // Widths are 8/16/32/64 bits, and for those first fit succeeds whenever
  // the total is at most 128 bits (every reachable (bits used in word 0,
  // bits used in word 1) state was checked; NOTES.md, "Review fixes").
  // fields_ stays in column order, so pack_bits/unpack and the output keep
  // the order the user gave; only the placement inside the key differs.
  unsigned total = 0;
  for (ColumnId c : cols_) total += static_cast<unsigned>(8 * value_width(spec_of(schema, c).type));
  if (total > 128) {
    throw std::invalid_argument("group key needs " + std::to_string(total) + " bits, at most 128 fit (two 64-bit words; "
                                "u8/u16/u32 take 8/16/32 bits, u64/i64 64): group by fewer or narrower columns");
  }
  unsigned used[2] = {0, 0};  // bits used in each word
  for (ColumnId c : cols_) {
    const unsigned bits = static_cast<unsigned>(8 * value_width(spec_of(schema, c).type));
    const unsigned word = used[0] + bits <= 64 ? 0u : 1u;
    if (used[word] + bits > 64) throw std::logic_error("KeyPacker: first fit failed under 128 bits");  // cannot happen, see above
    const uint64_t mask = bits == 64 ? ~uint64_t{0} : (uint64_t{1} << bits) - 1;
    fields_.push_back(Field{c, word, used[word], mask});
    used[word] += bits;
  }
}

GroupKey KeyPacker::pack(const Batch& b, uint32_t row) const {
  GroupKey k;
  for (const Field& f : fields_) {
    const uint64_t v = b.cols[static_cast<size_t>(f.col)].at(row) & f.mask;
    (f.word == 0 ? k.lo : k.hi) |= v << f.shift;
  }
  return k;
}

GroupKey KeyPacker::pack_bits(const std::vector<uint64_t>& bits) const {
  GroupKey k;
  for (size_t i = 0; i < fields_.size(); ++i) {
    const Field& f = fields_[i];
    (f.word == 0 ? k.lo : k.hi) |= (bits[i] & f.mask) << f.shift;
  }
  return k;
}

void KeyPacker::unpack(const GroupKey& k, std::vector<uint64_t>& out) const {
  out.resize(fields_.size());
  for (size_t i = 0; i < fields_.size(); ++i) {
    const Field& f = fields_[i];
    out[i] = ((f.word == 0 ? k.lo : k.hi) >> f.shift) & f.mask;
  }
}

// ---- specs --------------------------------------------------------------------------

const char* agg_kind_name(AggKind k) {
  switch (k) {
    case AggKind::Count: return "count";
    case AggKind::Sum: return "sum";
    case AggKind::Min: return "min";
    case AggKind::Max: return "max";
    case AggKind::CountIf: return "count_if";
    case AggKind::SumIf: return "sum_if";
  }
  return "?";
}

ColumnType output_type(const AggSpec& a) {
  switch (a.kind) {
    case AggKind::Count:
    case AggKind::CountIf: return ColumnType::U64;
    case AggKind::Sum:
    case AggKind::SumIf: return is_signed(a.col_type) ? ColumnType::I64 : ColumnType::U64;
    case AggKind::Min:
    case AggKind::Max: return a.col_type;
  }
  return ColumnType::U64;
}

namespace {

std::string lower(std::string_view s) {
  std::string out(s);
  for (char& ch : out) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  return out;
}

std::string_view trim(std::string_view s) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
  return s;
}

// Splits on commas that are not inside parentheses, so
// "sum_if(cost_micros, event_type=1), count()" gives two pieces.
std::vector<std::string_view> split_top_level(std::string_view text) {
  std::vector<std::string_view> out;
  int depth = 0;
  size_t start = 0;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '(') ++depth;
    else if (text[i] == ')') --depth;
    else if (text[i] == ',' && depth == 0) {
      out.push_back(text.substr(start, i - start));
      start = i + 1;
    }
  }
  out.push_back(text.substr(start));
  return out;
}

ColumnId column_or_throw(std::string_view name, const char* where, const Schema& schema) {
  const auto col = find_column(schema, trim(name));
  if (!col) throw std::invalid_argument(std::string("unknown column in ") + where + ": '" + std::string(trim(name)) + "'");
  return *col;
}

// One --where term, reusing the --where parser; exactly one term allowed.
Predicate condition_or_throw(std::string_view text, const Schema& schema) {
  const Conjunction c = parse_where(text, schema);
  if (c.size() != 1) {
    throw std::invalid_argument("condition must be a single 'column op value' term: '" + std::string(trim(text)) + "'");
  }
  return c.front();
}

// A name for a condition that is legal inside a column name:
// "event_type=1" -> "event_type_eq_1".
std::string condition_slug(const Predicate& p) {
  const char* op = "?";
  switch (p.op) {
    case Op::Lt: op = "lt"; break;
    case Op::Le: op = "le"; break;
    case Op::Eq: op = "eq"; break;
    case Op::Ge: op = "ge"; break;
    case Op::Gt: op = "gt"; break;
    case Op::Between: op = "between"; break;
  }
  std::string s = p.column_name + "_" + op + "_" + format_value(p.type, p.value_bits);
  if (p.op == Op::Between) s += "_" + format_value(p.type, p.hi_bits);
  for (char& ch : s) if (ch == '-') ch = 'm';  // "cost_micros_lt_m5": no '-' in a name
  return s;
}

std::string default_name(const AggSpec& a) {
  switch (a.kind) {
    case AggKind::Count: return "count";
    case AggKind::Sum: return "sum_" + a.col_name;
    case AggKind::Min: return "min_" + a.col_name;
    case AggKind::Max: return "max_" + a.col_name;
    case AggKind::CountIf: return "count_if_" + condition_slug(*a.cond);
    case AggKind::SumIf: return "sum_if_" + a.col_name + "_" + condition_slug(*a.cond);
  }
  return "?";
}

bool valid_name(std::string_view s) {
  if (s.empty() || std::isdigit(static_cast<unsigned char>(s.front()))) return false;
  return std::all_of(s.begin(), s.end(), [](char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
  });
}

// Resolves the aggregated column and copies its type and name into the spec.
void set_column(AggSpec& a, std::string_view name, const char* func, const Schema& schema) {
  a.col = column_or_throw(name, func, schema);
  a.col_type = spec_of(schema, *a.col).type;
  a.col_name = spec_of(schema, *a.col).name;
}

AggSpec parse_one(std::string_view piece, const Schema& schema) {
  piece = trim(piece);
  if (piece.empty()) throw std::invalid_argument("empty aggregate in --agg list");

  // Optional trailing "AS name". The alias is the last whitespace-separated
  // token after a closing parenthesis, preceded by "as".
  std::string alias;
  const size_t close = piece.rfind(')');
  if (close == std::string_view::npos) throw std::invalid_argument("aggregate needs parentheses: '" + std::string(piece) + "'");
  std::string_view tail = trim(piece.substr(close + 1));
  if (!tail.empty()) {
    const size_t sp = tail.find_first_of(" \t");
    if (sp == std::string_view::npos || lower(tail.substr(0, sp)) != "as") {
      throw std::invalid_argument("expected 'AS name' after aggregate, got '" + std::string(tail) + "'");
    }
    alias = std::string(trim(tail.substr(sp + 1)));
    if (!valid_name(alias)) throw std::invalid_argument("bad alias '" + alias + "': letters, digits and _ only");
  }
  const size_t open = piece.find('(');
  if (open == std::string_view::npos || open > close) throw std::invalid_argument("aggregate needs parentheses: '" + std::string(piece) + "'");
  const std::string func = lower(trim(piece.substr(0, open)));
  const std::string_view args = trim(piece.substr(open + 1, close - open - 1));

  AggSpec a;
  if (func == "count") {
    a.kind = AggKind::Count;
    if (!args.empty() && args != "*") throw std::invalid_argument("count() takes no argument (got '" + std::string(args) + "')");
  } else if (func == "sum" || func == "min" || func == "max") {
    a.kind = func == "sum" ? AggKind::Sum : func == "min" ? AggKind::Min : AggKind::Max;
    set_column(a, args, func.c_str(), schema);
  } else if (func == "count_if") {
    a.kind = AggKind::CountIf;
    a.cond = condition_or_throw(args, schema);
  } else if (func == "sum_if") {
    a.kind = AggKind::SumIf;
    const size_t comma = args.find(',');
    if (comma == std::string_view::npos) throw std::invalid_argument("sum_if needs (column, condition): '" + std::string(args) + "'");
    set_column(a, args.substr(0, comma), "sum_if", schema);
    a.cond = condition_or_throw(args.substr(comma + 1), schema);
  } else {
    throw std::invalid_argument("unknown aggregate '" + func + "' (count, sum, min, max, count_if, sum_if)");
  }
  a.name = alias.empty() ? default_name(a) : alias;
  return a;
}

}  // namespace

std::vector<AggSpec> parse_aggs(std::string_view text, const Schema& schema) {
  if (trim(text).empty()) throw std::invalid_argument("--agg needs at least one aggregate, e.g. count()");
  std::vector<AggSpec> out;
  for (std::string_view piece : split_top_level(text)) out.push_back(parse_one(piece, schema));
  return out;
}

std::string to_string(const AggSpec& a) {
  std::string s = std::string(agg_kind_name(a.kind)) + "(";
  if (a.col) s += a.col_name;
  if (a.cond) s += (a.col ? ", " : "") + to_string(*a.cond);
  return s + ") AS " + a.name;
}

std::vector<OutputColumn> output_columns(const std::vector<ColumnId>& group, const std::vector<AggSpec>& aggs,
                                         const Schema& schema) {
  std::vector<OutputColumn> out;
  for (ColumnId c : group) out.push_back({spec_of(schema, c).name, spec_of(schema, c).type});
  for (const AggSpec& a : aggs) out.push_back({a.name, output_type(a)});
  for (size_t i = 0; i < out.size(); ++i) {
    for (size_t j = 0; j < i; ++j) {
      if (out[i].name == out[j].name) throw std::invalid_argument("duplicate output column '" + out[i].name + "': use AS to rename");
    }
  }
  return out;
}

std::string format_value(ColumnType type, uint64_t bits) {
  return is_signed(type) ? std::to_string(as_signed(bits)) : std::to_string(bits);
}

std::string format_cell(const ResultTable& t, size_t col, size_t row) {
  return t.is_null(col, row) ? std::string() : format_value(t.schema[col].type, t.at(col, row));
}

bool null_when_empty(AggKind k) { return k != AggKind::Count && k != AggKind::CountIf; }

// ---- HashAggregate -------------------------------------------------------------------

HashAggregate::HashAggregate(std::vector<ColumnId> group, std::vector<AggSpec> aggs, const Schema& schema)
    : schema_(schema), packer_(std::move(group), schema), aggs_(std::move(aggs)) {
  if (aggs_.empty()) throw std::invalid_argument("HashAggregate: at least one aggregate");
  for (const AggSpec& a : aggs_) {
    Column c;
    if (a.col) {
      // The spec's copied type must be the schema's: a spec parsed for
      // another table would otherwise sum the wrong column's bytes.
      if (spec_of(schema_, *a.col).type != a.col_type || spec_of(schema_, *a.col).name != a.col_name) {
        throw std::invalid_argument("aggregate on '" + a.col_name + "' was built against a different schema");
      }
      c.col_index = static_cast<size_t>(*a.col);
      c.type = a.col_type;
    }
    if (a.cond) {
      if (spec_of(schema_, a.cond->col).type != a.cond->type || spec_of(schema_, a.cond->col).name != a.cond->column_name) {
        throw std::invalid_argument("condition on '" + a.cond->column_name + "' was built against a different schema");
      }
      c.cond_index = static_cast<size_t>(a.cond->col);
    }
    agg_cols_.push_back(c);
  }
  // Global aggregate: create group 0 now, so it exists (and is emitted)
  // even if no row ever arrives, and so consume() never has to hash.
  global_ = packer_.columns().empty();
  if (global_) {
    bool inserted = false;
    table_.find_or_insert(GroupKey{}, &inserted);
    for (const AggSpec& a : aggs_) state_.push_back(initial_state(a));
  }
}

std::vector<ColumnId> HashAggregate::columns_needed() const {
  std::vector<ColumnId> out = packer_.columns();
  auto add = [&](ColumnId c) { if (std::find(out.begin(), out.end(), c) == out.end()) out.push_back(c); };
  for (const AggSpec& a : aggs_) {
    if (a.col) add(*a.col);
    if (a.cond) add(a.cond->col);
  }
  return out;
}

uint64_t HashAggregate::initial_state(const AggSpec& a) {
  switch (a.kind) {
    case AggKind::Min: return ~uint64_t{0};  // larger than every order key
    case AggKind::Max: return 0;             // smaller than every order key
    default: return 0;
  }
}

void HashAggregate::assign_groups(const Batch& b) {
  const uint32_t n = b.selected();
  if (global_) {  // one group, already created: no key to pack, nothing to probe
    group_of_.assign(n, 0);
    return;
  }
  group_of_.resize(n);
  const size_t na = aggs_.size();
  for (uint32_t k = 0; k < n; ++k) {
    bool inserted = false;
    const uint32_t g = table_.find_or_insert(packer_.pack(b, b.row(k)), &inserted);
    if (inserted) {
      // New group: its cells are appended at the end, in agg order.
      for (size_t a = 0; a < na; ++a) state_.push_back(initial_state(aggs_[a]));
    }
    group_of_[k] = g;
  }
}

void HashAggregate::update(size_t a, const Batch& b) {
  const uint32_t n = b.selected();
  const size_t na = aggs_.size();
  const Column& c = agg_cols_[a];
  const ColumnVector& cv = b.cols[c.col_index];
  uint64_t* const state = state_.data() + a;  // cell(g, a) == state[g * na]

  switch (aggs_[a].kind) {
    case AggKind::Count:
      if (global_) {
        state[0] += n;  // every selected row is in group 0
      } else {
        for (uint32_t k = 0; k < n; ++k) state[group_of_[k] * na] += 1;
      }
      break;
    case AggKind::Sum:
      for (uint32_t k = 0; k < n; ++k) state[group_of_[k] * na] += cv.at(b.row(k));
      break;
    case AggKind::Min:
      for (uint32_t k = 0; k < n; ++k) {
        const uint64_t key = order_key(cv.at(b.row(k)), c.type);
        uint64_t& s = state[group_of_[k] * na];
        if (key < s) s = key;
      }
      break;
    case AggKind::Max:
      for (uint32_t k = 0; k < n; ++k) {
        const uint64_t key = order_key(cv.at(b.row(k)), c.type);
        uint64_t& s = state[group_of_[k] * na];
        if (key > s) s = key;
      }
      break;
    case AggKind::CountIf: {
      const Predicate& cond = *aggs_[a].cond;
      const ColumnVector& cc = b.cols[c.cond_index];
      // Branch-free add of 0 or 1, same reasoning as the filter's selection
      // vector: at mixed selectivity a branch here mispredicts.
      for (uint32_t k = 0; k < n; ++k) state[group_of_[k] * na] += cond.matches(cc.at(b.row(k))) ? 1u : 0u;
      break;
    }
    case AggKind::SumIf: {
      const Predicate& cond = *aggs_[a].cond;
      const ColumnVector& cc = b.cols[c.cond_index];
      for (uint32_t k = 0; k < n; ++k) {
        const uint32_t row = b.row(k);
        state[group_of_[k] * na] += cond.matches(cc.at(row)) ? cv.at(row) : 0;
      }
      break;
    }
  }
}

void HashAggregate::consume(const Batch& b) {
  const uint32_t n = b.selected();
  if (n == 0) return;
  for (ColumnId c : columns_needed()) {
    if (column_index(c) >= b.cols.size() || !b.cols[column_index(c)].loaded()) {
      throw std::invalid_argument("HashAggregate: column not loaded in batch: " + spec_of(schema_, c).name);
    }
  }
  assign_groups(b);
  for (size_t a = 0; a < aggs_.size(); ++a) update(a, b);
  rows_ += n;
  ++batches_;
}

void HashAggregate::combine(uint32_t group, size_t a, uint64_t other) {
  uint64_t& s = cell(group, a);
  switch (aggs_[a].kind) {
    case AggKind::Count:
    case AggKind::CountIf:
    case AggKind::Sum:
    case AggKind::SumIf: s += other; break;
    case AggKind::Min: if (other < s) s = other; break;
    case AggKind::Max: if (other > s) s = other; break;
  }
}

void HashAggregate::merge(const HashAggregate& other) {
  if (other.aggs_ != aggs_ || other.packer_.columns() != packer_.columns()) {
    throw std::invalid_argument("HashAggregate::merge: different plans");
  }
  const size_t na = aggs_.size();
  for (uint32_t g2 = 0; g2 < other.table_.size(); ++g2) {
    bool inserted = false;
    const uint32_t g = table_.find_or_insert(other.table_.key(g2), &inserted);
    if (inserted) {
      for (size_t a = 0; a < na; ++a) state_.push_back(initial_state(aggs_[a]));
    }
    for (size_t a = 0; a < na; ++a) combine(g, a, other.cell(g2, a));
  }
  rows_ += other.rows_;
  batches_ += other.batches_;
  merged_probes_ += other.table_.probes() + other.merged_probes_;
  merged_grows_ += other.table_.grows() + other.merged_grows_;
}

ResultTable HashAggregate::finish() const {
  ResultTable t;
  t.schema = output_columns(packer_.columns(), aggs_, schema_);
  t.cols.assign(t.schema.size(), {});
  const size_t ng = packer_.columns().size();
  const uint32_t groups = table_.size();
  for (auto& c : t.cols) c.reserve(groups);

  // A global aggregate that consumed no row is SQL's one row over an empty
  // input: counts stay 0, and sum / min / max have no value (NULL).
  if (global_ && rows_ == 0) {
    t.nulls.assign(t.schema.size(), std::vector<uint8_t>(groups, 0));
    for (size_t a = 0; a < aggs_.size(); ++a) {
      if (null_when_empty(aggs_[a].kind)) t.nulls[ng + a][0] = 1;
    }
  }
  std::vector<uint64_t> key_bits;
  for (uint32_t g = 0; g < groups; ++g) {
    packer_.unpack(table_.key(g), key_bits);
    for (size_t i = 0; i < ng; ++i) t.cols[i].push_back(key_bits[i]);
    for (size_t a = 0; a < aggs_.size(); ++a) {
      uint64_t v = cell(g, a);
      if (aggs_[a].kind == AggKind::Min || aggs_[a].kind == AggKind::Max) {
        v = order_key(v, agg_cols_[a].type);  // order_key is its own inverse (xor)
      }
      if (t.is_null(ng + a, g)) v = 0;  // NULL cells carry pattern 0, not the initial state
      t.cols[ng + a].push_back(v);
    }
  }
  return t;
}

AggStats HashAggregate::stats() const {
  AggStats s;
  s.rows = rows_;
  s.batches = batches_;
  s.groups = table_.size();
  s.slots = table_.slots();
  s.grows = table_.grows() + merged_grows_;
  s.probes = table_.probes() + merged_probes_;
  return s;
}

}  // namespace colstore
