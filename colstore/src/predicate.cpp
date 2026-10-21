// Predicate construction, zone-map decisions and the --where parser.
// See predicate.h for the order-key idea that makes all of this one case.

#include "src/predicate.h"

#include <cctype>
#include <charconv>
#include <stdexcept>

#include "src/encoding.h"  // widen()

namespace colstore {

const char* op_name(Op op) {
  switch (op) {
    case Op::Lt: return "<";
    case Op::Le: return "<=";
    case Op::Eq: return "=";
    case Op::Ge: return ">=";
    case Op::Gt: return ">";
    case Op::Between: return "BETWEEN";
  }
  return "?";
}

uint64_t max_order_key(ColumnType type) {
  // i64: key(INT64_MAX) = UINT64_MAX. u64: UINT64_MAX. Narrower unsigned:
  // all ones in the low 8*width bits.
  const size_t w = value_width(type);
  if (w >= 8) return ~uint64_t{0};
  return (uint64_t{1} << (8 * w)) - 1;
}

Predicate make_predicate(ColumnId col, Op op, uint64_t value_bits, uint64_t hi_bits) {
  Predicate p;
  p.col = col;
  p.op = op;
  p.value_bits = value_bits;
  p.hi_bits = op == Op::Between ? hi_bits : 0;

  const ColumnType type = column_spec(col).type;
  const uint64_t k = order_key(value_bits, type);
  const uint64_t top = max_order_key(type);
  // The "empty" representation is lo = 1, hi = 0. It is needed at the type
  // edges: "x < type_min" and "x > type_max" match nothing, and k - 1 or
  // k + 1 would wrap there.
  switch (op) {
    case Op::Lt:
      if (k == 0) { p.lo_key = 1; p.hi_key = 0; } else { p.lo_key = 0; p.hi_key = k - 1; }
      break;
    case Op::Le: p.lo_key = 0; p.hi_key = k; break;
    case Op::Eq: p.lo_key = k; p.hi_key = k; break;
    case Op::Ge: p.lo_key = k; p.hi_key = top; break;
    case Op::Gt:
      if (k == top) { p.lo_key = 1; p.hi_key = 0; } else { p.lo_key = k + 1; p.hi_key = top; }
      break;
    case Op::Between:
      p.lo_key = k;
      p.hi_key = order_key(hi_bits, type);  // lo > hi is naturally empty
      break;
  }
  return p;
}

uint64_t event_field_bits(const adstack::Event& e, ColumnId col) {
  switch (col) {
    case ColumnId::EventId: return widen(e.event_id);
    case ColumnId::EventTimeUs: return widen(e.event_time_us);
    case ColumnId::IngestTimeUs: return widen(e.ingest_time_us);
    case ColumnId::EventType: return widen(e.event_type);
    case ColumnId::AdvertiserId: return widen(e.advertiser_id);
    case ColumnId::CampaignId: return widen(e.campaign_id);
    case ColumnId::VideoId: return widen(e.video_id);
    case ColumnId::Country: return widen(e.country);
    case ColumnId::Device: return widen(e.device);
    case ColumnId::CostMicros: return widen(e.cost_micros);
  }
  throw std::invalid_argument("event_field_bits: bad column id");
}

bool matches_all(const Conjunction& where, const adstack::Event& e) {
  for (const Predicate& p : where) {
    if (!p.matches(event_field_bits(e, p.col))) return false;
  }
  return true;
}

// ---- zone maps ------------------------------------------------------------

const char* zone_decision_name(ZoneDecision d) {
  switch (d) {
    case ZoneDecision::Skip: return "skip";
    case ZoneDecision::All: return "all";
    case ZoneDecision::Some: return "some";
  }
  return "?";
}

ZoneDecision zone_decision(const Predicate& p, uint64_t min_bits, uint64_t max_bits) {
  if (p.empty()) return ZoneDecision::Skip;
  const ColumnType type = column_spec(p.col).type;
  const uint64_t kmin = order_key(min_bits, type);
  const uint64_t kmax = order_key(max_bits, type);
  // Two intervals on a line: [kmin, kmax] is the chunk, [lo, hi] is the
  // predicate. Disjoint means no row can match; contained means every row
  // matches; anything else needs the data.
  if (kmax < p.lo_key || kmin > p.hi_key) return ZoneDecision::Skip;
  if (p.lo_key <= kmin && kmax <= p.hi_key) return ZoneDecision::All;
  return ZoneDecision::Some;
}

// ---- parser -----------------------------------------------------------------

namespace {

// Token kinds are few enough that a string plus a kind is clearer than a
// variant. Numbers are kept as text and parsed against the column type.
enum class Tok { Ident, Number, Op, End };

struct Token {
  Tok kind;
  std::string text;
};

bool ieq(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(a[i])) != std::toupper(static_cast<unsigned char>(b[i]))) return false;
  }
  return true;
}

std::vector<Token> tokenize(std::string_view s) {
  std::vector<Token> out;
  size_t i = 0;
  while (i < s.size()) {
    const char c = s[i];
    if (std::isspace(static_cast<unsigned char>(c))) { ++i; continue; }
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
      size_t j = i;
      while (j < s.size() && (std::isalnum(static_cast<unsigned char>(s[j])) || s[j] == '_')) ++j;
      out.push_back({Tok::Ident, std::string(s.substr(i, j - i))});
      i = j;
      continue;
    }
    // A '-' starts a number only when a digit follows; there is no binary
    // minus in this grammar, so that is the only meaning it can have.
    if (std::isdigit(static_cast<unsigned char>(c)) ||
        (c == '-' && i + 1 < s.size() && std::isdigit(static_cast<unsigned char>(s[i + 1])))) {
      size_t j = i + 1;
      while (j < s.size() && std::isdigit(static_cast<unsigned char>(s[j]))) ++j;
      out.push_back({Tok::Number, std::string(s.substr(i, j - i))});
      i = j;
      continue;
    }
    if (c == '<' || c == '>' || c == '=') {
      // Two-character forms first: <=, >=, ==.
      if (i + 1 < s.size() && s[i + 1] == '=') {
        out.push_back({Tok::Op, std::string(s.substr(i, 2))});
        i += 2;
      } else {
        out.push_back({Tok::Op, std::string(1, c)});
        ++i;
      }
      continue;
    }
    throw std::invalid_argument(std::string("where: unexpected character '") + c + "' at offset " +
                                std::to_string(i));
  }
  out.push_back({Tok::End, ""});
  return out;
}

Op op_from_text(const std::string& t) {
  if (t == "<") return Op::Lt;
  if (t == "<=") return Op::Le;
  if (t == "=" || t == "==") return Op::Eq;
  if (t == ">=") return Op::Ge;
  if (t == ">") return Op::Gt;
  throw std::invalid_argument("where: unknown operator '" + t + "'");
}

// Parses an integer literal and checks it fits the column: unsigned columns
// reject negatives and anything above their width; i64 rejects values past
// INT64_MAX. A constant that cannot be a column value is far more likely a
// typo than a deliberate "match nothing", so it is an error, not an empty
// range.
uint64_t parse_literal(const std::string& text, const ColumnSpec& spec) {
  const char* b = text.data();
  const char* e = b + text.size();
  auto fail = [&](const char* why) {
    throw std::invalid_argument("where: literal " + text + " " + why + " for column " + spec.name + " (" +
                                column_type_name(spec.type) + ")");
  };
  if (is_signed(spec.type)) {
    int64_t v = 0;
    const auto r = std::from_chars(b, e, v);
    if (r.ec != std::errc{} || r.ptr != e) fail("does not fit");
    return widen(v);
  }
  if (!text.empty() && text[0] == '-') fail("is negative");
  uint64_t v = 0;
  const auto r = std::from_chars(b, e, v);
  if (r.ec != std::errc{} || r.ptr != e) fail("does not fit");
  if (v > max_order_key(spec.type)) fail("does not fit");
  return v;
}

class Parser {
 public:
  explicit Parser(std::string_view text) : toks_(tokenize(text)) {}

  Conjunction parse() {
    Conjunction out;
    if (peek().kind == Tok::End) return out;
    out.push_back(term());
    while (peek().kind != Tok::End) {
      expect_keyword("AND");
      out.push_back(term());
    }
    return out;
  }

 private:
  const Token& peek() const { return toks_[pos_]; }
  Token take() { return toks_[pos_++]; }

  void expect_keyword(const char* kw) {
    const Token t = take();
    if (t.kind != Tok::Ident || !ieq(t.text, kw)) {
      throw std::invalid_argument(std::string("where: expected ") + kw + " but found '" + t.text + "'");
    }
  }

  Predicate term() {
    const Token name = take();
    if (name.kind != Tok::Ident) throw std::invalid_argument("where: expected a column name, found '" + name.text + "'");
    const auto col = column_by_name(name.text);
    if (!col) throw std::invalid_argument("where: unknown column '" + name.text + "'");
    const ColumnSpec& spec = column_spec(*col);

    const Token op = take();
    if (op.kind == Tok::Ident && ieq(op.text, "BETWEEN")) {
      const uint64_t lo = parse_literal(number(), spec);
      expect_keyword("AND");
      const uint64_t hi = parse_literal(number(), spec);
      return make_predicate(*col, Op::Between, lo, hi);
    }
    if (op.kind != Tok::Op) throw std::invalid_argument("where: expected an operator after " + name.text + ", found '" + op.text + "'");
    return make_predicate(*col, op_from_text(op.text), parse_literal(number(), spec));
  }

  std::string number() {
    const Token t = take();
    if (t.kind != Tok::Number) throw std::invalid_argument("where: expected an integer, found '" + t.text + "'");
    return t.text;
  }

  std::vector<Token> toks_;
  size_t pos_ = 0;
};

std::string format_literal(uint64_t bits, ColumnType type) {
  return is_signed(type) ? std::to_string(as_signed(bits)) : std::to_string(bits);
}

}  // namespace

Conjunction parse_where(std::string_view text) { return Parser(text).parse(); }

std::string to_string(const Predicate& p) {
  const ColumnSpec& spec = column_spec(p.col);
  std::string s = std::string(spec.name) + " " + op_name(p.op) + " " + format_literal(p.value_bits, spec.type);
  if (p.op == Op::Between) s += " AND " + format_literal(p.hi_bits, spec.type);
  return s;
}

}  // namespace colstore
