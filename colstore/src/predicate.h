#pragma once
// Predicates for the scan: "column op constant" terms ANDed together, a
// tiny parser for the CLI's --where string, and the zone-map decision that
// lets the scanner skip a whole row group from its min/max alone.
//
// The central trick is that every supported operator (<, <=, =, >=, >,
// BETWEEN) is turned into ONE closed range [lo, hi] over an "order key":
// the column's 64-bit value pattern remapped so that plain unsigned
// comparison gives the column's natural order (for i64 columns that means
// flipping the sign bit; for unsigned columns it is the identity). After
// that, the filter loop, the zone map and the dictionary-code mapping all
// have exactly one case to handle instead of six, and "does x match" is a
// single subtract-and-compare with no branches on the operator.
//
// This file is deliberately free of I/O: it is unit tested on its own.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/event.h"
#include "src/format.h"

namespace colstore {

enum class Op { Lt, Le, Eq, Ge, Gt, Between };
const char* op_name(Op op);

// Maps a column's raw 64-bit pattern to a key whose UNSIGNED order is the
// column's natural order. For unsigned columns the pattern already is; for
// i64, xor-ing the sign bit moves INT64_MIN to 0 and INT64_MAX to
// UINT64_MAX while keeping every "<" relation intact.
inline uint64_t order_key(uint64_t bits, ColumnType type) {
  return is_signed(type) ? (bits ^ (uint64_t{1} << 63)) : bits;
}
// Largest order key a value of this type can have (smallest is always 0).
uint64_t max_order_key(ColumnType type);

struct Predicate {
  ColumnId col = ColumnId::EventId;
  Op op = Op::Eq;
  uint64_t value_bits = 0;  // the constant (BETWEEN: the lower bound), raw pattern
  uint64_t hi_bits = 0;     // BETWEEN only: the upper bound, raw pattern

  // Closed range in order-key space; empty when lo_key > hi_key (e.g.
  // "x < 0" on an unsigned column). Filled by make_predicate().
  uint64_t lo_key = 0;
  uint64_t hi_key = 0;

  bool empty() const { return lo_key > hi_key; }
  // One subtraction and one compare: k is inside [lo, hi] iff k - lo does
  // not wrap past hi - lo. Only valid when !empty().
  bool contains_key(uint64_t k) const { return (k - lo_key) <= (hi_key - lo_key); }
  bool matches(uint64_t bits) const {
    return !empty() && contains_key(order_key(bits, column_spec(col).type));
  }
  bool operator==(const Predicate&) const = default;
};

// Builds a predicate and its order-key range. For BETWEEN, `hi_bits` is the
// upper bound; for every other op it is ignored.
Predicate make_predicate(ColumnId col, Op op, uint64_t value_bits, uint64_t hi_bits = 0);

// A conjunction. Evaluated as "every term matches".
using Conjunction = std::vector<Predicate>;
bool matches_all(const Conjunction& where, const adstack::Event& e);

// Raw 64-bit pattern of one field of an Event (the same widening the writer
// applies), so the brute-force check in `verify` sees the bits the scanner
// sees.
uint64_t event_field_bits(const adstack::Event& e, ColumnId col);

// ---- zone maps ------------------------------------------------------------

// What a chunk's stored min/max say about a predicate before any value is
// decoded: no row can match (skip the group), every row matches (drop the
// predicate for this group), or we have to look.
enum class ZoneDecision { Skip, All, Some };
const char* zone_decision_name(ZoneDecision d);
ZoneDecision zone_decision(const Predicate& p, uint64_t min_bits, uint64_t max_bits);

// ---- parser -----------------------------------------------------------------

// Grammar (whitespace-separated or not; keywords case-insensitive):
//
//   where   := term ( AND term )*
//   term    := column op integer | column BETWEEN integer AND integer
//   op      := < | <= | = | == | >= | >
//   integer := -?[0-9]+   (must fit the column's type, else an error)
//
// e.g. "country=840 AND event_time_us BETWEEN 1767225600000000 AND 1767225660000000".
// Throws std::invalid_argument with a message that names the offending
// token. An empty or all-blank string parses to an empty conjunction.
Conjunction parse_where(std::string_view text);

// Human-readable form, e.g. "country = 840", for --stats output and tests.
std::string to_string(const Predicate& p);

}  // namespace colstore
