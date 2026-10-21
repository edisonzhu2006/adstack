// ORDER BY / LIMIT. See sort.h for why a permutation and why partial_sort.

#include "src/sort.h"

#include <algorithm>
#include <cctype>
#include <numeric>
#include <stdexcept>
#include <string>

#include "src/predicate.h"  // order_key

namespace colstore {

namespace {

std::string_view trim(std::string_view s) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
  return s;
}

std::string lower(std::string_view s) {
  std::string out(s);
  for (char& ch : out) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  return out;
}

}  // namespace

std::vector<OrderSpec> parse_order(std::string_view text, const std::vector<OutputColumn>& schema) {
  std::vector<OrderSpec> out;
  size_t start = 0;
  while (start <= text.size()) {
    size_t comma = text.find(',', start);
    if (comma == std::string_view::npos) comma = text.size();
    const std::string_view piece = trim(text.substr(start, comma - start));
    start = comma + 1;
    if (piece.empty()) {
      if (trim(text).empty()) break;  // "" parses to no ordering
      throw std::invalid_argument("empty term in --order");
    }
    // "name [asc|desc]"
    const size_t sp = piece.find_first_of(" \t");
    const std::string_view name = sp == std::string_view::npos ? piece : piece.substr(0, sp);
    const std::string dir = sp == std::string_view::npos ? "asc" : lower(trim(piece.substr(sp + 1)));
    OrderSpec o;
    if (dir == "asc") o.desc = false;
    else if (dir == "desc") o.desc = true;
    else throw std::invalid_argument("--order direction must be asc or desc, got '" + dir + "'");
    const auto it = std::find_if(schema.begin(), schema.end(), [&](const OutputColumn& c) { return c.name == name; });
    if (it == schema.end()) {
      std::string known;
      for (const OutputColumn& c : schema) known += (known.empty() ? "" : ", ") + c.name;
      throw std::invalid_argument("--order: unknown output column '" + std::string(name) + "' (have: " + known + ")");
    }
    o.column = static_cast<size_t>(it - schema.begin());
    out.push_back(o);
  }
  return out;
}

std::string to_string(const OrderSpec& o, const std::vector<OutputColumn>& schema) {
  return schema[o.column].name + (o.desc ? " desc" : " asc");
}

std::vector<uint32_t> sort_limit(const ResultTable& table, const std::vector<OrderSpec>& order, size_t limit) {
  const size_t n = table.rows();
  std::vector<uint32_t> idx(n);
  std::iota(idx.begin(), idx.end(), 0u);
  const size_t keep = (limit == 0 || limit > n) ? n : limit;

  if (!order.empty()) {
    // Compare on order keys: unsigned compare of the key is the column's
    // natural order for signed and unsigned columns alike (predicate.h).
    auto less = [&](uint32_t a, uint32_t b) {
      for (const OrderSpec& o : order) {
        const ColumnType type = table.schema[o.column].type;
        const uint64_t ka = order_key(table.at(o.column, a), type);
        const uint64_t kb = order_key(table.at(o.column, b), type);
        if (ka != kb) return o.desc ? ka > kb : ka < kb;
      }
      return a < b;  // deterministic tiebreak
    };
    if (keep < n) {
      std::partial_sort(idx.begin(), idx.begin() + static_cast<std::ptrdiff_t>(keep), idx.end(), less);
    } else {
      std::sort(idx.begin(), idx.end(), less);
    }
  }
  idx.resize(keep);
  return idx;
}

}  // namespace colstore
