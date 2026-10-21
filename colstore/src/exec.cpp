// Scanner, filter and gather. See exec.h for the model and the reasons.

#include "src/exec.h"

#include <algorithm>
#include <cstdio>
#include <stdexcept>

namespace colstore {

// ---- Scanner --------------------------------------------------------------------

Scanner::Scanner(const ColReader& reader, Conjunction where, std::vector<ColumnId> project, uint32_t batch_rows,
                 size_t first_group, size_t group_stride)
    : reader_(reader), where_(std::move(where)), batch_rows_(batch_rows), next_group_(first_group),
      group_stride_(group_stride) {
  if (batch_rows_ == 0) throw std::invalid_argument("Scanner: batch_rows must be > 0");
  if (group_stride_ == 0) throw std::invalid_argument("Scanner: group_stride must be > 0");
  const Schema& schema = reader_.schema();
  projected_.assign(schema.size(), 0);
  cols_.resize(schema.size());
  for (ColumnId c : project) {
    if (column_index(c) >= schema.size()) throw std::invalid_argument("Scanner: projected column outside the schema");
    projected_[column_index(c)] = 1;
  }
  // A predicate carries the column it was parsed against; the file must
  // have that column at that index with that type, or the comparison would
  // be on the wrong bytes.
  for (const Predicate& p : where_) {
    if (column_index(p.col) >= schema.size() || spec_of(schema, p.col).type != p.type ||
        spec_of(schema, p.col).name != p.column_name) {
      throw std::invalid_argument("Scanner: predicate on '" + p.column_name + "' was built against a different schema");
    }
  }
  // Groups this scanner owns, so the per-thread totals add up to the file's.
  const size_t n = reader_.num_row_groups();
  stats_.row_groups_total = first_group >= n ? 0 : (n - first_group + group_stride_ - 1) / group_stride_;
}

bool Scanner::next(Batch& out) {
  // Find a row group that the zone maps do not rule out. A group with zero
  // rows (never produced by the writer, but legal) is treated as consumed.
  while (!group_loaded_) {
    if (next_group_ >= reader_.num_row_groups()) return false;
    const size_t g = next_group_;
    next_group_ += group_stride_;
    if (load_row_group(g)) {
      group_loaded_ = group_rows_ > 0;
      next_row_ = 0;
      out.row_group = g;
    }
  }

  out.first_row = next_row_;
  out.n = std::min(batch_rows_, group_rows_ - next_row_);
  out.cols.resize(cols_.size());
  for (size_t c = 0; c < cols_.size(); ++c) out.cols[c] = slice(column_id(c), next_row_);
  out.predicates = &compiled_;
  out.filtered = false;
  out.sel.clear();

  next_row_ += out.n;
  stats_.rows_scanned += out.n;
  if (next_row_ == group_rows_) group_loaded_ = false;
  return true;
}

bool Scanner::load_row_group(size_t g) {
  // 1. Zone maps. Any predicate that cannot match skips the whole group;
  //    any predicate that every row satisfies is dropped for this group,
  //    which also means its column need not be decoded unless projected.
  std::vector<const Predicate*> needed;
  for (const Predicate& p : where_) {
    const ChunkMeta& m = reader_.chunk(g, p.col);
    switch (zone_decision(p, m.min_bits, m.max_bits)) {
      case ZoneDecision::Skip: ++stats_.row_groups_skipped_zone; return false;
      case ZoneDecision::All: ++stats_.predicates_dropped; break;
      case ZoneDecision::Some: needed.push_back(&p); break;
    }
  }

  for (DecodedColumn& c : cols_) c.clear();
  compiled_.clear();
  group_rows_ = reader_.footer().row_groups[g].rows;

  // 2. Decode the filter columns and compile the predicates against them.
  //    This happens before the projected columns so that a constant missing
  //    from a chunk's dictionary ("country = 500" when the chunk only has
  //    356 and 840) skips the group before any projected bytes are read.
  for (const Predicate* p : needed) {
    if (!cols_[static_cast<size_t>(p->col)].loaded) decode_column(g, p->col);
    CompiledPredicate cp;
    if (!compile_predicate(*p, cp)) { ++stats_.row_groups_skipped_dict; return false; }
    compiled_.push_back(cp);
  }

  // 3. Projected columns not already decoded for filtering.
  for (size_t c = 0; c < cols_.size(); ++c) {
    if (projected_[c] && !cols_[c].loaded) decode_column(g, column_id(c));
  }
  ++stats_.row_groups_scanned;
  return true;
}

void Scanner::decode_column(size_t g, ColumnId col) {
  const ChunkMeta& m = reader_.chunk(g, col);
  const ColumnType type = spec_of(reader_.schema(), col).type;
  const std::vector<uint8_t> bytes = reader_.read_chunk_bytes(g, col);
  DecodedColumn& dc = cols_[static_cast<size_t>(col)];
  // Check the id before naming it as an Encoding, so the guard reads as
  // guarding the cast (the cast itself is well-defined either way: Encoding
  // has a fixed uint32_t underlying type).
  if (m.encoding >= kNumEncodings) throw std::runtime_error("unsupported encoding id " + std::to_string(m.encoding));
  const auto enc = static_cast<Encoding>(m.encoding);

  // Dictionary-coded chunks stay as codes: cheaper to filter (compare
  // small ints, no lookup) and no cheaper to expand later than now.
  if (enc == Encoding::Dict || enc == Encoding::RleDict) {
    Dictionary d = enc == Encoding::Dict ? decode_dict_parts(bytes.data(), bytes.size(), m.rows, type)
                                         : decode_rle_dict_parts(bytes.data(), bytes.size(), m.rows, type);
    dc.dict = std::move(d.values);
    dc.codes = std::move(d.codes);
    dc.on_codes = true;
  } else {
    dc.values = decode_chunk(enc, bytes.data(), bytes.size(), m.rows, type);
  }
  if ((dc.on_codes ? dc.codes.size() : dc.values.size()) != group_rows_) {
    throw std::runtime_error("chunk row count disagrees with row group");
  }
  dc.loaded = true;
  ++stats_.chunks_decoded;
  stats_.bytes_read += bytes.size();
}

bool Scanner::compile_predicate(const Predicate& p, CompiledPredicate& out) {
  const DecodedColumn& dc = cols_[static_cast<size_t>(p.col)];
  const ColumnType type = p.type;
  out.col = p.col;
  if (!dc.on_codes) {
    out.on_codes = false;
    out.flip = is_signed(type) ? (uint64_t{1} << 63) : 0;
    out.lo = p.lo_key;
    out.hi = p.hi_key;
    ++stats_.predicates_on_values;
    return true;
  }
  // Dictionary is sorted in the column's natural order, so the order keys
  // of its entries are ascending and the value range [lo, hi] becomes the
  // code range [first entry >= lo, last entry <= hi].
  auto key_less = [type](uint64_t bits, uint64_t key) { return order_key(bits, type) < key; };
  auto key_less_rev = [type](uint64_t key, uint64_t bits) { return key < order_key(bits, type); };
  const auto first = std::lower_bound(dc.dict.begin(), dc.dict.end(), p.lo_key, key_less);
  const auto last = std::upper_bound(dc.dict.begin(), dc.dict.end(), p.hi_key, key_less_rev);
  if (first >= last) return false;  // no dictionary entry inside the range: no row matches
  out.on_codes = true;
  out.flip = 0;
  out.lo = static_cast<uint64_t>(first - dc.dict.begin());
  out.hi = static_cast<uint64_t>(last - dc.dict.begin()) - 1;
  ++stats_.predicates_on_codes;
  return true;
}

ColumnVector Scanner::slice(ColumnId col, uint32_t first) const {
  const DecodedColumn& dc = cols_[static_cast<size_t>(col)];
  ColumnVector v;
  if (dc.on_codes) {
    v.codes = dc.codes.data() + first;
    v.dict = dc.dict.data();
  } else if (dc.loaded) {
    v.values = dc.values.data() + first;
  }
  return v;
}

// ---- filter -------------------------------------------------------------------------

namespace {

// Keeps the entries of sel[0, count) whose row passes `test`, in place,
// and returns the new count. Branch-free: the index is always written,
// the cursor advances only on a pass. At 10-90% selectivity a branch here
// mispredicts constantly; the unconditional store costs one cycle.
template <typename Test>
uint32_t refine(std::vector<uint32_t>& sel, uint32_t count, Test test) {
  uint32_t k = 0;
  for (uint32_t j = 0; j < count; ++j) {
    const uint32_t i = sel[j];
    sel[k] = i;
    k += test(i) ? 1u : 0u;
  }
  return k;
}

}  // namespace

void apply_filter(Batch& batch) {
  if (batch.predicates == nullptr || batch.predicates->empty()) {
    batch.filtered = false;
    return;
  }
  batch.sel.resize(batch.n);
  for (uint32_t i = 0; i < batch.n; ++i) batch.sel[i] = i;
  uint32_t count = batch.n;

  // Each predicate only looks at the rows the previous ones kept, so a
  // selective first term makes the rest nearly free.
  for (const CompiledPredicate& p : *batch.predicates) {
    const ColumnVector& cv = batch.cols[static_cast<size_t>(p.col)];
    const uint64_t lo = p.lo, span = p.hi - p.lo;
    if (p.on_codes) {
      const uint32_t* codes = cv.codes;
      count = refine(batch.sel, count, [=](uint32_t i) { return (uint64_t{codes[i]} - lo) <= span; });
    } else {
      const uint64_t* values = cv.values;
      const uint64_t flip = p.flip;
      count = refine(batch.sel, count, [=](uint32_t i) { return ((values[i] ^ flip) - lo) <= span; });
    }
    if (count == 0) break;
  }
  batch.sel.resize(count);
  batch.filtered = true;
}

// ---- project ------------------------------------------------------------------------

void gather(const Batch& batch, ColumnId col, std::vector<uint64_t>& out) {
  if (column_index(col) >= batch.cols.size()) throw std::invalid_argument("gather: column outside the batch's schema");
  const ColumnVector& cv = batch.cols[column_index(col)];
  if (!cv.loaded()) throw std::invalid_argument("gather: column not projected: index " + std::to_string(column_index(col)));
  const uint32_t n = batch.selected();
  out.resize(n);
  for (uint32_t k = 0; k < n; ++k) out[k] = cv.at(batch.row(k));
}

// ---- stats --------------------------------------------------------------------------

std::string describe(const QueryStats& q) {
  const ScanStats& s = q.scan;
  char buf[512];
  std::snprintf(buf, sizeof buf,
                "row groups: %zu total, %zu skipped by zone map, %zu skipped by dictionary, %zu scanned\n"
                "rows: %llu decoded, %llu passed; chunks decoded: %zu (%.1f MB read)\n"
                "predicates per group: %zu on dictionary codes, %zu on values, %zu dropped as always true\n"
                "time: %.3f s",
                s.row_groups_total, s.row_groups_skipped_zone, s.row_groups_skipped_dict, s.row_groups_scanned,
                static_cast<unsigned long long>(s.rows_scanned), static_cast<unsigned long long>(q.rows_passed),
                s.chunks_decoded, static_cast<double>(s.bytes_read) / 1e6, s.predicates_on_codes,
                s.predicates_on_values, s.predicates_dropped, q.seconds);
  return buf;
}

}  // namespace colstore
