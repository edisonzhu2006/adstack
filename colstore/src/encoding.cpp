// DICT, RLE_DICT and DELTA_BP encoders/decoders and the per-chunk selector.
// Layouts are documented in encoding.h; this file is the executable
// version. Every decoder reads through a bounds-checked cursor so a corrupt
// or truncated chunk throws rather than reading out of bounds.

#include "src/encoding.h"

#include <array>
#include <climits>
#include <string>
#include <unordered_map>

#include "src/bitpack.h"

namespace colstore {

namespace {

// ---- byte-level helpers ---------------------------------------------------

// Appends `bits` at the column's natural width (1, 2, 4 or 8 bytes).
void put_value(std::vector<uint8_t>& out, uint64_t bits, ColumnType type) {
  uint8_t tmp[8];
  uint8_t* p = tmp;
  switch (type) {
    case ColumnType::U8: put_le(p, static_cast<uint8_t>(bits)); break;
    case ColumnType::U16: put_le(p, static_cast<uint16_t>(bits)); break;
    case ColumnType::U32: put_le(p, static_cast<uint32_t>(bits)); break;
    case ColumnType::U64:
    case ColumnType::I64: put_le(p, bits); break;
  }
  out.insert(out.end(), tmp, p);
}

void put_u8(std::vector<uint8_t>& out, uint8_t v) { out.push_back(v); }
void put_u32(std::vector<uint8_t>& out, uint32_t v) {
  uint8_t tmp[4];
  uint8_t* p = tmp;
  put_le(p, v);
  out.insert(out.end(), tmp, tmp + 4);
}
void put_u64(std::vector<uint8_t>& out, uint64_t v) {
  uint8_t tmp[8];
  uint8_t* p = tmp;
  put_le(p, v);
  out.insert(out.end(), tmp, tmp + 8);
}

// Sequential reader over [data, data+len) that refuses to run past the end.
class Cursor {
 public:
  Cursor(const uint8_t* data, size_t len) : p_(data), end_(data + len) {}

  size_t remaining() const { return static_cast<size_t>(end_ - p_); }
  const uint8_t* ptr() const { return p_; }

  void need(size_t bytes, const char* what) const {
    if (bytes > remaining()) throw std::runtime_error(std::string("corrupt chunk: truncated ") + what);
  }
  void skip(size_t bytes, const char* what) {
    need(bytes, what);
    p_ += bytes;
  }
  void expect_end(const char* what) const {
    if (remaining() != 0) throw std::runtime_error(std::string("corrupt chunk: trailing bytes after ") + what);
  }

  uint8_t u8(const char* what) { need(1, what); return get_le<uint8_t>(p_); }
  uint32_t u32(const char* what) { need(4, what); return get_le<uint32_t>(p_); }
  uint64_t u64(const char* what) { need(8, what); return get_le<uint64_t>(p_); }

  // Reads one value at the column's natural width, widened like widen().
  uint64_t value(ColumnType type, const char* what) {
    need(value_width(type), what);
    switch (type) {
      case ColumnType::U8: return get_le<uint8_t>(p_);
      case ColumnType::U16: return get_le<uint16_t>(p_);
      case ColumnType::U32: return get_le<uint32_t>(p_);
      case ColumnType::U64: return get_le<uint64_t>(p_);
      case ColumnType::I64: return static_cast<uint64_t>(get_le<int64_t>(p_));
    }
    throw std::logic_error("unknown column type");
  }

 private:
  const uint8_t* p_;
  const uint8_t* end_;
};

void check_count(uint32_t in_chunk, uint32_t rows) {
  if (in_chunk != rows) throw std::runtime_error("corrupt chunk: value count does not match footer");
}

// Reads the "u32 d, d values" dictionary prefix shared by DICT and RLE_DICT.
std::vector<uint64_t> read_dictionary(Cursor& c, ColumnType type) {
  const uint32_t d = c.u32("dictionary size");
  c.need(static_cast<size_t>(d) * value_width(type), "dictionary");
  std::vector<uint64_t> dict(d);
  for (uint32_t i = 0; i < d; ++i) dict[i] = c.value(type, "dictionary");
  return dict;
}

void write_dictionary(const Dictionary& dict, ColumnType type, std::vector<uint8_t>& out) {
  put_u32(out, static_cast<uint32_t>(dict.values.size()));
  for (uint64_t v : dict.values) put_value(out, v, type);
}

}  // namespace

// ---- 64-bit view of a PLAIN buffer ----------------------------------------------

namespace {

template <typename T>
void widen_typed(const uint8_t* bytes, size_t n, std::vector<uint64_t>& out) {
  out.resize(n);
  for (size_t i = 0; i < n; ++i) {
    T v;
    std::memcpy(&v, bytes + i * sizeof(T), sizeof(T));
    out[i] = widen(v);
  }
}

// The buffer holds n values of T back to back (what ColWriter fills), so
// a typed view of it is a copy into a vector<T>; min_max_bits() then runs
// its usual std::minmax_element. The copy is one memcpy of the chunk.
template <typename T>
void min_max_typed(const uint8_t* bytes, size_t n, uint64_t& min_bits, uint64_t& max_bits) {
  std::vector<T> values(n);
  if (n > 0) std::memcpy(values.data(), bytes, n * sizeof(T));
  min_max_bits(values, min_bits, max_bits);
}

}  // namespace

void widen_bytes(ColumnType type, const uint8_t* bytes, size_t n, std::vector<uint64_t>& out) {
  switch (type) {
    case ColumnType::U8: widen_typed<uint8_t>(bytes, n, out); return;
    case ColumnType::U16: widen_typed<uint16_t>(bytes, n, out); return;
    case ColumnType::U32: widen_typed<uint32_t>(bytes, n, out); return;
    case ColumnType::U64: widen_typed<uint64_t>(bytes, n, out); return;
    case ColumnType::I64: widen_typed<int64_t>(bytes, n, out); return;
  }
  throw std::invalid_argument("widen_bytes: bad column type");
}

void min_max_bytes(ColumnType type, const uint8_t* bytes, size_t n, uint64_t& min_bits, uint64_t& max_bits) {
  switch (type) {
    case ColumnType::U8: min_max_typed<uint8_t>(bytes, n, min_bits, max_bits); return;
    case ColumnType::U16: min_max_typed<uint16_t>(bytes, n, min_bits, max_bits); return;
    case ColumnType::U32: min_max_typed<uint32_t>(bytes, n, min_bits, max_bits); return;
    case ColumnType::U64: min_max_typed<uint64_t>(bytes, n, min_bits, max_bits); return;
    case ColumnType::I64: min_max_typed<int64_t>(bytes, n, min_bits, max_bits); return;
  }
  throw std::invalid_argument("min_max_bytes: bad column type");
}

// ---- Dictionary -----------------------------------------------------------------

unsigned Dictionary::bit_width_for_count(size_t count) {
  return count <= 1 ? 0 : bit_width_of(static_cast<uint64_t>(count - 1));
}

namespace {

// Smallest possible DICT / RLE_DICT sizes once a scan has seen `d` distinct
// values and `r` runs in a chunk of n values. Both are exact copies of the
// encoders' arithmetic with the final d and r replaced by the partial
// counts; since every term only grows as the scan continues, these are
// true lower bounds, and a scan can stop as soon as both exceed the best
// size already in hand. (9 and 14 are the fixed header bytes.)
size_t dict_lower_bound(size_t d, size_t n, size_t width) {
  return 9 + d * width + packed_bytes(n, Dictionary::bit_width_for_count(d));
}
size_t rle_dict_lower_bound(size_t d, size_t r, size_t width) {
  return 14 + d * width + packed_bytes(r, Dictionary::bit_width_for_count(d)) + packed_bytes(r, 1);
}

// One pass over the chunk: assigns each distinct value a code in
// first-seen order and counts runs. Stops early (returns false) once the
// dictionary codecs can no longer produce fewer than `size_limit` bytes.
struct DictScan {
  std::unordered_map<uint64_t, uint32_t> index;  // value -> first-seen code
  std::vector<uint64_t> first_seen;              // code -> value
  std::vector<uint32_t> raw_codes;               // per row, first-seen code
  size_t runs = 0;
};

bool scan_for_dictionary(const uint64_t* values, size_t n, size_t width, size_t size_limit, DictScan& scan) {
  scan.raw_codes.resize(n);
  scan.index.reserve(1024);
  for (size_t i = 0; i < n; ++i) {
    const uint64_t v = values[i];
    const auto [it, inserted] = scan.index.try_emplace(v, static_cast<uint32_t>(scan.first_seen.size()));
    if (inserted) scan.first_seen.push_back(v);
    scan.raw_codes[i] = it->second;
    if (i == 0 || v != values[i - 1]) ++scan.runs;
    if (inserted) {  // bounds only move when d changes (r alone cannot push both over)
      const size_t d = scan.first_seen.size();
      if (dict_lower_bound(d, n, width) >= size_limit && rle_dict_lower_bound(d, scan.runs, width) >= size_limit) {
        return false;
      }
    }
  }
  return true;
}

// Turns a completed scan into a Dictionary: sort the distinct values in
// the column's natural order and renumber the codes to match. Sorted codes
// mean code order == value order, so a range predicate on the column maps
// to a range of codes (useful in m3). Sorting d distinct values is cheap
// where a dictionary is worth having (d small); sorting all n values was
// the single largest cost of ingest before this scan existed.
Dictionary finish_dictionary(DictScan&& scan, ColumnType type) {
  const size_t d = scan.first_seen.size();
  std::vector<uint32_t> order(d);  // order[k] = first-seen code of the k-th smallest value
  for (uint32_t k = 0; k < d; ++k) order[k] = k;
  std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
    return less_in_type(scan.first_seen[a], scan.first_seen[b], type);
  });

  Dictionary dict;
  dict.values.resize(d);
  std::vector<uint32_t> sorted_code(d);  // first-seen code -> sorted code
  for (uint32_t k = 0; k < d; ++k) {
    dict.values[k] = scan.first_seen[order[k]];
    sorted_code[order[k]] = k;
  }
  dict.codes = std::move(scan.raw_codes);
  for (uint32_t& c : dict.codes) c = sorted_code[c];
  return dict;
}

}  // namespace

Dictionary build_dictionary(const uint64_t* values, size_t n, ColumnType type) {
  DictScan scan;
  scan_for_dictionary(values, n, value_width(type), SIZE_MAX, scan);  // no limit: always completes
  return finish_dictionary(std::move(scan), type);
}

// ---- DICT -----------------------------------------------------------------------

size_t encode_dict(const Dictionary& dict, ColumnType type, std::vector<uint8_t>& out) {
  const size_t start = out.size();
  const unsigned width = dict.code_width();
  put_u32(out, static_cast<uint32_t>(dict.codes.size()));
  write_dictionary(dict, type, out);
  put_u8(out, static_cast<uint8_t>(width));
  {
    BitWriter w(out);
    for (uint32_t code : dict.codes) w.put(code, width);
  }  // BitWriter flushes its partial word on destruction
  return out.size() - start;
}

Dictionary decode_dict_parts(const uint8_t* data, size_t len, uint32_t rows, ColumnType type) {
  Cursor c(data, len);
  check_count(c.u32("count"), rows);
  Dictionary d;
  d.values = read_dictionary(c, type);
  const unsigned width = c.u8("code width");
  if (width != Dictionary::bit_width_for_count(d.values.size())) {
    throw std::runtime_error("corrupt chunk: dictionary code width does not match dictionary size");
  }
  if (rows > 0 && d.values.empty()) throw std::runtime_error("corrupt chunk: empty dictionary with rows");

  BitReader bits(c.ptr(), c.remaining());
  bits.require(rows, width);
  d.codes.resize(rows);
  for (uint32_t i = 0; i < rows; ++i) {
    const uint64_t code = bits.get(width);
    if (code >= d.values.size()) throw std::runtime_error("corrupt chunk: dictionary code out of range");
    d.codes[i] = static_cast<uint32_t>(code);
  }
  c.skip(bits.bytes_consumed(), "codes");
  c.expect_end("codes");
  return d;
}

namespace {

// codes -> values through the dictionary. Codes were range-checked by the
// parts decoder, so this is a plain table lookup.
std::vector<uint64_t> expand_codes(const Dictionary& d) {
  std::vector<uint64_t> out(d.codes.size());
  for (size_t i = 0; i < d.codes.size(); ++i) out[i] = d.values[d.codes[i]];
  return out;
}

}  // namespace

std::vector<uint64_t> decode_dict(const uint8_t* data, size_t len, uint32_t rows, ColumnType type) {
  return expand_codes(decode_dict_parts(data, len, rows, type));
}

// ---- RLE_DICT -------------------------------------------------------------------

size_t encode_rle_dict(const Dictionary& dict, ColumnType type, std::vector<uint8_t>& out) {
  const size_t start = out.size();
  const size_t n = dict.codes.size();

  // Runs of equal codes. Two parallel arrays rather than interleaved pairs
  // so each can be bit-packed at its own width.
  std::vector<uint32_t> run_codes;
  std::vector<uint64_t> run_lens;
  uint64_t max_len = 0;
  for (size_t i = 0; i < n;) {
    size_t j = i + 1;
    while (j < n && dict.codes[j] == dict.codes[i]) ++j;
    run_codes.push_back(dict.codes[i]);
    run_lens.push_back(j - i);
    max_len = std::max<uint64_t>(max_len, j - i);
    i = j;
  }
  const unsigned code_width = dict.code_width();
  const unsigned len_width = bit_width_of(max_len);

  put_u32(out, static_cast<uint32_t>(n));
  write_dictionary(dict, type, out);
  put_u32(out, static_cast<uint32_t>(run_codes.size()));
  put_u8(out, static_cast<uint8_t>(code_width));
  put_u8(out, static_cast<uint8_t>(len_width));
  {
    BitWriter w(out);
    for (uint32_t code : run_codes) w.put(code, code_width);
  }
  {
    BitWriter w(out);
    for (uint64_t l : run_lens) w.put(l, len_width);
  }
  return out.size() - start;
}

Dictionary decode_rle_dict_parts(const uint8_t* data, size_t len, uint32_t rows, ColumnType type) {
  Cursor c(data, len);
  check_count(c.u32("count"), rows);
  Dictionary d;
  d.values = read_dictionary(c, type);
  const uint32_t runs = c.u32("run count");
  const unsigned code_width = c.u8("code width");
  const unsigned len_width = c.u8("length width");
  if (code_width != Dictionary::bit_width_for_count(d.values.size())) {
    throw std::runtime_error("corrupt chunk: dictionary code width does not match dictionary size");
  }
  if (len_width > 64) throw std::runtime_error("corrupt chunk: run length width > 64");
  if (runs > rows) throw std::runtime_error("corrupt chunk: more runs than rows");

  // Codes section, then lengths section, each padded to a whole byte.
  BitReader code_bits(c.ptr(), c.remaining());
  code_bits.require(runs, code_width);
  c.skip(packed_bytes(runs, code_width), "run codes");
  BitReader len_bits(c.ptr(), c.remaining());
  len_bits.require(runs, len_width);
  c.skip(packed_bytes(runs, len_width), "run lengths");
  c.expect_end("run lengths");

  d.codes.reserve(rows);
  for (uint32_t r = 0; r < runs; ++r) {
    const uint64_t code = code_bits.get(code_width);
    const uint64_t run_len = len_bits.get(len_width);
    if (code >= d.values.size()) throw std::runtime_error("corrupt chunk: dictionary code out of range");
    if (run_len == 0 || run_len > rows - d.codes.size()) throw std::runtime_error("corrupt chunk: bad run length");
    d.codes.insert(d.codes.end(), static_cast<size_t>(run_len), static_cast<uint32_t>(code));
  }
  if (d.codes.size() != rows) throw std::runtime_error("corrupt chunk: runs do not cover all rows");
  return d;
}

std::vector<uint64_t> decode_rle_dict(const uint8_t* data, size_t len, uint32_t rows, ColumnType type) {
  return expand_codes(decode_rle_dict_parts(data, len, rows, type));
}

// ---- DELTA_BP -------------------------------------------------------------------

size_t encode_delta_bp(const uint64_t* values, size_t n, std::vector<uint8_t>& out) {
  const size_t start = out.size();
  put_u32(out, static_cast<uint32_t>(n));

  std::array<uint64_t, kDeltaBlock> deltas{};
  for (size_t b = 0; b < n; b += kDeltaBlock) {
    const size_t m = std::min(kDeltaBlock, n - b);  // values in this block

    // Deltas in wrapping arithmetic: the decoder adds them back with the
    // same wrap, so the round trip is exact even across INT64_MIN/MAX or
    // for unsigned columns, and the encoder never needs a wider type.
    int64_t lo = 0, hi = 0;
    for (size_t k = 1; k < m; ++k) {
      deltas[k] = values[b + k] - values[b + k - 1];
      const int64_t d = as_signed(deltas[k]);
      if (k == 1 || d < lo) lo = d;
      if (k == 1 || d > hi) hi = d;
    }
    // Frame of reference over the deltas: subtract the smallest so the
    // packed numbers are 0..(hi-lo). hi >= lo, so the unsigned difference
    // is the true range even when the signed subtraction would overflow.
    const uint64_t base = static_cast<uint64_t>(lo);
    const uint64_t range = static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo);
    const unsigned width = bit_width_of(range);

    put_u64(out, values[b]);
    put_u8(out, static_cast<uint8_t>(width));
    put_u64(out, base);
    BitWriter w(out);
    for (size_t k = 1; k < m; ++k) w.put(deltas[k] - base, width);
  }
  return out.size() - start;
}

std::vector<uint64_t> decode_delta_bp(const uint8_t* data, size_t len, uint32_t rows) {
  Cursor c(data, len);
  check_count(c.u32("count"), rows);
  std::vector<uint64_t> out(rows);
  for (size_t b = 0; b < rows; b += kDeltaBlock) {
    const size_t m = std::min(kDeltaBlock, rows - b);
    const uint64_t first = c.u64("block first value");
    const unsigned width = c.u8("block bit width");
    const uint64_t base = c.u64("block base");
    if (width > 64) throw std::runtime_error("corrupt chunk: delta bit width > 64");

    BitReader bits(c.ptr(), c.remaining());
    bits.require(m - 1, width);
    uint64_t prev = first;
    out[b] = first;
    for (size_t k = 1; k < m; ++k) {
      prev += base + bits.get(width);  // wrapping, mirrors the encoder
      out[b + k] = prev;
    }
    c.skip(packed_bytes(m - 1, width), "block deltas");
  }
  c.expect_end("delta blocks");
  return out;
}

// ---- selection and dispatch ------------------------------------------------------

Encoding encode_smallest(const uint64_t* values, size_t n, ColumnType type,
                         std::vector<uint8_t>& out, CandidateSizes* sizes) {
  out.clear();
  const size_t width = value_width(type);
  CandidateSizes s;
  s.plain = n * width;

  Encoding best = Encoding::Plain;
  size_t best_bytes = s.plain;
  std::vector<uint8_t> tmp;
  // Strictly smaller replaces the current best, so ties keep the earlier
  // candidate (PLAIN decodes fastest).
  const auto consider = [&](Encoding enc, size_t bytes) {
    if (bytes < best_bytes) {
      best = enc;
      best_bytes = bytes;
      out.swap(tmp);
    }
  };

  // DELTA_BP first: one cheap pass, and its size is the bar the dictionary
  // codecs have to clear.
  tmp.clear();
  s.delta_bp = encode_delta_bp(values, n, tmp);
  consider(Encoding::DeltaBp, s.delta_bp);

  // DICT and RLE_DICT need the distinct values, which is the expensive
  // part. The scan gives up as soon as the dictionary alone would make
  // both codecs at least as large as the best so far, so high-cardinality
  // chunks (ids, timestamps, costs) never pay for a dictionary build.
  DictScan scan;
  if (scan_for_dictionary(values, n, width, best_bytes, scan)) {
    const Dictionary dict = finish_dictionary(std::move(scan), type);
    tmp.clear();
    s.dict = encode_dict(dict, type, tmp);
    consider(Encoding::Dict, s.dict);
    tmp.clear();
    s.rle_dict = encode_rle_dict(dict, type, tmp);
    consider(Encoding::RleDict, s.rle_dict);
  }

  if (best == Encoding::Plain) out.clear();  // caller writes PLAIN with the typed memcpy
  if (sizes) *sizes = s;
  return best;
}

std::vector<uint64_t> decode_chunk(Encoding enc, const uint8_t* data, size_t len, uint32_t rows,
                                   ColumnType type) {
  switch (enc) {
    case Encoding::Plain: {
      Cursor c(data, len);
      if (len != static_cast<size_t>(rows) * value_width(type)) {
        throw std::runtime_error("plain chunk: byte length does not match row count");
      }
      std::vector<uint64_t> out(rows);
      for (uint32_t i = 0; i < rows; ++i) out[i] = c.value(type, "plain values");
      return out;
    }
    case Encoding::Dict: return decode_dict(data, len, rows, type);
    case Encoding::RleDict: return decode_rle_dict(data, len, rows, type);
    case Encoding::DeltaBp: return decode_delta_bp(data, len, rows);
  }
  throw std::runtime_error("unsupported encoding id " + std::to_string(static_cast<uint32_t>(enc)));
}

}  // namespace colstore
