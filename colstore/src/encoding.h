#pragma once
// Column chunk encodings: PLAIN, DICT, DELTA_BP and RLE_DICT, plus the
// selector that picks one per chunk. The writer calls encode_smallest() on
// each column of each row group; the reader calls decode_chunk() with the
// encoding id it finds in the footer.
//
// One implementation per codec, not one per column type: the three
// non-PLAIN codecs work on 64-bit patterns (uint64_t). Unsigned columns
// are zero-extended, i64 columns sign-extended (see widen()/narrow()), and
// the ColumnType is passed along so dictionaries are stored at the
// column's natural width and sorted in its natural order. PLAIN stays a
// typed memcpy because it is the hot path for verify and the baseline for
// every size comparison.
//
// Byte layouts (all little-endian, no padding; "n" is the value count,
// also recorded in ChunkMeta.rows and checked against it on decode):
//
//   PLAIN      n * width bytes, each value at its natural width.
//
//   DICT       u32 n, u32 d, d values at natural width (sorted ascending),
//              u8 code_width (= bits needed for d-1, 0 when d <= 1),
//              ceil(n * code_width / 8) bytes of bit-packed codes.
//
//   DELTA_BP   u32 n, then ceil(n / 128) blocks; block j holds values
//              [128j, min(128j+128, n)):
//                u64 first   raw bits of the block's first value
//                u8  width   bits per packed delta, 0..64
//                u64 base    smallest delta in the block (raw bits)
//                ceil((m-1) * width / 8) bytes: (delta - base) for the
//                remaining m-1 values, delta = value[i] - value[i-1] in
//                wrapping 64-bit arithmetic.
//
//   RLE_DICT   u32 n, u32 d, d values at natural width (sorted ascending),
//              u32 r (number of runs), u8 code_width, u8 len_width,
//              ceil(r * code_width / 8) bytes of packed run codes,
//              ceil(r * len_width / 8) bytes of packed run lengths.
//
// Decoders validate the structure of everything they read (counts, code
// width and range, dictionary order, run length sum, value width for
// DELTA_BP, remaining bytes) and throw std::runtime_error on a corrupt
// chunk; they never read outside [data, data + len). A corruption that
// leaves the structure valid (one flipped bit in a value) is invisible to
// them; that is what the per-chunk CRC-32C checked by ColReader is for.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "src/format.h"

namespace colstore {

// ---- PLAIN (typed, memcpy) --------------------------------------------------

// Appends the PLAIN encoding of `values` to `out`. Returns bytes written.
template <typename T>
size_t encode_plain(const std::vector<T>& values, std::vector<uint8_t>& out) {
  static_assert(std::is_integral_v<T>);
  const size_t bytes = values.size() * sizeof(T);
  const size_t start = out.size();
  out.resize(start + bytes);
  // Little-endian host (asserted in format.h), so the in-memory layout is
  // already the wire layout.
  if (bytes > 0) std::memcpy(out.data() + start, values.data(), bytes);
  return bytes;
}

// Decodes `rows` PLAIN values of type T from `data` (exactly rows*sizeof(T) bytes).
template <typename T>
std::vector<T> decode_plain(const uint8_t* data, size_t len, uint32_t rows) {
  static_assert(std::is_integral_v<T>);
  if (len != static_cast<size_t>(rows) * sizeof(T)) {
    throw std::runtime_error("plain chunk: byte length does not match row count");
  }
  std::vector<T> values(rows);
  if (len > 0) std::memcpy(values.data(), data, len);
  return values;
}

// ---- 64-bit view of a column --------------------------------------------------

// Value -> 64-bit pattern. Signed values sign-extend so as_signed() and the
// signed comparisons in the codecs recover them; unsigned values zero-extend.
template <typename T>
inline uint64_t widen(T v) {
  static_assert(std::is_integral_v<T>);
  if constexpr (std::is_signed_v<T>) return static_cast<uint64_t>(static_cast<int64_t>(v));
  else return static_cast<uint64_t>(v);
}

template <typename T>
void widen_all(const std::vector<T>& in, std::vector<uint64_t>& out) {
  out.resize(in.size());
  for (size_t i = 0; i < in.size(); ++i) out[i] = widen(in[i]);
}

// 64-bit pattern -> value. Throws if the pattern does not fit the column
// width, which can only happen with a corrupt chunk: the encoders only ever
// see patterns produced by widen().
template <typename T>
std::vector<T> narrow_all(const std::vector<uint64_t>& in) {
  static_assert(std::is_integral_v<T>);
  std::vector<T> out(in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    const uint64_t bits = in[i];
    // Unsigned narrower than 64 bits: the high bits must be zero. Signed
    // (i64) and 64-bit unsigned use all 64 bits, nothing to check.
    if constexpr (sizeof(T) < 8) {
      if ((bits >> (8 * sizeof(T))) != 0) throw std::runtime_error("corrupt chunk: value exceeds column width");
    }
    out[i] = static_cast<T>(bits);  // well defined for every T in C++20 (two's complement)
  }
  return out;
}

// The same widening from a PLAIN byte buffer: n values of the column's
// natural width, little-endian (what ColWriter buffers and a PLAIN chunk
// holds). The type is a run-time value here because the writer handles
// any schema; the switch dispatches once per chunk, not per value.
void widen_bytes(ColumnType type, const uint8_t* bytes, size_t n, std::vector<uint64_t>& out);

// Compares two 64-bit patterns in the column's natural order.
inline bool less_in_type(uint64_t a, uint64_t b, ColumnType type) {
  if (is_signed(type)) return as_signed(a) < as_signed(b);
  return a < b;
}

// min_max_bits() for a PLAIN byte buffer of a run-time type: the same
// typed std::minmax_element pass, dispatched once per chunk.
void min_max_bytes(ColumnType type, const uint8_t* bytes, size_t n, uint64_t& min_bits, uint64_t& max_bits);

// Min and max of a chunk as the raw 8-byte form stored in ChunkMeta.
// Signed values are sign-extended so as_signed() recovers them; unsigned
// values are zero-extended. Empty input gives {0, 0}.
template <typename T>
void min_max_bits(const std::vector<T>& values, uint64_t& min_bits, uint64_t& max_bits) {
  static_assert(std::is_integral_v<T>);
  if (values.empty()) {
    min_bits = max_bits = 0;
    return;
  }
  const auto [lo, hi] = std::minmax_element(values.begin(), values.end());
  min_bits = widen(*lo);
  max_bits = widen(*hi);
}

// ---- DICT / RLE_DICT ------------------------------------------------------------

// The sorted distinct values of a chunk and each row's index into them.
// Built once and shared by DICT and RLE_DICT, since the sort is the
// expensive part of both.
struct Dictionary {
  std::vector<uint64_t> values;  // distinct, ascending in the column's natural order
  std::vector<uint32_t> codes;   // codes[i] = index of row i's value in `values`

  unsigned code_width() const { return values.size() <= 1 ? 0 : bit_width_for_count(values.size()); }
  static unsigned bit_width_for_count(size_t count);  // bits to hold 0..count-1
};

Dictionary build_dictionary(const uint64_t* values, size_t n, ColumnType type);

size_t encode_dict(const Dictionary& dict, ColumnType type, std::vector<uint8_t>& out);
std::vector<uint64_t> decode_dict(const uint8_t* data, size_t len, uint32_t rows, ColumnType type);

size_t encode_rle_dict(const Dictionary& dict, ColumnType type, std::vector<uint8_t>& out);
std::vector<uint64_t> decode_rle_dict(const uint8_t* data, size_t len, uint32_t rows, ColumnType type);

// The same two decoders stopped one step early: the chunk's sorted
// dictionary plus one code per row, NOT expanded to values. The scanner
// (m3) filters dictionary columns on codes, so it needs this form; the
// value-returning decoders above are these plus a table lookup per row.
Dictionary decode_dict_parts(const uint8_t* data, size_t len, uint32_t rows, ColumnType type);
Dictionary decode_rle_dict_parts(const uint8_t* data, size_t len, uint32_t rows, ColumnType type);

// ---- DELTA_BP -------------------------------------------------------------------

constexpr size_t kDeltaBlock = 128;  // values per block; one bit width per block

size_t encode_delta_bp(const uint64_t* values, size_t n, std::vector<uint8_t>& out);
// `type` only bounds the result: for a u8/u16/u32 column a decoded value
// with bits above the column's width throws (corrupt chunk).
std::vector<uint64_t> decode_delta_bp(const uint8_t* data, size_t len, uint32_t rows,
                                      ColumnType type = ColumnType::U64);

// ---- selection and dispatch ----------------------------------------------------

// Byte size of each candidate for one chunk, as produced by the encoders.
// Returned by encode_smallest() so callers (tests, a future `info -v`) can
// see why an encoding was chosen. dict / rle_dict are 0 when the selector
// proved they could not win and skipped encoding them.
struct CandidateSizes {
  size_t plain = 0;
  size_t dict = 0;
  size_t delta_bp = 0;
  size_t rle_dict = 0;
};

// Encodes `values` with the candidate codecs and keeps the smallest.
// Returns the winner's id; `out` receives its bytes. If PLAIN wins (or
// ties: PLAIN decodes fastest), `out` is left EMPTY and the caller writes
// PLAIN itself with the typed encode_plain(), which avoids a second copy
// of the widened values.
//
// Rule: smallest encoded size wins; ties keep the earlier candidate, in
// the order PLAIN, DELTA_BP, DICT, RLE_DICT. Sizes come from actually
// encoding (no size model that can drift from the encoder), except that
// the dictionary codecs are skipped when a running lower bound shows they
// cannot beat the best so far; the result is identical to trying all four,
// which a test checks. The CPU cost at ingest is measured in NOTES.md.
Encoding encode_smallest(const uint64_t* values, size_t n, ColumnType type,
                         std::vector<uint8_t>& out, CandidateSizes* sizes = nullptr);

// Decodes a chunk of any encoding to 64-bit patterns. PLAIN goes through
// here too (one typed widen loop, widen_bytes), for callers that pick the
// column at run time: the query Scanner, verify, dump and the tests.
std::vector<uint64_t> decode_chunk(Encoding enc, const uint8_t* data, size_t len, uint32_t rows,
                                   ColumnType type);

}  // namespace colstore
