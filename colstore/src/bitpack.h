#pragma once
// Bit packing: store n integers that each fit in `width` bits (0..64) using
// exactly ceil(n * width / 8) bytes instead of n * 8. Every m2 encoding
// (dictionary codes, delta blocks, run lengths) ends in one of these two
// classes, so they live in their own header with their own tests.
//
// Layout: a single bit stream, least-significant bit first. Bit k of the
// stream is bit (k % 8) of byte (k / 8). Value i occupies stream bits
// [i*width, (i+1)*width). Reading the bytes as little-endian 64-bit words
// gives the same stream, which is why the writer can work a word at a time.
//
// Written by hand on purpose (no SIMD, no compiler intrinsics beyond
// countl_zero): the point of the exercise is to be able to explain every
// shift. Throughput is still tens of millions of values per second, which
// is far more than the ingest path needs.

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace colstore {

// Smallest number of bits that can hold x: 0 for 0, 1 for 1, 2 for 2..3,
// ..., 64 for anything with the top bit set.
inline unsigned bit_width_of(uint64_t x) {
  return static_cast<unsigned>(64 - std::countl_zero(x));
}

// Bytes needed to hold n values of `width` bits.
inline size_t packed_bytes(size_t n, unsigned width) {
  return (n * width + 7) / 8;
}

// Appends packed values to a byte vector. All values must be written with
// the same width the reader will use; the class does not store it.
class BitWriter {
 public:
  explicit BitWriter(std::vector<uint8_t>& out) : out_(out) {}
  ~BitWriter() { finish(); }
  BitWriter(const BitWriter&) = delete;
  BitWriter& operator=(const BitWriter&) = delete;

  // Appends the low `width` bits of v. Any higher bits of v are ignored, so
  // the caller is responsible for choosing a width that fits.
  void put(uint64_t v, unsigned width) {
    if (width == 0) return;
    if (width < 64) v &= (uint64_t{1} << width) - 1;  // honour "higher bits ignored"
    // acc_ holds acc_bits_ (< 64) pending bits in its low end. Shift v up
    // past them; whatever does not fit in the word is written after the
    // word is flushed. acc_bits_ < 64 guarantees the shift is defined.
    acc_ |= v << acc_bits_;
    if (acc_bits_ + width < 64) {
      acc_bits_ += width;
      return;
    }
    // The word is full: write it, then keep the bits of v that did not
    // fit. `consumed` is how many of v's bits went into the word; it is 64
    // only when acc_bits_ was 0 and width is 64, in which case nothing
    // remains and the explicit branch avoids an undefined 64-bit shift.
    const unsigned consumed = 64 - acc_bits_;
    flush_word();  // resets acc_ and acc_bits_
    acc_ = consumed == 64 ? 0 : v >> consumed;
    acc_bits_ = width - consumed;
  }

  // Writes the pending partial word, rounded up to whole bytes. Called by
  // the destructor; calling it earlier is fine (further put() is not).
  void finish() {
    if (acc_bits_ == 0) return;
    const size_t bytes = (acc_bits_ + 7) / 8;
    uint8_t tmp[8];
    std::memcpy(tmp, &acc_, 8);  // little-endian host, asserted in format.h
    out_.insert(out_.end(), tmp, tmp + bytes);
    acc_ = 0;
    acc_bits_ = 0;
  }

 private:
  void flush_word() {
    uint8_t tmp[8];
    std::memcpy(tmp, &acc_, 8);
    out_.insert(out_.end(), tmp, tmp + 8);
    acc_ = 0;
    acc_bits_ = 0;
  }

  std::vector<uint8_t>& out_;
  uint64_t acc_ = 0;
  unsigned acc_bits_ = 0;  // always < 64 between calls
};

// Reads packed values from a byte range. Bounds are checked once per
// value against the range, so a truncated or corrupt chunk throws instead
// of reading past the buffer.
class BitReader {
 public:
  BitReader(const uint8_t* data, size_t len) : data_(data), total_bits_(len * 8) {}

  // Throws if fewer than n * width bits remain. Callers check this once
  // before a decode loop so the loop itself cannot overrun.
  void require(size_t n, unsigned width) const {
    if (n * width > total_bits_ - pos_) throw std::runtime_error("bit-packed data truncated");
  }

  uint64_t get(unsigned width) {
    if (width == 0) return 0;
    if (width > total_bits_ - pos_) throw std::runtime_error("bit-packed data truncated");
    const size_t byte = pos_ / 8;
    const unsigned shift = static_cast<unsigned>(pos_ % 8);  // 0..7
    // Load up to 8 bytes starting at `byte` (zero padded near the end of
    // the buffer) and drop the `shift` bits that belong to earlier values.
    uint64_t v = load_word(byte) >> shift;
    // A value can straddle 9 bytes: shift (1..7) + width (up to 64) > 64.
    // The extra bits are the low bits of byte+8, placed above the 64-shift
    // bits already taken. shift >= 1 here, so the shift amount is < 64.
    if (shift + width > 64) v |= static_cast<uint64_t>(byte_at(byte + 8)) << (64 - shift);
    pos_ += width;
    return width == 64 ? v : v & ((uint64_t{1} << width) - 1);
  }

  size_t bits_consumed() const { return pos_; }
  // Bytes covered so far, rounded up to a whole byte, so the caller can
  // step past this packed section to whatever follows it in the chunk.
  size_t bytes_consumed() const { return (pos_ + 7) / 8; }

 private:
  uint64_t load_word(size_t byte) const {
    uint64_t w = 0;
    const size_t avail = total_bits_ / 8 - byte;
    std::memcpy(&w, data_ + byte, avail < 8 ? avail : 8);
    return w;
  }
  uint8_t byte_at(size_t i) const { return i < total_bits_ / 8 ? data_[i] : 0; }

  const uint8_t* data_;
  size_t total_bits_;
  size_t pos_ = 0;
};

}  // namespace colstore
