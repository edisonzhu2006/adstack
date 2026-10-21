// Bit packing: every width 0..64 round-trips, the byte count is exactly
// ceil(n * width / 8), the layout matches an independent bit-by-bit
// reference (so a different implementation could read our files), and
// truncated input throws instead of reading past the buffer.

#include <random>

#include <gtest/gtest.h>

#include "src/bitpack.h"

using namespace colstore;

namespace {

// Reference extractor: bit k of the stream is bit (k % 8) of byte (k / 8).
// Deliberately slow and obvious, so the fast reader is checked against
// something that cannot be wrong in the same way.
uint64_t ref_get(const std::vector<uint8_t>& bytes, size_t bit_pos, unsigned width) {
  uint64_t v = 0;
  for (unsigned i = 0; i < width; ++i) {
    const size_t k = bit_pos + i;
    const uint64_t bit = (bytes[k / 8] >> (k % 8)) & 1u;
    v |= bit << i;
  }
  return v;
}

uint64_t mask(unsigned width) { return width == 64 ? ~uint64_t{0} : (uint64_t{1} << width) - 1; }

}  // namespace

TEST(BitWidth, SmallestWidthThatHolds) {
  EXPECT_EQ(bit_width_of(0), 0u);
  EXPECT_EQ(bit_width_of(1), 1u);
  EXPECT_EQ(bit_width_of(2), 2u);
  EXPECT_EQ(bit_width_of(3), 2u);
  EXPECT_EQ(bit_width_of(255), 8u);
  EXPECT_EQ(bit_width_of(256), 9u);
  EXPECT_EQ(bit_width_of(uint64_t{1} << 63), 64u);
  EXPECT_EQ(bit_width_of(~uint64_t{0}), 64u);
  EXPECT_EQ(packed_bytes(0, 64), 0u);
  EXPECT_EQ(packed_bytes(1, 1), 1u);
  EXPECT_EQ(packed_bytes(8, 1), 1u);
  EXPECT_EQ(packed_bytes(9, 1), 2u);
  EXPECT_EQ(packed_bytes(3, 64), 24u);
}

TEST(BitPack, EveryWidthRoundTripsAndMatchesReference) {
  std::mt19937_64 rng(7);
  for (unsigned width = 0; width <= 64; ++width) {
    const size_t n = 1000;
    std::vector<uint64_t> values(n);
    for (auto& v : values) v = rng() & mask(width);
    if (width > 0) {
      values[0] = mask(width);  // all ones
      values[1] = 0;
      values[2] = uint64_t{1} << (width - 1);  // top bit only
    }

    std::vector<uint8_t> bytes;
    {
      BitWriter w(bytes);
      for (uint64_t v : values) w.put(v, width);
    }
    ASSERT_EQ(bytes.size(), packed_bytes(n, width)) << "width " << width;

    BitReader r(bytes.data(), bytes.size());
    for (size_t i = 0; i < n; ++i) {
      ASSERT_EQ(r.get(width), values[i]) << "width " << width << " index " << i;
      ASSERT_EQ(ref_get(bytes, i * width, width), values[i]) << "width " << width << " index " << i;
    }
    EXPECT_EQ(r.bits_consumed(), n * width);
    EXPECT_EQ(r.bytes_consumed(), bytes.size());
  }
}

TEST(BitPack, HigherBitsOfInputAreIgnored) {
  std::vector<uint8_t> bytes;
  {
    BitWriter w(bytes);
    w.put(0xFF, 3);  // only low 3 bits (111) must land
    w.put(0x00, 3);
    w.put(0xFF, 2);
  }
  ASSERT_EQ(bytes.size(), 1u);
  EXPECT_EQ(bytes[0], 0b11000111);
}

TEST(BitPack, WidthZeroWritesNothingAndReadsZero) {
  std::vector<uint8_t> bytes;
  {
    BitWriter w(bytes);
    for (int i = 0; i < 100; ++i) w.put(12345, 0);
  }
  EXPECT_TRUE(bytes.empty());
  BitReader r(bytes.data(), bytes.size());
  r.require(100, 0);
  for (int i = 0; i < 100; ++i) EXPECT_EQ(r.get(0), 0u);
}

TEST(BitPack, ValuesStraddlingWordBoundaries) {
  // Width 33 means every other value straddles a 64-bit word; width 63
  // shifts the straddle by one bit per value, covering every alignment.
  for (unsigned width : {7u, 33u, 63u, 64u}) {
    std::vector<uint64_t> values;
    for (uint64_t i = 0; i < 200; ++i) values.push_back((i * 0x9E3779B97F4A7C15ULL) & mask(width));
    std::vector<uint8_t> bytes;
    {
      BitWriter w(bytes);
      for (uint64_t v : values) w.put(v, width);
    }
    BitReader r(bytes.data(), bytes.size());
    for (size_t i = 0; i < values.size(); ++i) ASSERT_EQ(r.get(width), values[i]) << width << "/" << i;
  }
}

TEST(BitPack, TruncatedInputThrows) {
  std::vector<uint8_t> bytes;
  {
    BitWriter w(bytes);
    for (int i = 0; i < 10; ++i) w.put(5, 13);
  }
  ASSERT_EQ(bytes.size(), packed_bytes(10, 13));
  BitReader short_reader(bytes.data(), bytes.size() - 1);
  EXPECT_THROW(short_reader.require(10, 13), std::runtime_error);
  for (int i = 0; i < 9; ++i) EXPECT_EQ(short_reader.get(13), 5u);
  EXPECT_THROW(short_reader.get(13), std::runtime_error);  // the 10th value needs the missing byte

  BitReader empty(nullptr, 0);
  EXPECT_NO_THROW(empty.require(0, 64));
  EXPECT_THROW(empty.get(1), std::runtime_error);
}
