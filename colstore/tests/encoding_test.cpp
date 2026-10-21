// Codec round trips on edge cases (empty, one value, all equal, strictly
// increasing, random, negative, INT64 extremes, bit widths 0 and 64),
// corrupt-input rejection, and the per-chunk selector: expected winner on
// constructed inputs, and the early-exit pruning never changing the answer
// versus trying every codec.

#include <climits>
#include <random>

#include <gtest/gtest.h>

#include "src/bitpack.h"
#include "src/encoding.h"

using namespace colstore;

namespace {

// Encodes with one specific codec and decodes through the public dispatch,
// as the reader would. Returns the encoded size.
size_t round_trip_one(Encoding enc, const std::vector<uint64_t>& values, ColumnType type) {
  std::vector<uint8_t> bytes;
  const uint32_t rows = static_cast<uint32_t>(values.size());
  switch (enc) {
    case Encoding::Plain:
      for (uint64_t v : values) {
        const size_t w = value_width(type);
        for (size_t b = 0; b < w; ++b) bytes.push_back(static_cast<uint8_t>(v >> (8 * b)));
      }
      break;
    case Encoding::Dict: encode_dict(build_dictionary(values.data(), values.size(), type), type, bytes); break;
    case Encoding::RleDict: encode_rle_dict(build_dictionary(values.data(), values.size(), type), type, bytes); break;
    case Encoding::DeltaBp: encode_delta_bp(values.data(), values.size(), bytes); break;
  }
  const std::vector<uint64_t> back = decode_chunk(enc, bytes.data(), bytes.size(), rows, type);
  EXPECT_EQ(back, values) << encoding_name(static_cast<uint32_t>(enc));
  return bytes.size();
}

void round_trip_all(const std::vector<uint64_t>& values, ColumnType type) {
  for (Encoding enc : {Encoding::Plain, Encoding::Dict, Encoding::RleDict, Encoding::DeltaBp}) {
    round_trip_one(enc, values, type);
  }
}

std::vector<uint64_t> as_bits(std::initializer_list<int64_t> xs) {
  std::vector<uint64_t> v;
  for (int64_t x : xs) v.push_back(widen(x));
  return v;
}

}  // namespace

// ---- round trips ------------------------------------------------------------

TEST(Codecs, Empty) { round_trip_all({}, ColumnType::I64); round_trip_all({}, ColumnType::U8); }

TEST(Codecs, SingleValue) {
  round_trip_all({42}, ColumnType::U64);
  round_trip_all(as_bits({-42}), ColumnType::I64);
  round_trip_all({255}, ColumnType::U8);
}

TEST(Codecs, AllEqual) {
  round_trip_all(std::vector<uint64_t>(5000, 7), ColumnType::U32);
  round_trip_all(std::vector<uint64_t>(5000, widen(int64_t{-7})), ColumnType::I64);
  // All-equal DELTA_BP: every delta is 0, so every block has width 0.
  std::vector<uint8_t> bytes;
  encode_delta_bp(std::vector<uint64_t>(256, 9).data(), 256, bytes);
  EXPECT_EQ(bytes.size(), 4 + 2 * (8 + 1 + 8));  // header + two blocks of pure header
}

TEST(Codecs, StrictlyIncreasing) {
  std::vector<uint64_t> v;
  for (uint64_t i = 0; i < 10'000; ++i) v.push_back(i * 3);
  round_trip_all(v, ColumnType::U64);
  // Constant delta -> width 0 per block: 4 + ceil(10000/128)=79 blocks * 17 bytes.
  std::vector<uint8_t> bytes;
  encode_delta_bp(v.data(), v.size(), bytes);
  EXPECT_EQ(bytes.size(), 4 + 79 * 17);
}

TEST(Codecs, RandomFullRange) {
  std::mt19937_64 rng(11);
  std::vector<uint64_t> v(3000);
  for (auto& x : v) x = rng();
  round_trip_all(v, ColumnType::U64);
  round_trip_all(v, ColumnType::I64);  // same bits, signed interpretation
}

TEST(Codecs, NegativeValues) {
  round_trip_all(as_bits({-1, -2, -3, 0, 3, 2, 1, -1'000'000'000'000LL, 5}), ColumnType::I64);
  std::vector<uint64_t> v;
  for (int64_t i = -5000; i < 5000; ++i) v.push_back(widen(i * 7 - 3));
  round_trip_all(v, ColumnType::I64);
}

TEST(Codecs, Int64Extremes) {
  // Deltas between these wrap around 64 bits; the round trip must still be exact.
  round_trip_all(as_bits({LLONG_MIN, LLONG_MAX, LLONG_MIN, 0, LLONG_MAX, -1, 1, LLONG_MIN}), ColumnType::I64);
  round_trip_all(as_bits({LLONG_MAX, LLONG_MAX, LLONG_MIN, LLONG_MIN}), ColumnType::I64);
  round_trip_all({0, ~uint64_t{0}, 0, ~uint64_t{0}, 1, ~uint64_t{0} - 1}, ColumnType::U64);
}

TEST(Codecs, DeltaBitWidthZeroAndSixtyFour) {
  // Width 64 block: deltas of +INT64_MAX then INT64_MIN span the whole
  // 64-bit range. (Note {0, ~0, 0} would NOT: in wrapping arithmetic those
  // deltas are -1 and +1, two bits. That is why wrapping is used.)
  const std::vector<uint64_t> wide = {0, uint64_t{LLONG_MAX}, ~uint64_t{0}, 0, uint64_t{LLONG_MAX}};
  std::vector<uint8_t> bytes;
  encode_delta_bp(wide.data(), wide.size(), bytes);
  EXPECT_EQ(bytes[4 + 8], 64);  // width byte of block 0
  EXPECT_EQ(decode_delta_bp(bytes.data(), bytes.size(), 5), wide);
  std::vector<uint8_t> small;
  const std::vector<uint64_t> wrap = {0, ~uint64_t{0}, 0, 1, ~uint64_t{0}};
  encode_delta_bp(wrap.data(), wrap.size(), small);
  EXPECT_EQ(small[4 + 8], 2);  // deltas -1, +1, +1, -2: range 3 -> 2 bits
  EXPECT_EQ(decode_delta_bp(small.data(), small.size(), 5), wrap);
  // Width 0 block: one value per block has no deltas at all.
  bytes.clear();
  encode_delta_bp(wide.data(), 1, bytes);
  EXPECT_EQ(bytes[4 + 8], 0);
  EXPECT_EQ(bytes.size(), 4u + 17u);
}

// The writer widens typed vectors and the reader narrows them; exercise
// every width and codec with values at the type's limits.
template <typename T>
void typed_round_trip(const std::vector<T>& typed, ColumnType type) {
  std::vector<uint64_t> wide;
  widen_all(typed, wide);
  const Dictionary dict = build_dictionary(wide.data(), wide.size(), type);
  for (Encoding enc : {Encoding::Dict, Encoding::RleDict, Encoding::DeltaBp}) {
    std::vector<uint8_t> bytes;
    if (enc == Encoding::Dict) encode_dict(dict, type, bytes);
    if (enc == Encoding::RleDict) encode_rle_dict(dict, type, bytes);
    if (enc == Encoding::DeltaBp) encode_delta_bp(wide.data(), wide.size(), bytes);
    const auto back = narrow_all<T>(decode_chunk(enc, bytes.data(), bytes.size(), static_cast<uint32_t>(typed.size()), type));
    EXPECT_EQ(back, typed) << column_type_name(type) << " via " << encoding_name(static_cast<uint32_t>(enc));
  }
}

TEST(Codecs, NarrowTypesThroughTypedPath) {
  typed_round_trip<uint8_t>({0, 1, 255, 128, 7, 7, 7}, ColumnType::U8);
  typed_round_trip<uint16_t>({0, 65535, 1, 840, 356, 840}, ColumnType::U16);
  typed_round_trip<uint32_t>({0, 0xFFFFFFFFu, 123, 99'999, 99'999}, ColumnType::U32);
  typed_round_trip<uint64_t>({0, ~uint64_t{0}, 5, 5}, ColumnType::U64);
  typed_round_trip<int64_t>({LLONG_MIN, -1, 0, 1, LLONG_MAX, -1}, ColumnType::I64);
}

TEST(Codecs, NarrowRejectsValuesThatDoNotFit) {
  EXPECT_THROW(narrow_all<uint8_t>({256}), std::runtime_error);
  EXPECT_THROW(narrow_all<uint16_t>({65'536}), std::runtime_error);
  EXPECT_THROW(narrow_all<uint32_t>({uint64_t{1} << 32}), std::runtime_error);
  EXPECT_EQ(narrow_all<uint8_t>({255})[0], 255);
  EXPECT_EQ(narrow_all<int64_t>({widen(int64_t{-5})})[0], -5);
  EXPECT_EQ(narrow_all<uint64_t>({~uint64_t{0}})[0], ~uint64_t{0});
}

TEST(Dictionary, SortedInNaturalOrderWithMatchingCodes) {
  const std::vector<uint64_t> v = as_bits({5, -3, 5, 100, -3, 0});
  const Dictionary d = build_dictionary(v.data(), v.size(), ColumnType::I64);
  ASSERT_EQ(d.values, as_bits({-3, 0, 5, 100}));  // signed order: negatives first
  EXPECT_EQ(d.codes, (std::vector<uint32_t>{2, 0, 2, 3, 0, 1}));
  EXPECT_EQ(d.code_width(), 2u);

  // Same bits as unsigned: -3 is a huge value and sorts last.
  const Dictionary u = build_dictionary(v.data(), v.size(), ColumnType::U64);
  EXPECT_EQ(u.values.back(), widen(int64_t{-3}));
  EXPECT_EQ(Dictionary::bit_width_for_count(0), 0u);
  EXPECT_EQ(Dictionary::bit_width_for_count(1), 0u);
  EXPECT_EQ(Dictionary::bit_width_for_count(2), 1u);
  EXPECT_EQ(Dictionary::bit_width_for_count(3), 2u);
  EXPECT_EQ(Dictionary::bit_width_for_count(65'536), 16u);
}

// ---- corruption -------------------------------------------------------------

TEST(Codecs, EveryTruncationThrowsAndNeverCrashes) {
  std::mt19937_64 rng(3);
  std::vector<uint64_t> v(700);
  for (size_t i = 0; i < v.size(); ++i) v[i] = (i / 9) % 5 + (rng() % 2);  // few distinct, some runs
  for (Encoding enc : {Encoding::Dict, Encoding::RleDict, Encoding::DeltaBp}) {
    std::vector<uint8_t> bytes;
    if (enc == Encoding::Dict) encode_dict(build_dictionary(v.data(), v.size(), ColumnType::U32), ColumnType::U32, bytes);
    if (enc == Encoding::RleDict) encode_rle_dict(build_dictionary(v.data(), v.size(), ColumnType::U32), ColumnType::U32, bytes);
    if (enc == Encoding::DeltaBp) encode_delta_bp(v.data(), v.size(), bytes);
    const uint32_t rows = static_cast<uint32_t>(v.size());
    for (size_t len = 0; len < bytes.size(); ++len) {
      EXPECT_THROW(decode_chunk(enc, bytes.data(), len, rows, ColumnType::U32), std::runtime_error)
          << encoding_name(static_cast<uint32_t>(enc)) << " truncated to " << len;
    }
    // One extra byte is also rejected: a chunk must be consumed exactly.
    bytes.push_back(0);
    EXPECT_THROW(decode_chunk(enc, bytes.data(), bytes.size(), rows, ColumnType::U32), std::runtime_error);
    bytes.pop_back();
    // Footer and chunk disagreeing on the row count is rejected.
    EXPECT_THROW(decode_chunk(enc, bytes.data(), bytes.size(), rows + 1, ColumnType::U32), std::runtime_error);
  }
  EXPECT_THROW(decode_chunk(Encoding::Plain, nullptr, 3, 1, ColumnType::U32), std::runtime_error);
  EXPECT_THROW(decode_chunk(static_cast<Encoding>(99), nullptr, 0, 0, ColumnType::U32), std::runtime_error);
}

TEST(Codecs, DictCodeOutOfRangeThrows) {
  // Hand-built DICT chunk: n=2, d=2 (values 10, 20 as u8), width 1, codes
  // then patched so the second code points past the dictionary... with a
  // 1-bit code that is impossible, so use d=3 (width 2) and code 3.
  std::vector<uint8_t> bytes = {2, 0, 0, 0,  3, 0, 0, 0,  10, 20, 30,  2,  0b00001100};  // codes 0 and 3
  EXPECT_THROW(decode_dict(bytes.data(), bytes.size(), 2, ColumnType::U8), std::runtime_error);
  bytes.back() = 0b00001000;  // codes 0 and 2: valid
  EXPECT_EQ(decode_dict(bytes.data(), bytes.size(), 2, ColumnType::U8), (std::vector<uint64_t>{10, 30}));
  bytes[11] = 7;  // width byte inconsistent with d=3
  EXPECT_THROW(decode_dict(bytes.data(), bytes.size(), 2, ColumnType::U8), std::runtime_error);
}

// ---- selector -----------------------------------------------------------------

TEST(Selector, RandomFullRangePicksPlainAndLeavesOutputEmpty) {
  std::mt19937_64 rng(5);
  std::vector<uint64_t> v(4096);
  for (auto& x : v) x = rng();
  std::vector<uint8_t> out = {1, 2, 3};
  CandidateSizes s;
  EXPECT_EQ(encode_smallest(v.data(), v.size(), ColumnType::U64, out, &s), Encoding::Plain);
  EXPECT_TRUE(out.empty());
  EXPECT_EQ(s.plain, 4096u * 8);
  EXPECT_GE(s.delta_bp, s.plain);  // 64-bit deltas plus block headers
  EXPECT_EQ(s.dict, 0u);           // pruned: 4096 distinct 8-byte values cannot win
}

TEST(Selector, EmptyPicksPlain) {
  std::vector<uint8_t> out;
  EXPECT_EQ(encode_smallest(nullptr, 0, ColumnType::I64, out), Encoding::Plain);
  EXPECT_TRUE(out.empty());
}

TEST(Selector, LowCardinalityRandomPicksDict) {
  std::mt19937_64 rng(5);
  std::vector<uint64_t> v(4096);
  for (auto& x : v) x = rng() % 3;  // like device / event_type with short runs
  std::vector<uint8_t> out;
  CandidateSizes s;
  EXPECT_EQ(encode_smallest(v.data(), v.size(), ColumnType::U8, out, &s), Encoding::Dict);
  EXPECT_EQ(s.dict, 9u + 3 + packed_bytes(4096, 2));
  EXPECT_LT(s.dict, s.rle_dict);
  EXPECT_LT(s.dict, s.delta_bp);
  EXPECT_EQ(decode_chunk(Encoding::Dict, out.data(), out.size(), 4096, ColumnType::U8), v);
}

TEST(Selector, LongRunsPickRleDict) {
  std::vector<uint64_t> v;
  for (uint64_t run = 0; run < 64; ++run) v.insert(v.end(), 500, (run * 7) % 20);  // 64 runs of 500, 20 distinct
  std::vector<uint8_t> out;
  CandidateSizes s;
  EXPECT_EQ(encode_smallest(v.data(), v.size(), ColumnType::U32, out, &s), Encoding::RleDict);
  EXPECT_LT(s.rle_dict, s.dict);
  EXPECT_LT(s.rle_dict, s.delta_bp);
  EXPECT_EQ(decode_chunk(Encoding::RleDict, out.data(), out.size(), static_cast<uint32_t>(v.size()), ColumnType::U32), v);
}

TEST(Selector, NearMonotoneTimestampsPickDeltaBp) {
  std::mt19937_64 rng(9);
  std::vector<uint64_t> v;
  int64_t t = 1'767'225'600'000'000LL;
  for (int i = 0; i < 4096; ++i) {
    t += 100;
    v.push_back(widen(t - static_cast<int64_t>(rng() % 200'000)));  // 0..200 ms jitter, like event_time_us
  }
  std::vector<uint8_t> out;
  CandidateSizes s;
  EXPECT_EQ(encode_smallest(v.data(), v.size(), ColumnType::I64, out, &s), Encoding::DeltaBp);
  EXPECT_LT(s.delta_bp, s.plain / 2);
  EXPECT_EQ(s.dict, 0u);  // 4096 distinct timestamps: dictionary pruned
  EXPECT_EQ(decode_chunk(Encoding::DeltaBp, out.data(), out.size(), 4096, ColumnType::I64), v);
}

TEST(Selector, AllEqualPicksDictOverRle) {
  // Both are tiny; DICT wins because RLE_DICT carries 5 more header bytes
  // plus a run length, and ties/near-ties should favour the simpler decode.
  const std::vector<uint64_t> v(65'536, 42);
  std::vector<uint8_t> out;
  CandidateSizes s;
  EXPECT_EQ(encode_smallest(v.data(), v.size(), ColumnType::U32, out, &s), Encoding::Dict);
  EXPECT_EQ(s.dict, 9u + 4);
  EXPECT_LT(s.dict, s.rle_dict);
}

TEST(Selector, PruningMatchesTryingEveryCodec) {
  // The early exit in the dictionary scan must never change the answer.
  // Brute force: encode all four, pick the smallest with the same tie
  // order, and compare both the choice and the bytes.
  std::mt19937_64 rng(21);
  const ColumnType types[] = {ColumnType::U8, ColumnType::U16, ColumnType::U32, ColumnType::U64, ColumnType::I64};
  for (int trial = 0; trial < 60; ++trial) {
    const ColumnType type = types[static_cast<size_t>(trial) % 5];
    const size_t n = 1 + rng() % 3000;
    const uint64_t cardinality = 1 + rng() % (trial % 3 == 0 ? 4 : (trial % 3 == 1 ? 64 : n));
    const uint64_t run = 1 + rng() % 40;
    const uint64_t limit = type == ColumnType::U8 ? 256 : (type == ColumnType::U16 ? 65'536 : 1'000'000);
    std::vector<uint64_t> v(n);
    for (size_t i = 0; i < n; ++i) {
      uint64_t x = ((i / run) * 2654435761ULL) % cardinality % limit;
      if (type == ColumnType::I64) x = widen(static_cast<int64_t>(x) - 50);
      v[i] = x;
    }

    std::vector<uint8_t> out;
    CandidateSizes s;
    const Encoding chosen = encode_smallest(v.data(), v.size(), type, out, &s);

    const Dictionary dict = build_dictionary(v.data(), v.size(), type);
    std::vector<uint8_t> b_dict, b_rle, b_delta;
    encode_dict(dict, type, b_dict);
    encode_rle_dict(dict, type, b_rle);
    encode_delta_bp(v.data(), v.size(), b_delta);
    Encoding expect = Encoding::Plain;
    size_t best = n * value_width(type);
    const std::vector<uint8_t>* best_bytes = nullptr;
    auto consider = [&](Encoding e, const std::vector<uint8_t>& b) {
      if (b.size() < best) { best = b.size(); expect = e; best_bytes = &b; }
    };
    consider(Encoding::DeltaBp, b_delta);
    consider(Encoding::Dict, b_dict);
    consider(Encoding::RleDict, b_rle);

    ASSERT_EQ(chosen, expect) << "trial " << trial << " n=" << n << " card=" << cardinality << " run=" << run;
    if (best_bytes) ASSERT_EQ(out, *best_bytes) << "trial " << trial;
    else ASSERT_TRUE(out.empty());
    // When the selector did try the dictionary codecs, its sizes are the real ones.
    if (s.dict != 0) { EXPECT_EQ(s.dict, b_dict.size()); EXPECT_EQ(s.rle_dict, b_rle.size()); }
    EXPECT_EQ(s.delta_bp, b_delta.size());
  }
}
