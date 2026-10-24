// File integrity (format version 3): the CRC-32C implementation against the
// standard check value and against the table version, every single-bit
// flip anywhere in a .col file detected before a value is returned, and the
// decoder-level checks that back it up (DELTA_BP output wider than the
// column, a dictionary that is not strictly ascending).

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "src/crc32c.h"
#include "src/encoding.h"
#include "src/exec.h"
#include "src/reader.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

std::vector<uint8_t> read_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), {});
}

void write_file(const std::string& path, const std::vector<uint8_t>& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

// Opens the file and decodes every chunk two ways (the typed-agnostic read
// that verify and dump use, and a Scanner over every column, which is the
// query path). Returns true if all of it succeeded, false if anything threw.
bool reads_cleanly(const std::string& path) {
  try {
    ColReader r(path);
    for (size_t g = 0; g < r.num_row_groups(); ++g) {
      for (size_t c = 0; c < r.schema().size(); ++c) (void)r.read_column_bits(g, column_id(c));
    }
    std::vector<ColumnId> all;
    for (size_t c = 0; c < r.schema().size(); ++c) all.push_back(column_id(c));
    Scanner s(r, {}, all);
    Batch b;
    while (s.next(b)) {
    }
    return true;
  } catch (const std::runtime_error&) {
    return false;
  }
}

}  // namespace

TEST(Crc32c, StandardCheckValueAndChaining) {
  const std::string s = "123456789";
  EXPECT_EQ(crc32c(s.data(), s.size()), 0xE3069283u);  // the CRC-32C check value (RFC 3720 family)
  EXPECT_EQ(crc32c_table(s.data(), s.size()), 0xE3069283u);
  EXPECT_EQ(crc32c(nullptr, 0), 0u);
  EXPECT_EQ(crc32c(s.data() + 4, s.size() - 4, crc32c(s.data(), 4)), 0xE3069283u);
  // 32 zero bytes, from RFC 3720 appendix B.4.
  const std::vector<uint8_t> zeros(32, 0);
  EXPECT_EQ(crc32c(zeros.data(), zeros.size()), 0x8A9136AAu);
}

TEST(Crc32c, HardwarePathMatchesTableOnEveryLengthAndAlignment) {
  std::mt19937_64 rng(11);
  std::vector<uint8_t> buf(1100);
  for (uint8_t& b : buf) b = static_cast<uint8_t>(rng());
  for (size_t start = 0; start < 9; ++start) {
    for (size_t len = 0; len + start <= buf.size(); len += 1 + len / 7) {
      ASSERT_EQ(crc32c(buf.data() + start, len), crc32c_table(buf.data() + start, len))
          << crc32c_impl() << " start " << start << " len " << len;
    }
  }
}

TEST(Corruption, EverySingleBitFlipInTheFileIsDetected) {
  // Auto encodings so every codec is present (delta_bp, dict, rle_dict),
  // three row groups, and a few hundred bytes of footer.
  TempFile good("integrity.col");
  TempFile bad("integrity_bad.col");
  {
    ColWriter w(good.path(), 100, EncodingPolicy::Auto);
    for (uint64_t i = 0; i < 250; ++i) w.append(sample_event(i));
    w.finish();
  }
  ASSERT_TRUE(reads_cleanly(good.path()));
  const std::vector<uint8_t> bytes = read_file(good.path());
  // One flipped bit per byte, the bit chosen by position, so every byte of
  // header, chunks, footer and trailer is hit and every bit index is used.
  size_t undetected = 0;
  for (size_t i = 0; i < bytes.size(); ++i) {
    std::vector<uint8_t> copy = bytes;
    copy[i] ^= static_cast<uint8_t>(1u << (i % 8));
    write_file(bad.path(), copy);
    if (reads_cleanly(bad.path())) {
      ++undetected;
      ADD_FAILURE() << "flip at byte " << i << " of " << bytes.size() << " went unnoticed";
      if (undetected > 5) break;
    }
  }
  EXPECT_EQ(undetected, 0u);
}

TEST(Corruption, FlippedValueBitIsAChecksumErrorNotAWrongAnswer) {
  // The reviewer's case: a bit in the first value of a DELTA_BP block
  // decodes to a different, structurally valid value. It must throw.
  TempFile tmp("integrity_value.col");
  {
    ColWriter w(tmp.path(), 1000, EncodingPolicy::Auto);
    for (uint64_t i = 0; i < 1000; ++i) w.append(sample_event(i));
    w.finish();
  }
  std::vector<uint8_t> bytes = read_file(tmp.path());
  uint64_t offset = 0;
  {
    ColReader r(tmp.path());
    const ChunkMeta& m = r.chunk(0, ColumnId::CampaignId);
    ASSERT_EQ(m.encoding, static_cast<uint32_t>(Encoding::DeltaBp));
    offset = m.offset;
  }
  bytes[offset + 4 + 4] ^= 0x80;  // u32 count, then byte 4 of the block's first u64 value
  write_file(tmp.path(), bytes);
  ColReader r(tmp.path());  // the footer is intact
  try {
    (void)r.read_column_bits(0, ColumnId::CampaignId);
    FAIL() << "corrupt chunk was decoded";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("checksum mismatch"), std::string::npos) << e.what();
    EXPECT_NE(std::string(e.what()).find("campaign_id"), std::string::npos) << e.what();
  }
  EXPECT_THROW(run_query(r, {}, {ColumnId::CampaignId}, [](const Batch&) { return true; }), std::runtime_error);
}

TEST(Corruption, DeltaBpValueWiderThanTheColumnThrows) {
  // A DELTA_BP chunk is arithmetic, so it can produce bits a u8/u16/u32
  // column cannot hold. Encode values that need 33 bits and decode as u32.
  const std::vector<uint64_t> wide = {1, 2, (uint64_t{1} << 32) + 5, 7};
  std::vector<uint8_t> bytes;
  encode_delta_bp(wide.data(), wide.size(), bytes);
  EXPECT_EQ(decode_chunk(Encoding::DeltaBp, bytes.data(), bytes.size(), 4, ColumnType::U64), wide);
  EXPECT_THROW(decode_chunk(Encoding::DeltaBp, bytes.data(), bytes.size(), 4, ColumnType::U32), std::runtime_error);
  const std::vector<uint64_t> u8_ok = {0, 255, 3};
  bytes.clear();
  encode_delta_bp(u8_ok.data(), u8_ok.size(), bytes);
  EXPECT_EQ(decode_chunk(Encoding::DeltaBp, bytes.data(), bytes.size(), 3, ColumnType::U8), u8_ok);
}

TEST(Corruption, DictionaryMustBeStrictlyAscending) {
  // DICT, n=2, d=2, values (u8) then code width 1, codes 0 and 1.
  std::vector<uint8_t> ok = {2, 0, 0, 0, 2, 0, 0, 0, 10, 20, 1, 0b10};
  EXPECT_EQ(decode_dict(ok.data(), ok.size(), 2, ColumnType::U8), (std::vector<uint64_t>{10, 20}));
  std::vector<uint8_t> unsorted = ok;
  unsorted[8] = 30;  // 30, 20: the value path would decode it, the code-range filter would be wrong
  EXPECT_THROW(decode_dict(unsorted.data(), unsorted.size(), 2, ColumnType::U8), std::runtime_error);
  std::vector<uint8_t> dup = ok;
  dup[9] = 10;  // 10, 10: not distinct
  EXPECT_THROW(decode_dict(dup.data(), dup.size(), 2, ColumnType::U8), std::runtime_error);
  // Signed order: -1 sorts before 0 even though its pattern is larger.
  std::vector<uint8_t> neg = {2, 0, 0, 0, 2, 0, 0, 0};
  for (int k = 0; k < 8; ++k) neg.push_back(0xFF);  // -1 as i64
  for (int k = 0; k < 8; ++k) neg.push_back(0x00);  // 0
  neg.push_back(1);
  neg.push_back(0b10);
  EXPECT_EQ(decode_dict(neg.data(), neg.size(), 2, ColumnType::I64), (std::vector<uint64_t>{~uint64_t{0}, 0}));
  EXPECT_THROW(decode_dict(neg.data(), neg.size(), 2, ColumnType::U64), std::runtime_error);
}
