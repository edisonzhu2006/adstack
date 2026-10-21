// Chunk addressing and per-chunk encodings: the footer's (offset, length)
// for a chunk must point at exactly the bytes a raw fseek/fread would find,
// chunks must tile the data region with no gaps, min/max must describe the
// chunk's contents, and with automatic encoding each sample column must
// land on the codec its shape calls for and still read back identically.

#include <cstdio>

#include <gtest/gtest.h>

#include "src/encoding.h"
#include "src/reader.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

constexpr uint64_t kRows = 1'000;
constexpr uint32_t kGroup = 256;  // -> groups of 256,256,256,232

void write_sample_file(const std::string& path, EncodingPolicy policy) {
  ColWriter w(path, kGroup, policy);
  for (uint64_t i = 0; i < kRows; ++i) w.append(sample_event(i));
  w.finish();
}

// PLAIN-only file: every chunk is rows * width bytes, so addressing can be
// checked arithmetically.
struct Fixture : ::testing::Test {
  TempFile tmp{"chunk_plain.col"};
  void SetUp() override { write_sample_file(tmp.path(), EncodingPolicy::PlainOnly); }
};

// Default (Auto) file: the writer picks an encoding per chunk.
struct AutoFixture : ::testing::Test {
  TempFile tmp{"chunk_auto.col"};
  void SetUp() override { write_sample_file(tmp.path(), EncodingPolicy::Auto); }
};

}  // namespace

TEST_F(Fixture, ReadColumnMatchesExpectedValues) {
  ColReader r(tmp.path());
  ASSERT_EQ(r.num_row_groups(), 4u);
  const auto campaigns = r.read_column<uint32_t>(2, ColumnId::CampaignId);
  ASSERT_EQ(campaigns.size(), kGroup);
  for (uint32_t i = 0; i < kGroup; ++i) {
    EXPECT_EQ(campaigns[i], sample_event(2 * kGroup + i).campaign_id);
  }
  const auto costs = r.read_column<int64_t>(3, ColumnId::CostMicros);
  ASSERT_EQ(costs.size(), kRows - 3 * kGroup);
  EXPECT_EQ(costs[0], sample_event(3 * kGroup).cost_micros);
}

TEST_F(Fixture, OffsetAndLengthAddressTheChunkBytes) {
  ColReader r(tmp.path());
  const ChunkMeta& m = r.chunk(1, ColumnId::EventTimeUs);
  EXPECT_EQ(m.rows, kGroup);
  EXPECT_EQ(m.length, kGroup * sizeof(int64_t));
  EXPECT_EQ(m.encoding, static_cast<uint32_t>(Encoding::Plain));

  // Independent read: raw fseek/fread at the recorded offset, bypassing ColReader.
  FILE* f = std::fopen(tmp.path().c_str(), "rb");
  ASSERT_NE(f, nullptr);
  ASSERT_EQ(std::fseek(f, static_cast<long>(m.offset), SEEK_SET), 0);
  std::vector<uint8_t> raw(static_cast<size_t>(m.length));
  ASSERT_EQ(std::fread(raw.data(), 1, raw.size(), f), raw.size());
  std::fclose(f);

  const auto values = decode_plain<int64_t>(raw.data(), raw.size(), m.rows);
  for (uint32_t i = 0; i < kGroup; ++i) {
    EXPECT_EQ(values[i], sample_event(kGroup + i).event_time_us);
  }
  EXPECT_EQ(r.read_chunk_bytes(1, ColumnId::EventTimeUs), raw);
}

TEST_F(Fixture, ChunksTileTheDataRegion) {
  ColReader r(tmp.path());
  uint64_t expect = kHeaderSize;
  for (const auto& rg : r.footer().row_groups) {
    ASSERT_EQ(rg.chunks.size(), kNumEventColumns);
    for (size_t c = 0; c < kNumEventColumns; ++c) {
      const ChunkMeta& m = rg.chunks[c];
      EXPECT_EQ(m.offset, expect) << "group/col gap";
      EXPECT_EQ(m.length, static_cast<uint64_t>(m.rows) * value_width(event_schema()[c].type));
      expect += m.length;
    }
  }
  EXPECT_EQ(expect, r.file_size() - r.footer_bytes() - kTrailerSize);
}

TEST_F(Fixture, MinMaxDescribeChunk) {
  ColReader r(tmp.path());
  // cost_micros = 3*i - 1000 is monotone, so min is the first row, max the last.
  const ChunkMeta& cost = r.chunk(0, ColumnId::CostMicros);
  EXPECT_EQ(as_signed(cost.min_bits), sample_event(0).cost_micros);  // -1000
  EXPECT_EQ(as_signed(cost.max_bits), sample_event(kGroup - 1).cost_micros);
  EXPECT_LT(as_signed(cost.min_bits), 0);

  const ChunkMeta& id = r.chunk(3, ColumnId::EventId);
  EXPECT_EQ(id.min_bits, 3 * kGroup + 1);
  EXPECT_EQ(id.max_bits, kRows);
  EXPECT_EQ(id.null_count, 0u);
}

TEST_F(Fixture, WrongValueTypeThrows) {
  ColReader r(tmp.path());
  EXPECT_THROW(r.read_column<uint32_t>(0, ColumnId::EventId), std::runtime_error);
  EXPECT_THROW(r.read_column<uint64_t>(0, ColumnId::Device), std::runtime_error);
  EXPECT_THROW(r.chunk(99, ColumnId::Device), std::out_of_range);
}

TEST_F(AutoFixture, EveryColumnReadsBackIdentically) {
  ColReader r(tmp.path());
  uint64_t row = 0;
  for (size_t g = 0; g < r.num_row_groups(); ++g) {
    for (const adstack::Event& e : r.read_events(g)) {
      ASSERT_EQ(e, sample_event(row)) << "row " << row;
      ++row;
    }
  }
  EXPECT_EQ(row, kRows);
}

TEST_F(AutoFixture, ColumnsLandOnTheExpectedEncoding) {
  ColReader r(tmp.path());
  auto enc = [&](ColumnId c) { return static_cast<Encoding>(r.chunk(0, c).encoding); };
  // sample_event: event_time_us steps by a constant 101 -> deltas all equal,
  // DELTA_BP at width 0. event_id is i+1, same story. video_id is i times
  // a constant (wrapping), so its deltas are constant too.
  EXPECT_EQ(enc(ColumnId::EventTimeUs), Encoding::DeltaBp);
  EXPECT_EQ(enc(ColumnId::IngestTimeUs), Encoding::DeltaBp);
  EXPECT_EQ(enc(ColumnId::EventId), Encoding::DeltaBp);
  EXPECT_EQ(enc(ColumnId::VideoId), Encoding::DeltaBp);
  EXPECT_EQ(enc(ColumnId::CostMicros), Encoding::DeltaBp);
  // country alternates between two values, device between 0 and 1,
  // event_type cycles through three: tiny dictionaries, runs of length 1.
  EXPECT_EQ(enc(ColumnId::Country), Encoding::Dict);
  EXPECT_EQ(enc(ColumnId::Device), Encoding::Dict);
  EXPECT_EQ(enc(ColumnId::EventType), Encoding::Dict);
  // Encoded chunks are smaller than PLAIN would be, and info's sum works.
  for (const auto& rg : r.footer().row_groups) {
    for (size_t c = 0; c < kNumEventColumns; ++c) {
      const ChunkMeta& m = rg.chunks[c];
      EXPECT_LE(m.length, static_cast<uint64_t>(m.rows) * value_width(event_schema()[c].type));
    }
  }
}

TEST_F(AutoFixture, MinMaxStoredForEncodedChunks) {
  ColReader r(tmp.path());
  const ChunkMeta& cost = r.chunk(0, ColumnId::CostMicros);
  EXPECT_EQ(as_signed(cost.min_bits), sample_event(0).cost_micros);  // -1000
  EXPECT_EQ(as_signed(cost.max_bits), sample_event(kGroup - 1).cost_micros);
  const ChunkMeta& country = r.chunk(1, ColumnId::Country);
  EXPECT_EQ(country.min_bits, 356u);
  EXPECT_EQ(country.max_bits, 840u);
}

TEST_F(AutoFixture, ReadColumnBitsMatchesTypedRead) {
  ColReader r(tmp.path());
  const auto bits = r.read_column_bits(2, ColumnId::CostMicros);
  const auto typed = r.read_column<int64_t>(2, ColumnId::CostMicros);
  ASSERT_EQ(bits.size(), typed.size());
  for (size_t i = 0; i < bits.size(); ++i) EXPECT_EQ(as_signed(bits[i]), typed[i]);
}
