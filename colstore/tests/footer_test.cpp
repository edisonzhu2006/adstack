// Footer encode/decode round trip, in memory and through a real file.

#include <gtest/gtest.h>

#include "src/format.h"
#include "src/reader.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

Footer synthetic_footer(const Schema& schema = event_schema()) {
  Footer f;
  f.rows_per_group = 4096;
  f.columns = schema;
  uint64_t offset = kHeaderSize;
  for (uint32_t g = 0; g < 3; ++g) {
    RowGroupMeta rg;
    rg.rows = g == 2 ? 17 : 4096;  // last group short
    rg.chunks.resize(schema.size());
    for (size_t c = 0; c < schema.size(); ++c) {
      ChunkMeta& m = rg.chunks[c];
      m.offset = offset;
      m.length = static_cast<uint64_t>(rg.rows) * value_width(schema[c].type);
      m.rows = rg.rows;
      m.encoding = static_cast<uint32_t>(Encoding::Plain);
      m.min_bits = static_cast<uint64_t>(-static_cast<int64_t>(c + 1));  // negative, sign-extended
      m.max_bits = c * 1000 + g;
      offset += m.length;
    }
    f.total_rows += rg.rows;
    f.row_groups.push_back(rg);
  }
  return f;
}

template <typename F>
void expect_throw_containing(F f, const std::string& needle) {
  try {
    f();
    ADD_FAILURE() << "no exception, expected one containing '" << needle << "'";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find(needle), std::string::npos) << e.what();
  }
}

}  // namespace

TEST(Footer, EncodeDecodeRoundTrip) {
  const Footer f = synthetic_footer();
  const std::vector<uint8_t> bytes = encode_footer(f);
  // 24 fixed + the schema (8 + name bytes per column) + 3 groups of (4 + 10 * 48).
  EXPECT_EQ(bytes.size(), kFooterFixedSize + schema_wire_size(event_schema()) + 3 * row_group_meta_wire_size(10));
  EXPECT_EQ(schema_wire_size(event_schema()), 10 * 8 + std::string("event_idevent_time_usingest_time_usevent_type"
                                                                  "advertiser_idcampaign_idvideo_idcountrydevicecost_micros").size());
  const Footer back = decode_footer(bytes.data(), bytes.size());
  EXPECT_EQ(back, f);
  EXPECT_EQ(back.columns, event_schema());
  EXPECT_EQ(as_signed(back.row_groups[0].chunks[4].min_bits), -5);
}

TEST(Footer, AggregateSchemaRoundTrip) {
  // The six-column table streamkit's CSV becomes: same codec, different
  // column list, and the row-group records shrink with it.
  const Footer f = synthetic_footer(aggregate_schema());
  const std::vector<uint8_t> bytes = encode_footer(f);
  EXPECT_EQ(bytes.size(), kFooterFixedSize + schema_wire_size(aggregate_schema()) + 3 * row_group_meta_wire_size(6));
  const Footer back = decode_footer(bytes.data(), bytes.size());
  EXPECT_EQ(back, f);
  ASSERT_EQ(back.columns.size(), 6u);
  EXPECT_EQ(back.columns[0], (ColumnSpec{"window_start_us", ColumnType::I64}));
  EXPECT_EQ(back.columns[5], (ColumnSpec{"cost_micros", ColumnType::I64}));
  EXPECT_EQ(back.row_groups[1].chunks.size(), 6u);
}

TEST(Footer, EmptyFooterRoundTrip) {
  Footer f;
  f.columns = event_schema();
  const std::vector<uint8_t> bytes = encode_footer(f);
  EXPECT_EQ(bytes.size(), kFooterFixedSize + schema_wire_size(event_schema()));
  EXPECT_EQ(decode_footer(bytes.data(), bytes.size()), f);
  // No schema at all is not a table.
  EXPECT_THROW(encode_footer(Footer{}), std::invalid_argument);
}

TEST(Footer, RejectsTruncatedAndInconsistent) {
  const Footer f = synthetic_footer();
  std::vector<uint8_t> bytes = encode_footer(f);
  EXPECT_THROW(decode_footer(bytes.data(), bytes.size() - 1), std::runtime_error);
  EXPECT_THROW(decode_footer(bytes.data(), kFooterFixedSize - 1), std::runtime_error);
  EXPECT_THROW(decode_footer(bytes.data(), kFooterFixedSize + 3), std::runtime_error);  // inside the schema

  // Corrupt total_rows (bytes 16..23): the checksum fires first; resealed
  // (as if the writer itself had written the wrong count), the consistency
  // check behind it fires.
  bytes[16] ^= 0x01;
  expect_throw_containing([&] { decode_footer(bytes.data(), bytes.size()); }, "checksum");
  reseal_footer(bytes);
  expect_throw_containing([&] { decode_footer(bytes.data(), bytes.size()); }, "total_rows");
  bytes[16] ^= 0x01;
  reseal_footer(bytes);
  EXPECT_EQ(decode_footer(bytes.data(), bytes.size()), f);

  // A row group whose chunk list does not match the schema cannot be encoded.
  Footer short_group = f;
  short_group.row_groups[1].chunks.pop_back();
  EXPECT_THROW(encode_footer(short_group), std::invalid_argument);
}

TEST(Footer, RejectsBadSchemas) {
  auto with = [](Schema columns) {
    Footer f;
    f.columns = std::move(columns);
    return f;
  };
  EXPECT_THROW(encode_footer(with({{"", ColumnType::U8}})), std::invalid_argument);           // empty name
  EXPECT_THROW(encode_footer(with({{"1abc", ColumnType::U8}})), std::invalid_argument);       // leading digit
  EXPECT_THROW(encode_footer(with({{"a-b", ColumnType::U8}})), std::invalid_argument);        // not an identifier
  EXPECT_THROW(encode_footer(with({{"a", ColumnType::U8}, {"a", ColumnType::U8}})), std::invalid_argument);  // dup
  EXPECT_THROW(encode_footer(with({{"a", static_cast<ColumnType>(99)}})), std::invalid_argument);  // bad type
  EXPECT_NO_THROW(encode_footer(with({{"_ok_1", ColumnType::I64}})));

  // The same rules on the way in: patch a decoded footer's type byte.
  std::vector<uint8_t> bytes = encode_footer(with({{"a", ColumnType::U8}}));
  bytes[kFooterFixedSize] = 99;  // the first column's type
  reseal_footer(bytes);           // reach the schema check, not the checksum
  expect_throw_containing([&] { decode_footer(bytes.data(), bytes.size()); }, "bad type");
}

TEST(Footer, ChecksumCoversEveryByte) {
  const std::vector<uint8_t> bytes = encode_footer(synthetic_footer());
  for (size_t i = 0; i < bytes.size(); ++i) {
    std::vector<uint8_t> copy = bytes;
    copy[i] ^= static_cast<uint8_t>(1u << (i % 8));
    EXPECT_THROW(decode_footer(copy.data(), copy.size()), std::runtime_error) << "byte " << i;
  }
}

TEST(Schema, LookupsAndIndices) {
  EXPECT_EQ(find_column(event_schema(), "campaign_id"), ColumnId::CampaignId);
  EXPECT_EQ(find_column(aggregate_schema(), "campaign_id"), column_id(1));
  EXPECT_EQ(find_column(aggregate_schema(), "event_id"), std::nullopt);
  EXPECT_EQ(spec_of(event_schema(), ColumnId::CostMicros).type, ColumnType::I64);
  EXPECT_THROW(spec_of(aggregate_schema(), ColumnId::CostMicros), std::out_of_range);  // index 9 of 6
  EXPECT_EQ(column_index(column_id(7)), 7u);
  EXPECT_EQ(event_schema().size(), kNumEventColumns);
  EXPECT_NE(event_schema(), aggregate_schema());
}

TEST(Footer, SurvivesWriteAndOpen) {
  TempFile tmp("footer.col");
  // PLAIN only, so the file size below can be computed arithmetically.
  ColWriter w(tmp.path(), 100, EncodingPolicy::PlainOnly);
  for (uint64_t i = 0; i < 250; ++i) w.append(sample_event(i));
  w.finish();
  const Footer written = w.footer();

  ColReader r(tmp.path());
  EXPECT_EQ(r.footer(), written);
  EXPECT_EQ(r.total_rows(), 250u);
  EXPECT_EQ(r.num_row_groups(), 3u);
  EXPECT_EQ(r.footer_bytes(), encode_footer(written).size());
  EXPECT_EQ(r.file_size(), kHeaderSize + 250 * adstack::kEventWireSize + r.footer_bytes() + kTrailerSize);
}

TEST(Footer, ReaderRejectsFileWithoutTrailer) {
  TempFile tmp("notrailer.col");
  {
    ColWriter w(tmp.path(), 100);
    for (uint64_t i = 0; i < 50; ++i) w.append(sample_event(i));
    // no finish(): header is written, data may be buffered, no trailer
  }
  EXPECT_THROW(ColReader r(tmp.path()), std::runtime_error);
}
