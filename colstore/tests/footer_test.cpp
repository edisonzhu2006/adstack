// Footer encode/decode round trip, in memory and through a real file.

#include <gtest/gtest.h>

#include "src/format.h"
#include "src/reader.h"
#include "src/writer.h"
#include "tests/test_util.h"

using namespace colstore;
using namespace colstore::test;

namespace {

Footer synthetic_footer() {
  Footer f;
  f.rows_per_group = 4096;
  uint64_t offset = kHeaderSize;
  for (uint32_t g = 0; g < 3; ++g) {
    RowGroupMeta rg;
    rg.rows = g == 2 ? 17 : 4096;  // last group short
    for (size_t c = 0; c < kNumColumns; ++c) {
      ChunkMeta& m = rg.chunks[c];
      m.offset = offset;
      m.length = static_cast<uint64_t>(rg.rows) * value_width(all_columns()[c].type);
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

}  // namespace

TEST(Footer, EncodeDecodeRoundTrip) {
  const Footer f = synthetic_footer();
  const std::vector<uint8_t> bytes = encode_footer(f);
  EXPECT_EQ(bytes.size(), kFooterFixedSize + 3 * kRowGroupMetaWireSize);
  const Footer back = decode_footer(bytes.data(), bytes.size());
  EXPECT_EQ(back, f);
  EXPECT_EQ(as_signed(back.row_groups[0].chunks[4].min_bits), -5);
}

TEST(Footer, EmptyFooterRoundTrip) {
  Footer f;
  const std::vector<uint8_t> bytes = encode_footer(f);
  EXPECT_EQ(bytes.size(), kFooterFixedSize);
  EXPECT_EQ(decode_footer(bytes.data(), bytes.size()), f);
}

TEST(Footer, RejectsTruncatedAndInconsistent) {
  const Footer f = synthetic_footer();
  std::vector<uint8_t> bytes = encode_footer(f);
  EXPECT_THROW(decode_footer(bytes.data(), bytes.size() - 1), std::runtime_error);
  EXPECT_THROW(decode_footer(bytes.data(), kFooterFixedSize - 1), std::runtime_error);

  // Corrupt total_rows (bytes 16..23) and expect the consistency check to fire.
  bytes[16] ^= 0x01;
  EXPECT_THROW(decode_footer(bytes.data(), bytes.size()), std::runtime_error);
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
