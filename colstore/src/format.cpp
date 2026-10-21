// Column table lookups and footer encode/decode. See format.h for the layout.

#include "src/format.h"

#include <stdexcept>

namespace colstore {

namespace {

// Order must match ColumnId and the field order of adstack::Event.
constexpr std::array<ColumnSpec, kNumColumns> kColumns = {{
    {ColumnId::EventId, "event_id", ColumnType::U64},
    {ColumnId::EventTimeUs, "event_time_us", ColumnType::I64},
    {ColumnId::IngestTimeUs, "ingest_time_us", ColumnType::I64},
    {ColumnId::EventType, "event_type", ColumnType::U8},
    {ColumnId::AdvertiserId, "advertiser_id", ColumnType::U32},
    {ColumnId::CampaignId, "campaign_id", ColumnType::U32},
    {ColumnId::VideoId, "video_id", ColumnType::U64},
    {ColumnId::Country, "country", ColumnType::U16},
    {ColumnId::Device, "device", ColumnType::U8},
    {ColumnId::CostMicros, "cost_micros", ColumnType::I64},
}};

void put_chunk_meta(uint8_t*& p, const ChunkMeta& m) {
  put_le(p, m.offset);
  put_le(p, m.length);
  put_le(p, m.rows);
  put_le(p, m.encoding);
  put_le(p, m.min_bits);
  put_le(p, m.max_bits);
  put_le(p, m.null_count);
  put_le(p, m.reserved);
}

ChunkMeta get_chunk_meta(const uint8_t*& p) {
  ChunkMeta m;
  m.offset = get_le<uint64_t>(p);
  m.length = get_le<uint64_t>(p);
  m.rows = get_le<uint32_t>(p);
  m.encoding = get_le<uint32_t>(p);
  m.min_bits = get_le<uint64_t>(p);
  m.max_bits = get_le<uint64_t>(p);
  m.null_count = get_le<uint32_t>(p);
  m.reserved = get_le<uint32_t>(p);
  return m;
}

}  // namespace

const ColumnSpec& column_spec(ColumnId id) { return kColumns[static_cast<size_t>(id)]; }

const std::array<ColumnSpec, kNumColumns>& all_columns() { return kColumns; }

std::optional<ColumnId> column_by_name(std::string_view name) {
  for (const auto& c : kColumns) {
    if (name == c.name) return c.id;
  }
  return std::nullopt;
}

size_t value_width(ColumnType t) {
  switch (t) {
    case ColumnType::U8: return 1;
    case ColumnType::U16: return 2;
    case ColumnType::U32: return 4;
    case ColumnType::U64: return 8;
    case ColumnType::I64: return 8;
  }
  return 0;
}

bool is_signed(ColumnType t) { return t == ColumnType::I64; }

const char* column_type_name(ColumnType t) {
  switch (t) {
    case ColumnType::U8: return "u8";
    case ColumnType::U16: return "u16";
    case ColumnType::U32: return "u32";
    case ColumnType::U64: return "u64";
    case ColumnType::I64: return "i64";
  }
  return "?";
}

const char* encoding_name(uint32_t encoding_id) {
  switch (static_cast<Encoding>(encoding_id)) {
    case Encoding::Plain: return "plain";
    case Encoding::Dict: return "dict";
    case Encoding::DeltaBp: return "delta_bp";
    case Encoding::RleDict: return "rle_dict";
  }
  return "unknown";
}

int64_t as_signed(uint64_t bits) {
  int64_t v;
  std::memcpy(&v, &bits, sizeof v);  // bit-cast, well defined for two's complement
  return v;
}

std::vector<uint8_t> encode_footer(const Footer& f) {
  std::vector<uint8_t> out(kFooterFixedSize + f.row_groups.size() * kRowGroupMetaWireSize);
  uint8_t* p = out.data();
  put_le(p, static_cast<uint32_t>(f.row_groups.size()));
  put_le(p, kNumColumns);
  put_le(p, f.rows_per_group);
  put_le(p, uint32_t{0});
  put_le(p, f.total_rows);
  for (const auto& rg : f.row_groups) {
    put_le(p, rg.rows);
    for (const auto& c : rg.chunks) put_chunk_meta(p, c);
  }
  return out;
}

Footer decode_footer(const uint8_t* data, size_t len) {
  if (len < kFooterFixedSize) throw std::runtime_error("footer truncated");
  const uint8_t* p = data;
  const uint32_t num_groups = get_le<uint32_t>(p);
  const uint32_t num_columns = get_le<uint32_t>(p);
  Footer f;
  f.rows_per_group = get_le<uint32_t>(p);
  (void)get_le<uint32_t>(p);  // reserved
  f.total_rows = get_le<uint64_t>(p);

  if (num_columns != kNumColumns) throw std::runtime_error("footer: unexpected column count");
  if (len != kFooterFixedSize + static_cast<size_t>(num_groups) * kRowGroupMetaWireSize) {
    throw std::runtime_error("footer: length does not match row group count");
  }

  f.row_groups.resize(num_groups);
  uint64_t rows_seen = 0;
  for (auto& rg : f.row_groups) {
    rg.rows = get_le<uint32_t>(p);
    for (auto& c : rg.chunks) {
      c = get_chunk_meta(p);
      if (c.rows != rg.rows) throw std::runtime_error("footer: chunk row count mismatch");
    }
    rows_seen += rg.rows;
  }
  if (rows_seen != f.total_rows) throw std::runtime_error("footer: total_rows mismatch");
  return f;
}

}  // namespace colstore
