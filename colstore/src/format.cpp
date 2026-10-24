// Schema tables, lookups and footer encode/decode. See format.h for the layout.

#include "src/format.h"

#include <cctype>
#include <stdexcept>

#include "src/crc32c.h"

namespace colstore {

namespace {

void put_chunk_meta(uint8_t*& p, const ChunkMeta& m) {
  put_le(p, m.offset);
  put_le(p, m.length);
  put_le(p, m.rows);
  put_le(p, m.encoding);
  put_le(p, m.min_bits);
  put_le(p, m.max_bits);
  put_le(p, m.null_count);
  put_le(p, m.crc32c);
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
  m.crc32c = get_le<uint32_t>(p);
  return m;
}

// CRC-32C of a footer body with its checksum field read as zero.
uint32_t footer_checksum(const uint8_t* data, size_t len) {
  static constexpr uint8_t kZero[4] = {0, 0, 0, 0};
  uint32_t c = crc32c(data, kFooterCrcOffset);
  c = crc32c(kZero, 4, c);
  return crc32c(data + kFooterCrcOffset + 4, len - kFooterCrcOffset - 4, c);
}

// The --where grammar's identifier rule (predicate.cpp): a name that could
// not be typed in a predicate would be a column nobody can query.
bool valid_column_name(std::string_view s) {
  if (s.empty() || s.size() > kMaxColumnNameLen) return false;
  if (!(std::isalpha(static_cast<unsigned char>(s[0])) || s[0] == '_')) return false;
  for (char c : s) {
    if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) return false;
  }
  return true;
}

}  // namespace

// ---- the two tables ---------------------------------------------------------------

const Schema& event_schema() {
  // Order must match ColumnId's named members and the field order of
  // adstack::Event (predicate.cpp's event_field_bits() switches on both).
  static const Schema s = {
      {"event_id", ColumnType::U64},      {"event_time_us", ColumnType::I64}, {"ingest_time_us", ColumnType::I64},
      {"event_type", ColumnType::U8},     {"advertiser_id", ColumnType::U32}, {"campaign_id", ColumnType::U32},
      {"video_id", ColumnType::U64},      {"country", ColumnType::U16},       {"device", ColumnType::U8},
      {"cost_micros", ColumnType::I64},
  };
  return s;
}

const Schema& aggregate_schema() {
  // Same names and order as write_csv_header() in common/agg.h, so the CSV
  // header is the check that a file really is streamkit output. Types are
  // those of adstack::Agg and WindowKey.
  static const Schema s = {
      {"window_start_us", ColumnType::I64}, {"campaign_id", ColumnType::U32}, {"impressions", ColumnType::U64},
      {"clicks", ColumnType::U64},          {"conversions", ColumnType::U64}, {"cost_micros", ColumnType::I64},
  };
  return s;
}

std::optional<ColumnId> find_column(const Schema& schema, std::string_view name) {
  for (size_t i = 0; i < schema.size(); ++i) {
    if (schema[i].name == name) return column_id(i);
  }
  return std::nullopt;
}

const ColumnSpec& spec_of(const Schema& schema, ColumnId id) {
  const size_t i = column_index(id);
  if (i >= schema.size()) throw std::out_of_range("column index " + std::to_string(i) + " outside the schema");
  return schema[i];
}

void validate_schema(const Schema& schema) {
  if (schema.empty()) throw std::invalid_argument("schema: a table needs at least one column");
  if (schema.size() > kMaxColumns) throw std::invalid_argument("schema: too many columns");
  for (size_t i = 0; i < schema.size(); ++i) {
    const ColumnSpec& c = schema[i];
    if (!valid_column_name(c.name)) throw std::invalid_argument("schema: bad column name '" + c.name + "'");
    if (static_cast<uint32_t>(c.type) >= kNumColumnTypes) throw std::invalid_argument("schema: bad type for " + c.name);
    for (size_t j = 0; j < i; ++j) {
      if (schema[j].name == c.name) throw std::invalid_argument("schema: duplicate column name '" + c.name + "'");
    }
  }
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

// ---- footer ------------------------------------------------------------------------

size_t schema_wire_size(const Schema& schema) {
  size_t n = 0;
  for (const ColumnSpec& c : schema) n += 4 + 4 + c.name.size();
  return n;
}

std::vector<uint8_t> encode_footer(const Footer& f) {
  validate_schema(f.columns);
  for (const RowGroupMeta& rg : f.row_groups) {
    if (rg.chunks.size() != f.columns.size()) throw std::invalid_argument("footer: row group has the wrong chunk count");
  }
  std::vector<uint8_t> out(footer_wire_size(f));
  uint8_t* p = out.data();
  put_le(p, static_cast<uint32_t>(f.row_groups.size()));
  put_le(p, static_cast<uint32_t>(f.columns.size()));
  put_le(p, f.rows_per_group);
  put_le(p, uint32_t{0});
  put_le(p, f.total_rows);
  for (const ColumnSpec& c : f.columns) {
    put_le(p, static_cast<uint32_t>(c.type));
    put_le(p, static_cast<uint32_t>(c.name.size()));
    std::memcpy(p, c.name.data(), c.name.size());
    p += c.name.size();
  }
  for (const auto& rg : f.row_groups) {
    put_le(p, rg.rows);
    for (const auto& c : rg.chunks) put_chunk_meta(p, c);
  }
  reseal_footer(out);
  return out;
}

void reseal_footer(std::vector<uint8_t>& body) {
  if (body.size() < kFooterFixedSize) throw std::invalid_argument("reseal_footer: not a footer");
  const uint32_t c = footer_checksum(body.data(), body.size());
  uint8_t* p = body.data() + kFooterCrcOffset;
  put_le(p, c);
}

Footer decode_footer(const uint8_t* data, size_t len) {
  if (len < kFooterFixedSize) throw std::runtime_error("footer truncated");
  {
    const uint8_t* q = data + kFooterCrcOffset;
    if (get_le<uint32_t>(q) != footer_checksum(data, len)) {
      throw std::runtime_error("footer checksum mismatch: the footer is corrupt or truncated");
    }
  }
  const uint8_t* p = data;
  const uint8_t* const end = data + len;
  const uint32_t num_groups = get_le<uint32_t>(p);
  const uint32_t num_columns = get_le<uint32_t>(p);
  Footer f;
  f.rows_per_group = get_le<uint32_t>(p);
  (void)get_le<uint32_t>(p);  // footer_crc32c, checked above
  f.total_rows = get_le<uint64_t>(p);

  if (num_columns == 0 || num_columns > kMaxColumns) throw std::runtime_error("footer: bad column count");
  // The schema is variable-length, so it is read with a bounds check per
  // field; the fixed-size part after it is checked once against what is left.
  f.columns.resize(num_columns);
  for (ColumnSpec& c : f.columns) {
    if (static_cast<size_t>(end - p) < 8) throw std::runtime_error("footer: schema truncated");
    const uint32_t type = get_le<uint32_t>(p);
    const uint32_t name_len = get_le<uint32_t>(p);
    if (name_len > kMaxColumnNameLen || static_cast<size_t>(end - p) < name_len) {
      throw std::runtime_error("footer: schema truncated");
    }
    c.type = static_cast<ColumnType>(type);
    c.name.assign(reinterpret_cast<const char*>(p), name_len);
    p += name_len;
  }
  try {
    validate_schema(f.columns);
  } catch (const std::invalid_argument& ex) {
    throw std::runtime_error(std::string("footer: ") + ex.what());
  }
  if (static_cast<size_t>(end - p) != static_cast<size_t>(num_groups) * row_group_meta_wire_size(num_columns)) {
    throw std::runtime_error("footer: length does not match row group count");
  }

  f.row_groups.resize(num_groups);
  uint64_t rows_seen = 0;
  for (auto& rg : f.row_groups) {
    rg.rows = get_le<uint32_t>(p);
    rg.chunks.resize(num_columns);
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
