#pragma once
// colstore on-disk format (.col files): constants, the schema (column list),
// and the footer structures with their byte encoding. This header is the
// single source of truth for the layout; colstore/FORMAT.md is the prose
// version.
//
// Layout of a .col file, in file order:
//
//   [header]      8 bytes: magic "COL1" + uint32 version
//   [row group 0] one encoded chunk per column, in schema order
//   [row group 1] ...
//   ...
//   [footer]      the schema (column names and types), then per-row-group,
//                 per-column ChunkMeta (offset, length, rows, encoding,
//                 min, max, null count)
//   [trailer]     12 bytes: uint64 footer length + magic "COL1"
//
// The footer goes at the END so the writer is single-pass: it does not know
// chunk offsets or min/max until it has written the data, and seeking back
// to patch a header would make the file unreadable if the writer dies
// mid-way (a reader can tell "no trailer" from "valid file"). A reader seeks
// to the last 12 bytes, reads the footer length, reads the footer, and can
// then jump straight to any chunk. This is the same idea as Parquet and ORC.
//
// The schema lives in the file (version 2) rather than in this header
// (version 1 hardcoded the ten adstack::Event fields) because colstore now
// stores two kinds of table: raw events and streamkit's per-window
// aggregates (six columns). A reader learns the column names and physical
// types from the footer and everything downstream (scan, predicates,
// GROUP BY) works on "column i of type t"; the Event layout is just the
// schema that `event_schema()` returns. Only integer physical types exist:
// every field of both tables is an integer, and keeping the type system
// that small is what keeps the codecs one implementation each.
//
// Byte helpers here duplicate the ones in common/event.cpp on purpose: those
// are in an anonymous namespace (private to that file) and the colstore
// track is not allowed to edit common/.

#include <bit>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace colstore {

// ---- constants ------------------------------------------------------------

constexpr uint32_t kColMagic = 0x314C4F43;  // "COL1" read as little-endian
constexpr uint32_t kColVersion = 2;         // 1 = fixed Event columns, no schema in the footer
constexpr size_t kHeaderSize = 8;           // magic + version
constexpr size_t kTrailerSize = 12;         // uint64 footer length + uint32 magic
constexpr uint32_t kDefaultRowsPerGroup = 65'536;
// Upper bound on columns per file, so a corrupt footer cannot make the
// reader allocate wildly. Far above anything this project writes (10).
constexpr uint32_t kMaxColumns = 4096;
constexpr size_t kMaxColumnNameLen = 255;

// ---- schema -------------------------------------------------------------------

// Physical type of a column's values. Only the width and signedness matter
// for storage; the logical meaning (enum, timestamp) lives with the table.
enum class ColumnType : uint32_t { U8 = 0, U16 = 1, U32 = 2, U64 = 3, I64 = 4 };
constexpr uint32_t kNumColumnTypes = 5;

// Encoding id stored per chunk. Readers dispatch on this field, so adding
// an id changes no byte of the layout. Byte layouts are in encoding.h and
// FORMAT.md.
enum class Encoding : uint32_t {
  Plain = 0,    // values at natural width, in row order (memcpy)
  Dict = 1,     // sorted dictionary + bit-packed codes (low cardinality)
  DeltaBp = 2,  // blocks of 128: first value, min delta, bit-packed deltas (near-monotone ints)
  RleDict = 3,  // dictionary + bit-packed (code, run length) pairs (long runs)
};
constexpr uint32_t kNumEncodings = 4;

struct ColumnSpec {
  std::string name;  // [A-Za-z_][A-Za-z0-9_]*, unique within a schema; it is what --where names
  ColumnType type = ColumnType::U64;
  bool operator==(const ColumnSpec&) const = default;
};

// A table's columns in chunk order. Column i of the schema is chunk i of
// every row group.
using Schema = std::vector<ColumnSpec>;

// A column is addressed by its index in the schema. ColumnId is an enum
// rather than a bare integer so that a column index cannot be confused
// with a row index or a row-group index at a call site; the named members
// are the indices of the EVENT schema (event_schema()), which is what most
// code and every test addresses by name. For any other schema, use
// column_id(i) / find_column(schema, name) and never a named member.
enum class ColumnId : uint32_t {
  EventId = 0,
  EventTimeUs = 1,
  IngestTimeUs = 2,
  EventType = 3,
  AdvertiserId = 4,
  CampaignId = 5,
  VideoId = 6,
  Country = 7,
  Device = 8,
  CostMicros = 9,
};
constexpr uint32_t kNumEventColumns = 10;

inline size_t column_index(ColumnId id) { return static_cast<size_t>(id); }
inline ColumnId column_id(size_t index) { return static_cast<ColumnId>(index); }

// One column per field of adstack::Event, in declaration order (the table
// `colstore ingest --in events.ads` writes).
const Schema& event_schema();
// streamkit's output: window_start_us (i64), campaign_id (u32), impressions,
// clicks, conversions (u64), cost_micros (i64); see common/agg.h for the
// CSV header it matches (the table `colstore ingest --in-csv` writes).
const Schema& aggregate_schema();

std::optional<ColumnId> find_column(const Schema& schema, std::string_view name);
// Bounds-checked schema[column_index(id)]; throws std::out_of_range.
const ColumnSpec& spec_of(const Schema& schema, ColumnId id);
// Throws std::invalid_argument on an empty schema, a bad or duplicate name,
// a bad type, or more than kMaxColumns columns.
void validate_schema(const Schema& schema);

size_t value_width(ColumnType t);  // bytes per value
bool is_signed(ColumnType t);
const char* column_type_name(ColumnType t);
const char* encoding_name(uint32_t encoding_id);

// ---- footer structures ------------------------------------------------------

// Everything a reader needs to fetch and decode one column chunk without
// touching any other byte of the file. 48 bytes on the wire.
struct ChunkMeta {
  uint64_t offset = 0;      // absolute byte offset of the chunk in the file
  uint64_t length = 0;      // encoded byte length of the chunk
  uint32_t rows = 0;        // values in the chunk
  uint32_t encoding = 0;    // Encoding
  uint64_t min_bits = 0;    // raw 8 bytes; signed columns are sign-extended
  uint64_t max_bits = 0;    //   (compare with is_signed(column type))
  uint32_t null_count = 0;  // reserved, always 0 in v1/v2: no table here has a nullable field
  uint32_t reserved = 0;    // keeps the record 8-byte aligned; must be 0

  bool operator==(const ChunkMeta&) const = default;
};
constexpr size_t kChunkMetaWireSize = 8 + 8 + 4 + 4 + 8 + 8 + 4 + 4;

struct RowGroupMeta {
  uint32_t rows = 0;
  std::vector<ChunkMeta> chunks;  // one per schema column, in schema order

  bool operator==(const RowGroupMeta&) const = default;
};

struct Footer {
  uint32_t rows_per_group = kDefaultRowsPerGroup;  // writer setting; last group may be shorter
  uint64_t total_rows = 0;
  Schema columns;
  std::vector<RowGroupMeta> row_groups;

  bool operator==(const Footer&) const = default;
};

// Footer body wire layout (no trailer):
//   uint32 num_row_groups, uint32 num_columns, uint32 rows_per_group,
//   uint32 reserved, uint64 total_rows,
//   then per column: uint32 type, uint32 name_len, name bytes (no NUL),
//   then per row group: uint32 rows, then num_columns ChunkMeta records.
constexpr size_t kFooterFixedSize = 4 + 4 + 4 + 4 + 8;
size_t schema_wire_size(const Schema& schema);
inline size_t row_group_meta_wire_size(size_t num_columns) { return 4 + num_columns * kChunkMetaWireSize; }
inline size_t footer_wire_size(const Footer& f) {
  return kFooterFixedSize + schema_wire_size(f.columns) + f.row_groups.size() * row_group_meta_wire_size(f.columns.size());
}

// Throws std::invalid_argument if f.columns is not a valid schema.
std::vector<uint8_t> encode_footer(const Footer& f);
// Throws std::runtime_error on a truncated or inconsistent footer.
Footer decode_footer(const uint8_t* data, size_t len);

// Signed/unsigned view of the raw min/max bits.
int64_t as_signed(uint64_t bits);

// ---- little-endian byte helpers ---------------------------------------------
// The whole format is little-endian. Every machine this runs on (arm64,
// x86-64) is little-endian, so PLAIN chunks are plain memcpys; the
// static_assert turns a silent corruption on a big-endian host into a
// compile error.
static_assert(std::endian::native == std::endian::little,
              "colstore assumes a little-endian host");

template <typename T>
inline void put_le(uint8_t*& p, T v) {
  static_assert(std::is_trivially_copyable_v<T>);
  std::memcpy(p, &v, sizeof(T));
  p += sizeof(T);
}

template <typename T>
inline T get_le(const uint8_t*& p) {
  static_assert(std::is_trivially_copyable_v<T>);
  T v;
  std::memcpy(&v, p, sizeof(T));
  p += sizeof(T);
  return v;
}

}  // namespace colstore
