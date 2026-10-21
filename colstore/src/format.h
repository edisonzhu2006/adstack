#pragma once
// colstore on-disk format (.col files): constants, the column table, and the
// footer structures with their byte encoding. This header is the single
// source of truth for the layout; colstore/FORMAT.md is the prose version.
//
// Layout of a .col file, in file order:
//
//   [header]      8 bytes: magic "COL1" + uint32 version
//   [row group 0] one encoded chunk per column, 10 chunks back to back
//   [row group 1] ...
//   ...
//   [footer]      per-row-group, per-column ChunkMeta (offset, length, rows,
//                 encoding, min, max, null count)
//   [trailer]     12 bytes: uint64 footer length + magic "COL1"
//
// The footer goes at the END so the writer is single-pass: it does not know
// chunk offsets or min/max until it has written the data, and seeking back
// to patch a header would make the file unreadable if the writer dies
// mid-way (a reader can tell "no trailer" from "valid file"). A reader seeks
// to the last 12 bytes, reads the footer length, reads the footer, and can
// then jump straight to any chunk. This is the same idea as Parquet and ORC.
//
// Byte helpers here duplicate the ones in common/event.cpp on purpose: those
// are in an anonymous namespace (private to that file) and the colstore
// track is not allowed to edit common/.

#include <array>
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
constexpr uint32_t kColVersion = 1;
constexpr size_t kHeaderSize = 8;    // magic + version
constexpr size_t kTrailerSize = 12;  // uint64 footer length + uint32 magic
constexpr uint32_t kDefaultRowsPerGroup = 65'536;
constexpr uint32_t kNumColumns = 10;

// ---- column table -----------------------------------------------------------

// One column per field of adstack::Event, in declaration order.
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

// Physical type of a column's values. Only the width and signedness matter
// for storage; the logical meaning (enum, timestamp) lives in event.h.
enum class ColumnType : uint32_t { U8 = 0, U16 = 1, U32 = 2, U64 = 3, I64 = 4 };

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
  ColumnId id;
  const char* name;
  ColumnType type;
};

const ColumnSpec& column_spec(ColumnId id);
const std::array<ColumnSpec, kNumColumns>& all_columns();
std::optional<ColumnId> column_by_name(std::string_view name);

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
  uint32_t null_count = 0;  // reserved, always 0 in v1: Event has no nullable field
  uint32_t reserved = 0;    // keeps the record 8-byte aligned; must be 0

  bool operator==(const ChunkMeta&) const = default;
};
constexpr size_t kChunkMetaWireSize = 8 + 8 + 4 + 4 + 8 + 8 + 4 + 4;

struct RowGroupMeta {
  uint32_t rows = 0;
  std::array<ChunkMeta, kNumColumns> chunks{};

  bool operator==(const RowGroupMeta&) const = default;
};

struct Footer {
  uint32_t rows_per_group = kDefaultRowsPerGroup;  // writer setting; last group may be shorter
  uint64_t total_rows = 0;
  std::vector<RowGroupMeta> row_groups;

  bool operator==(const Footer&) const = default;
};

// Footer body wire layout (no trailer):
//   uint32 num_row_groups, uint32 num_columns, uint32 rows_per_group,
//   uint32 reserved, uint64 total_rows,
//   then per row group: uint32 rows, then kNumColumns ChunkMeta records.
constexpr size_t kFooterFixedSize = 4 + 4 + 4 + 4 + 8;
constexpr size_t kRowGroupMetaWireSize = 4 + kNumColumns * kChunkMetaWireSize;

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
