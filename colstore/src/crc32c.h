#pragma once
// CRC-32C (Castagnoli), the checksum on every chunk and on the footer of a
// .col file (format version 3, FORMAT.md). Same polynomial and conventions
// as iSCSI, ext4 metadata, Parquet's page CRC option and RocksDB blocks:
// reflected polynomial 0x82F63B78, initial value and final xor 0xFFFFFFFF,
// so crc32c("123456789") == 0xE3069283.
//
// Why a checksum at all: the decoders check structure (counts, widths,
// code ranges, run lengths), but most single-bit flips leave a chunk
// structurally valid and only change a value. A flipped bit in the first
// value of a DELTA_BP block shifts every value of that block, and nothing
// short of a checksum (or the source file) can tell. Why CRC-32C and not
// a cryptographic hash or xxHash: it detects every burst error up to 32
// bits, which is the failure being guarded against (media and transfer
// corruption, not an adversary), and arm64 has an instruction for it
// (__crc32cd, 8 bytes per instruction), so checking every chunk on every
// read costs about a millisecond per 10 MB; NOTES.md has the measurement.
// x86-64 with SSE4.2 has the same instruction; anything else falls back to
// a table-driven loop (slicing by 8), slower but identical in result.

#include <cstddef>
#include <cstdint>

namespace colstore {

// CRC-32C of [data, data + len). Chaining: crc32c(b, nb, crc32c(a, na))
// equals the CRC of a followed by b.
uint32_t crc32c(const void* data, size_t len, uint32_t crc = 0);

// The table-driven version, compiled on every target so the tests can check
// the hardware path against it.
uint32_t crc32c_table(const void* data, size_t len, uint32_t crc = 0);

// Which implementation crc32c() uses on this build: "arm64-crc32c",
// "x86-sse4.2" or "table-slicing-by-8".
const char* crc32c_impl();

}  // namespace colstore
