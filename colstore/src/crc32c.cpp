// CRC-32C: hardware instruction where the target has one, else slicing by
// 8. See crc32c.h.

#include "src/crc32c.h"

#include <array>
#include <cstring>

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#elif defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

namespace colstore {

namespace {

constexpr uint32_t kPoly = 0x82F63B78u;  // reflected Castagnoli polynomial

// table[k][b]: CRC contribution of byte b followed by k zero bytes, so eight
// bytes are folded per step with eight lookups instead of eight dependent
// shift loops.
using Tables = std::array<std::array<uint32_t, 256>, 8>;
constexpr Tables make_tables() {
  Tables t{};
  for (uint32_t b = 0; b < 256; ++b) {
    uint32_t c = b;
    for (int i = 0; i < 8; ++i) c = (c & 1) ? (c >> 1) ^ kPoly : c >> 1;
    t[0][b] = c;
  }
  for (size_t k = 1; k < 8; ++k) {
    for (uint32_t b = 0; b < 256; ++b) t[k][b] = (t[k - 1][b] >> 8) ^ t[0][t[k - 1][b] & 0xFF];
  }
  return t;
}
constexpr Tables kTables = make_tables();

}  // namespace

uint32_t crc32c_table(const void* data, size_t len, uint32_t crc) {
  const auto* p = static_cast<const uint8_t*>(data);
  uint32_t c = ~crc;
  const Tables& t = kTables;
  for (; len >= 8; p += 8, len -= 8) {
    uint32_t lo, hi;
    std::memcpy(&lo, p, 4);
    std::memcpy(&hi, p + 4, 4);
    lo ^= c;
    c = t[7][lo & 0xFF] ^ t[6][(lo >> 8) & 0xFF] ^ t[5][(lo >> 16) & 0xFF] ^ t[4][lo >> 24] ^
        t[3][hi & 0xFF] ^ t[2][(hi >> 8) & 0xFF] ^ t[1][(hi >> 16) & 0xFF] ^ t[0][hi >> 24];
  }
  for (; len > 0; ++p, --len) c = (c >> 8) ^ t[0][(c ^ *p) & 0xFF];
  return ~c;
}

uint32_t crc32c(const void* data, size_t len, uint32_t crc) {
#if defined(__ARM_FEATURE_CRC32)
  const auto* p = static_cast<const uint8_t*>(data);
  uint32_t c = ~crc;
  for (; len >= 8; p += 8, len -= 8) {
    uint64_t w;
    std::memcpy(&w, p, 8);  // little-endian load (format.h asserts the host is)
    c = __crc32cd(c, w);
  }
  for (; len > 0; ++p, --len) c = __crc32cb(c, *p);
  return ~c;
#elif defined(__SSE4_2__)
  const auto* p = static_cast<const uint8_t*>(data);
  uint64_t c = ~crc;
  for (; len >= 8; p += 8, len -= 8) {
    uint64_t w;
    std::memcpy(&w, p, 8);
    c = _mm_crc32_u64(c, w);
  }
  uint32_t c32 = static_cast<uint32_t>(c);
  for (; len > 0; ++p, --len) c32 = _mm_crc32_u8(c32, *p);
  return ~c32;
#else
  return crc32c_table(data, len, crc);
#endif
}

const char* crc32c_impl() {
#if defined(__ARM_FEATURE_CRC32)
  return "arm64-crc32c";
#elif defined(__SSE4_2__)
  return "x86-sse4.2";
#else
  return "table-slicing-by-8";
#endif
}

}  // namespace colstore
