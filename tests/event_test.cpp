#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "common/event.h"

using namespace adstack;

namespace {

Event sample(uint64_t id) {
  Event e;
  e.event_id = id;
  e.event_time_us = 1'767'225'600'000'000LL + static_cast<int64_t>(id) * 7;
  e.ingest_time_us = e.event_time_us + 150'000;
  e.event_type = static_cast<uint8_t>(id % 3);
  e.advertiser_id = static_cast<uint32_t>(id % 10'000);
  e.campaign_id = static_cast<uint32_t>(id % 100'000);
  e.video_id = id * 2654435761ULL;
  e.country = 840;
  e.device = 1;
  e.cost_micros = static_cast<int64_t>(id) * -3;  // negative on purpose
  return e;
}

std::string temp_path(const char* name) {
  return (std::filesystem::temp_directory_path() / name).string();
}

}  // namespace

TEST(Event, EncodeDecodeRoundTrip) {
  for (uint64_t id : {0ULL, 1ULL, 12345ULL, ~0ULL}) {
    uint8_t buf[kEventWireSize];
    Event e = sample(id);
    encode(e, buf);
    EXPECT_EQ(decode(buf), e);
  }
}

TEST(Event, WireSizeIsFixed) {
  EXPECT_EQ(kEventWireSize, 52u);
}

TEST(Event, WriterReaderRoundTrip) {
  const std::string path = temp_path("adstack_event_test.ads");
  constexpr uint64_t kN = 100'000;  // crosses the 1 MiB write buffer
  {
    EventWriter w(path);
    for (uint64_t i = 0; i < kN; ++i) w.write(sample(i));
    EXPECT_EQ(w.count(), kN);
  }
  EXPECT_EQ(std::filesystem::file_size(path), kFileHeaderSize + kN * kEventWireSize);
  {
    EventReader r(path);
    Event e;
    uint64_t i = 0;
    while (r.next(e)) {
      ASSERT_EQ(e, sample(i)) << "at index " << i;
      ++i;
    }
    EXPECT_EQ(i, kN);
  }
  std::filesystem::remove(path);
}

TEST(Event, ReaderRejectsBadMagic) {
  const std::string path = temp_path("adstack_bad_magic.ads");
  FILE* f = std::fopen(path.c_str(), "wb");
  ASSERT_NE(f, nullptr);
  const char junk[8] = {'n', 'o', 'p', 'e', 0, 0, 0, 0};
  std::fwrite(junk, 1, 8, f);
  std::fclose(f);
  EXPECT_THROW(EventReader r(path), std::runtime_error);
  std::filesystem::remove(path);
}

TEST(Event, ReaderDetectsTornRecord) {
  const std::string path = temp_path("adstack_torn.ads");
  {
    EventWriter w(path);
    w.write(sample(1));
  }
  std::filesystem::resize_file(path, kFileHeaderSize + kEventWireSize - 1);
  EventReader r(path);
  Event e;
  EXPECT_THROW(r.next(e), std::runtime_error);
  std::filesystem::remove(path);
}

TEST(Wire, CampaignIdOffsetMatchesDecode) {
  // streamkit's reader peeks campaign_id at a fixed offset without decoding;
  // the offset must agree with the real layout for every value.
  for (uint32_t c : {0u, 1u, 12345u, 0xDEADBEEFu, 0xFFFFFFFFu}) {
    Event e;
    e.event_id = 7;
    e.campaign_id = c;
    uint8_t buf[kEventWireSize];
    encode(e, buf);
    uint32_t peeked = 0;
    std::memcpy(&peeked, buf + kWireOffsetCampaignId, sizeof(peeked));
    EXPECT_EQ(peeked, c);
    EXPECT_EQ(decode(buf).campaign_id, c);
  }
}
