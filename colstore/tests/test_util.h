#pragma once
// Shared helpers for colstore tests: a deterministic sample event and a
// temp file that deletes itself.

#include <cstdint>
#include <filesystem>
#include <string>

#include "common/event.h"

namespace colstore::test {

// Deterministic event for row i. Exercises every field, including a
// negative cost so signed min/max handling is covered.
inline adstack::Event sample_event(uint64_t i) {
  adstack::Event e;
  e.event_id = i + 1;
  e.event_time_us = 1'767'225'600'000'000LL + static_cast<int64_t>(i) * 101;
  e.ingest_time_us = e.event_time_us + 150'000;
  e.event_type = static_cast<uint8_t>(i % 3);
  e.advertiser_id = static_cast<uint32_t>(i % 10'000);
  e.campaign_id = static_cast<uint32_t>((i * 7) % 100'000);
  e.video_id = i * 2654435761ULL;
  e.country = static_cast<uint16_t>(i % 2 ? 840 : 356);
  e.device = static_cast<uint8_t>(i % 2);
  e.cost_micros = static_cast<int64_t>(i) * 3 - 1000;  // crosses zero
  return e;
}

class TempFile {
 public:
  explicit TempFile(const std::string& name)
      : path_((std::filesystem::temp_directory_path() / ("colstore_test_" + name)).string()) {
    std::filesystem::remove(path_);
  }
  ~TempFile() { std::filesystem::remove(path_); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

}  // namespace colstore::test
