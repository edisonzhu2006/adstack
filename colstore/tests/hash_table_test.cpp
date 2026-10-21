// GroupTable: growth policy, collisions and probing, agreement with
// std::unordered_map on random keys with duplicates.

#include <gtest/gtest.h>

#include <bit>
#include <random>
#include <unordered_map>

#include "src/hash_table.h"

using namespace colstore;

namespace {

GroupKey key(uint64_t lo, uint64_t hi = 0) { return GroupKey{lo, hi}; }

TEST(GroupTable, EmptyTableFindsNothing) {
  GroupTable t;
  EXPECT_EQ(t.size(), 0u);
  EXPECT_EQ(t.find(key(0)), GroupTable::kNoGroup);
  EXPECT_EQ(t.find(key(123, 456)), GroupTable::kNoGroup);
}

TEST(GroupTable, GroupIndexIsInsertionOrder) {
  GroupTable t;
  for (uint64_t i = 0; i < 100; ++i) {
    bool inserted = false;
    EXPECT_EQ(t.find_or_insert(key(i * 7919), &inserted), i);
    EXPECT_TRUE(inserted);
  }
  EXPECT_EQ(t.size(), 100u);
  for (uint64_t i = 0; i < 100; ++i) {
    bool inserted = true;
    EXPECT_EQ(t.find_or_insert(key(i * 7919), &inserted), i) << "second insert must find the same group";
    EXPECT_FALSE(inserted);
    EXPECT_EQ(t.find(key(i * 7919)), i);
    EXPECT_EQ(t.key(static_cast<uint32_t>(i)), key(i * 7919));
  }
  EXPECT_EQ(t.size(), 100u);
}

TEST(GroupTable, HighWordDistinguishesKeys) {
  GroupTable t;
  EXPECT_EQ(t.find_or_insert(key(1, 0)), 0u);
  EXPECT_EQ(t.find_or_insert(key(1, 1)), 1u);
  EXPECT_EQ(t.find_or_insert(key(0, 1)), 2u);
  EXPECT_EQ(t.find_or_insert(key(1, 0)), 0u);
  EXPECT_EQ(t.find_or_insert(key(1, 1)), 1u);
  EXPECT_EQ(t.size(), 3u);
  EXPECT_NE(hash_key(key(1, 0)), hash_key(key(1, 1)));
}

TEST(GroupTable, GrowsByDoublingAndStaysAtMostHalfFull) {
  GroupTable t(2);  // smallest table: forces a doubling on the first insert
  EXPECT_EQ(t.slots(), 2u);
  for (uint64_t i = 0; i < 10'000; ++i) {
    t.find_or_insert(key(i));
    ASSERT_LE(t.size() * 2, t.slots()) << "load factor above 1/2 after " << i + 1 << " inserts";
    ASSERT_TRUE(std::has_single_bit(t.slots())) << "capacity must stay a power of two";
  }
  // 10,000 groups need > 20,000 slots, so 32,768; never more than 4x.
  EXPECT_EQ(t.slots(), 32'768u);
  EXPECT_EQ(t.grows(), 14u);  // 2 -> 32768 is 14 doublings
  for (uint64_t i = 0; i < 10'000; ++i) ASSERT_EQ(t.find(key(i)), i) << "key lost during growth";
  EXPECT_EQ(t.find(key(10'000)), GroupTable::kNoGroup);
}

TEST(GroupTable, CollisionsAreResolvedByProbingAndAbsentKeysTerminate) {
  // 1,000 keys in a 2,048-slot table (no growth): with ~1000 keys hashed
  // into 2048 slots there are hundreds of collisions, and every key must
  // still be found. The probe count exceeding the key count proves that
  // probing happened; an absent key's probe must stop at an empty slot.
  GroupTable t(2'048);
  for (uint64_t i = 0; i < 1'000; ++i) t.find_or_insert(key(i, i * 31));
  EXPECT_EQ(t.slots(), 2'048u);
  EXPECT_EQ(t.grows(), 0u);
  EXPECT_GT(t.probes(), 1'000u);
  for (uint64_t i = 0; i < 1'000; ++i) ASSERT_EQ(t.find(key(i, i * 31)), i);
  for (uint64_t i = 1'000; i < 2'000; ++i) ASSERT_EQ(t.find(key(i, i * 31)), GroupTable::kNoGroup);
}

TEST(GroupTable, MeanProbeLengthIsSmallForSequentialAndRandomKeys) {
  // Knuth: linear probing at load 1/2 costs ~1.5 probes per hit and ~2.5
  // per miss; the table runs between 1/4 and 1/2 full, so an average well
  // under 2.5 is the bound a correct mixer must meet, for dense sequential
  // keys (campaign ids) as well as random ones.
  {
    GroupTable t;
    for (uint64_t i = 0; i < 100'000; ++i) t.find_or_insert(key(i));
    for (uint64_t i = 0; i < 100'000; ++i) t.find(key(i));
    EXPECT_LT(static_cast<double>(t.probes()) / 200'000.0, 2.5);
  }
  {
    GroupTable t;
    std::mt19937_64 rng(7);
    std::vector<uint64_t> keys(100'000);
    for (auto& k : keys) k = rng();
    for (uint64_t k : keys) t.find_or_insert(key(k));
    for (uint64_t k : keys) t.find(key(k));
    EXPECT_LT(static_cast<double>(t.probes()) / 200'000.0, 2.5);
  }
}

TEST(GroupTable, MatchesUnorderedMapOnRandomKeysWithDuplicates) {
  std::mt19937_64 rng(42);
  std::uniform_int_distribution<uint64_t> pick(0, 49'999);
  GroupTable t;
  std::unordered_map<uint64_t, uint32_t> ref;
  for (int i = 0; i < 300'000; ++i) {
    const uint64_t k = pick(rng) * 0x9E3779B97F4A7C15ULL;  // spread, with duplicates
    const auto [it, fresh] = ref.try_emplace(k, static_cast<uint32_t>(ref.size()));
    bool inserted = false;
    const uint32_t g = t.find_or_insert(key(k, k >> 7), &inserted);
    ASSERT_EQ(g, it->second);
    ASSERT_EQ(inserted, fresh);
  }
  EXPECT_EQ(t.size(), ref.size());
  for (const auto& [k, g] : ref) ASSERT_EQ(t.find(key(k, k >> 7)), g);
}

}  // namespace
