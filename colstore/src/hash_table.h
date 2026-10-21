#pragma once
// GroupTable: the open-addressing hash table behind GROUP BY (m4).
//
// It maps a packed group key (GroupKey, 128 bits) to a dense GROUP INDEX:
// 0 for the first key ever seen, 1 for the second, and so on. The aggregate
// state (counts, sums, mins, maxes) is NOT stored in the table; it lives in
// flat arrays owned by HashAggregate and indexed by that group index. That
// split keeps the table generic (it knows nothing about aggregates), keeps
// each aggregate's update loop a plain array walk, and makes merging two
// tables (one per thread) a loop over one table's keys.
//
// Why a hand-written table instead of std::unordered_map:
//   - unordered_map allocates one node per key and chains them, so every
//     probe is at least two dependent cache misses (bucket array, then the
//     node). Here one slot holds the key and the index, so a hit is one
//     memory access.
//   - We control growth and layout, which is what makes the probe cost
//     explainable and measurable (probes() counts every slot visited).
//   - A student can read the whole thing in five minutes.
//
// Probing: LINEAR. On a collision the next slot tried is the adjacent one
// (index + 1, wrapping). Adjacent slots share a cache line, so a short probe
// sequence costs no extra memory traffic. The known weakness of linear
// probing is clustering (runs of occupied slots grow and merge), which is
// kept in check by two things: a load factor capped at 1/2, and a hash
// mixer (below) so that nearby keys land far apart.
//
// Growth: the slot array has a power-of-two size, so "hash mod capacity" is
// a mask. When the number of groups reaches half the slots the array
// doubles and every key is reinserted. At load <= 1/2 the expected probe
// length with linear probing is about 1.5 for a hit and 2.5 for a miss
// (Knuth's (1 + 1/(1-a))/2 and (1 + 1/(1-a)^2)/2 with a = 0.5); at 0.9 the
// miss cost would be 50. Doubling means the total reinsertion work over the
// table's life is bounded by 2x the final size, i.e. amortised O(1) per
// insert.

#include <cstddef>
#include <cstdint>
#include <vector>

namespace colstore {

// A group key: the group-by column values packed into two 64-bit words by
// KeyPacker (aggregate.h). Two words so that any pair of columns fits
// (two i64 columns are 128 bits); comparing two words instead of one is
// noise next to the cache miss of the probe.
struct GroupKey {
  uint64_t lo = 0;
  uint64_t hi = 0;
  bool operator==(const GroupKey&) const = default;
};

// splitmix64's finalizer: a bijection on 64-bit values that spreads every
// input bit over every output bit. Needed because the table uses the LOW
// bits of the hash as the slot index: a packed key with campaign_id in
// bits 0..31 and country in bits 32..47 would, with an identity hash, put
// every (campaign, country) pair with the same campaign into the same
// slot. The mixer costs three multiplies/shifts, about a nanosecond.
inline uint64_t mix64(uint64_t z) {
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
  z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
  return z ^ (z >> 31);
}

inline uint64_t hash_key(const GroupKey& k) {
  // The golden-ratio multiply folds the high word in before mixing so
  // keys that differ only in `hi` do not collide.
  return mix64(k.lo ^ (k.hi * 0x9e3779b97f4a7c15ULL));
}

class GroupTable {
 public:
  static constexpr uint32_t kNoGroup = UINT32_MAX;

  // initial_slots is rounded up to a power of two (minimum 2).
  explicit GroupTable(size_t initial_slots = 1024);

  // Index of `key`'s group, inserting it as a new group if absent. The
  // new group's index is always size() - 1 before the call. `inserted`,
  // when given, says whether a new group was created.
  uint32_t find_or_insert(const GroupKey& key, bool* inserted = nullptr);

  // Index of `key`'s group, or kNoGroup. Never inserts.
  uint32_t find(const GroupKey& key) const;

  uint32_t size() const { return static_cast<uint32_t>(keys_.size()); }
  const GroupKey& key(uint32_t group) const { return keys_[group]; }

  // Introspection for --stats and the tests.
  size_t slots() const { return slots_.size(); }
  size_t grows() const { return grows_; }
  uint64_t probes() const { return probes_; }  // slots visited over the table's life

 private:
  struct Slot {
    GroupKey key;
    uint32_t group = kNoGroup;  // kNoGroup marks an empty slot
  };

  // The slot that holds `key`, or the empty slot where it would be
  // inserted. Requires at least one empty slot, which the load cap
  // guarantees.
  size_t find_slot(const GroupKey& key) const;
  void grow();

  std::vector<Slot> slots_;      // power-of-two length
  std::vector<GroupKey> keys_;   // keys_[g] is group g's key; insertion order
  size_t grows_ = 0;
  mutable uint64_t probes_ = 0;  // mutable: find() is logically const
};

}  // namespace colstore
