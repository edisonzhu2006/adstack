// GroupTable implementation. See hash_table.h for the probing and growth
// policy and the reasons behind them.

#include "src/hash_table.h"

#include <bit>
#include <stdexcept>

namespace colstore {

GroupTable::GroupTable(size_t initial_slots) {
  if (initial_slots < 2) initial_slots = 2;
  slots_.resize(std::bit_ceil(initial_slots));
}

size_t GroupTable::find_slot(const GroupKey& key) const {
  const size_t mask = slots_.size() - 1;
  size_t i = hash_key(key) & mask;
  for (;;) {
    ++probes_;
    const Slot& s = slots_[i];
    if (s.group == kNoGroup || s.key == key) return i;
    i = (i + 1) & mask;  // linear probing: the adjacent slot
  }
}

uint32_t GroupTable::find(const GroupKey& key) const {
  return slots_[find_slot(key)].group;
}

uint32_t GroupTable::find_or_insert(const GroupKey& key, bool* inserted) {
  // Grow BEFORE probing so the slot found below is still valid after. The
  // cap is one half: with `size * 2 >= slots` the table is half full.
  if ((static_cast<size_t>(keys_.size()) + 1) * 2 > slots_.size()) grow();
  Slot& s = slots_[find_slot(key)];
  if (s.group != kNoGroup) {
    if (inserted) *inserted = false;
    return s.group;
  }
  if (keys_.size() >= kNoGroup) throw std::length_error("GroupTable: too many groups");
  s.key = key;
  s.group = static_cast<uint32_t>(keys_.size());
  keys_.push_back(key);
  if (inserted) *inserted = true;
  return s.group;
}

void GroupTable::grow() {
  // Double, then reinsert every occupied slot. The hash is recomputed from
  // the key (a mixer is cheaper than 8 more bytes per slot to cache it).
  std::vector<Slot> old;
  old.swap(slots_);
  slots_.resize(old.size() * 2);
  const size_t mask = slots_.size() - 1;
  for (const Slot& s : old) {
    if (s.group == kNoGroup) continue;
    size_t i = hash_key(s.key) & mask;
    while (slots_[i].group != kNoGroup) i = (i + 1) & mask;
    slots_[i] = s;
  }
  ++grows_;
}

}  // namespace colstore
