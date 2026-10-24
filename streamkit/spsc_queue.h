#pragma once
// Bounded single-producer single-consumer ring queue (streamkit milestone 3).
//
// WHY THIS EXISTS. The reader thread hands batches of events to P worker
// threads. Each worker has exactly one producer (the reader) and exactly one
// consumer (itself), so a queue with one writer and one reader per end is
// enough, and that restriction is what makes a lock unnecessary:
//
//   - only the producer ever writes tail_, only the consumer ever writes head_;
//   - a slot is written by the producer strictly before it publishes the new
//     tail_, and read by the consumer strictly before it publishes the new
//     head_;
//   - so the two threads never write the same word, and the only thing to get
//     right is that the slot contents become visible together with the index
//     that announces them. That is exactly what release/acquire gives us.
//
// MEMORY ORDERING, spelled out.
//   producer: write slot[tail]; tail_.store(tail + 1, release)
//   consumer: t = tail_.load(acquire); if (head < t) read slot[head]
// The release store "publishes" every write the producer made before it, and
// the acquire load that observes it "receives" them; the consumer therefore
// sees a fully written slot, never a half-written one. The same pair in the
// other direction (consumer releases head_, producer acquires it) guarantees
// the producer never overwrites a slot the consumer is still reading. Nothing
// here needs sequential consistency, and relaxed would be wrong: it orders
// the index but not the slot bytes.
//
// CACHE BEHAVIOUR. head_ and tail_ live on their own cache lines so the
// producer's writes to tail_ do not invalidate the consumer's head_ line
// (false sharing). Each side also keeps a private cached copy of the other
// side's index and only reloads it when the queue looks full (producer) or
// empty (consumer). While the producer is ahead (the queue holds several
// slots) that turns one cross-core cache-line transfer per operation into
// one per wrap, which is the difference between the queue being a
// bottleneck and being invisible. A consumer that has caught up (empty
// queue, e.g. on a paced live stream) reloads tail_ on every try_front, so
// there the saving is on the producer side only.
//
// IN-PLACE API. Besides try_push/try_pop by value, the queue exposes the slot
// itself: the producer claims a slot, fills it and publishes; the consumer
// looks at the front slot, consumes it in place and pops. streamkit keeps
// its event batches *inside* the slots this way, so the batch buffers are
// allocated once per slot and reused forever instead of being allocated by
// the reader and freed by a worker for every batch.
//
// Capacity is rounded up to a power of two so the slot index is idx & mask
// instead of a division. Indices are monotonically increasing size_t values;
// unsigned wraparound after 2^64 operations keeps tail - head correct.

#include <atomic>
#include <cstddef>
#include <utility>
#include <vector>

namespace adstack {

template <typename T>
class SpscQueue {
 public:
  // capacity: minimum number of slots (rounded up to a power of two, >= 2).
  explicit SpscQueue(size_t capacity) {
    size_t n = 2;
    while (n < capacity) n <<= 1;
    slots_.resize(n);
    mask_ = n - 1;
  }

  SpscQueue(const SpscQueue&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;

  size_t capacity() const { return slots_.size(); }

  // ---- producer side (exactly one thread may call these) ------------------

  // Slot the producer may fill next, or nullptr when the queue is full.
  // Calling it twice without publish() returns the same slot.
  T* try_claim() {
    const size_t tail = tail_.load(std::memory_order_relaxed);  // our own write, no sync needed
    if (tail - cached_head_ == slots_.size()) {
      // Looks full. Re-read the consumer's real position (acquire: pairs with
      // the consumer's release in pop(), so the slot it freed is really free).
      cached_head_ = head_.load(std::memory_order_acquire);
      if (tail - cached_head_ == slots_.size()) return nullptr;
    }
    return &slots_[tail & mask_];
  }

  // Make the slot returned by try_claim() visible to the consumer.
  void publish() {
    const size_t tail = tail_.load(std::memory_order_relaxed);
    tail_.store(tail + 1, std::memory_order_release);  // slot writes happen-before this
  }

  bool try_push(T v) {
    T* slot = try_claim();
    if (!slot) return false;
    *slot = std::move(v);
    publish();
    return true;
  }

  // ---- consumer side (exactly one thread may call these) ------------------

  // Oldest unconsumed slot, or nullptr when the queue is empty. The pointer
  // stays valid until pop().
  T* try_front() {
    const size_t head = head_.load(std::memory_order_relaxed);  // our own write
    if (head == cached_tail_) {
      // Looks empty. Re-read the producer's real position (acquire: pairs with
      // publish()'s release, so the slot contents are visible).
      cached_tail_ = tail_.load(std::memory_order_acquire);
      if (head == cached_tail_) return nullptr;
    }
    return &slots_[head & mask_];
  }

  // Release the slot returned by try_front() back to the producer.
  void pop() {
    const size_t head = head_.load(std::memory_order_relaxed);
    head_.store(head + 1, std::memory_order_release);  // our reads of the slot happen-before this
  }

  bool try_pop(T& out) {
    T* slot = try_front();
    if (!slot) return false;
    out = std::move(*slot);
    pop();
    return true;
  }

  // Approximate (racy by design; for stats, never for control flow).
  size_t size_approx() const {
    return tail_.load(std::memory_order_relaxed) - head_.load(std::memory_order_relaxed);
  }

 private:
  // Cache-line size used for padding. std::hardware_destructive_interference_size
  // is the standard name for this but GCC warns on every use of it (its value
  // is an ABI hazard), so the number is written down instead: 128 bytes is the
  // line size on Apple silicon and a safe over-estimate on x86-64 (64-byte
  // lines, fetched in adjacent pairs by the prefetcher).
  static constexpr size_t kLine = 128;

  std::vector<T> slots_;
  size_t mask_ = 0;

  // Producer-owned line: its index plus its private snapshot of head_.
  alignas(kLine) std::atomic<size_t> tail_{0};
  size_t cached_head_ = 0;

  // Consumer-owned line: its index plus its private snapshot of tail_.
  alignas(kLine) std::atomic<size_t> head_{0};
  size_t cached_tail_ = 0;

  // Trailing pad so whatever is allocated after this object does not share
  // the consumer's line.
  alignas(kLine) char pad_end_[kLine] = {};
};

}  // namespace adstack
