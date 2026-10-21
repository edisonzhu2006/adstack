// Unit tests for the SPSC ring (streamkit milestone 3). Single-threaded tests
// pin FIFO order, the full/empty boundaries and wraparound; the two-thread
// tests are what TSan checks for data races, so they move real data through
// the queue rather than counters.

#include <gtest/gtest.h>

#include <cstdint>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "spsc_queue.h"

using namespace adstack;

TEST(SpscQueue, CapacityRoundsUpToPowerOfTwo) {
  EXPECT_EQ(SpscQueue<int>(1).capacity(), 2u);
  EXPECT_EQ(SpscQueue<int>(2).capacity(), 2u);
  EXPECT_EQ(SpscQueue<int>(3).capacity(), 4u);
  EXPECT_EQ(SpscQueue<int>(4096).capacity(), 4096u);
  EXPECT_EQ(SpscQueue<int>(4097).capacity(), 8192u);
}

TEST(SpscQueue, FifoAndFullEmpty) {
  SpscQueue<int> q(4);
  int v = 0;
  EXPECT_FALSE(q.try_pop(v));  // empty
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(q.try_push(i));
  EXPECT_FALSE(q.try_push(99));  // full: exactly capacity items fit
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(q.try_pop(v));
    EXPECT_EQ(v, i);
  }
  EXPECT_FALSE(q.try_pop(v));
}

TEST(SpscQueue, WrapsAroundManyTimes) {
  SpscQueue<uint64_t> q(8);
  uint64_t next_push = 0, next_pop = 0, v = 0;
  for (int round = 0; round < 1000; ++round) {
    // Push a varying amount (never beyond capacity), pop a varying amount,
    // so head/tail cross the ring boundary at every phase.
    const int n = 1 + round % 7;
    for (int i = 0; i < n && next_push - next_pop < q.capacity(); ++i) {
      ASSERT_TRUE(q.try_push(next_push++));
    }
    EXPECT_FALSE(next_push - next_pop == q.capacity() && q.try_push(0));  // full stays full
    const int m = 1 + (round * 3) % 7;
    for (int i = 0; i < m && next_pop < next_push; ++i) {
      ASSERT_TRUE(q.try_pop(v));
      EXPECT_EQ(v, next_pop++);
    }
  }
  while (q.try_pop(v)) EXPECT_EQ(v, next_pop++);
  EXPECT_EQ(next_pop, next_push);
}

TEST(SpscQueue, ClaimIsIdempotentUntilPublish) {
  SpscQueue<std::string> q(2);
  std::string* a = q.try_claim();
  ASSERT_NE(a, nullptr);
  EXPECT_EQ(q.try_claim(), a);  // same slot until published
  *a = "first";
  q.publish();
  std::string* b = q.try_claim();
  ASSERT_NE(b, nullptr);
  EXPECT_NE(b, a);
  *b = "second";
  q.publish();
  EXPECT_EQ(q.try_claim(), nullptr);  // full

  std::string* f = q.try_front();
  ASSERT_NE(f, nullptr);
  EXPECT_EQ(*f, "first");
  EXPECT_EQ(q.try_front(), f);  // same slot until popped
  q.pop();
  ASSERT_NE(q.try_front(), nullptr);
  EXPECT_EQ(*q.try_front(), "second");
  q.pop();
  EXPECT_EQ(q.try_front(), nullptr);
}

TEST(SpscQueue, SlotsAreReusedInPlace) {
  // The in-place API exists so a slot's buffer survives across uses. After a
  // slot is popped and claimed again, it is the same object with its
  // capacity intact.
  SpscQueue<std::vector<int>> q(2);
  std::vector<int>* s = q.try_claim();
  s->reserve(1024);
  const int* data_before = s->data();
  q.publish();
  q.try_front()->clear();
  q.pop();
  q.try_claim();
  q.publish();  // slot 1
  q.try_front();
  q.pop();
  std::vector<int>* again = q.try_claim();  // wraps to slot 0
  EXPECT_EQ(again, s);
  EXPECT_EQ(again->data(), data_before);
  EXPECT_GE(again->capacity(), 1024u);
}

TEST(SpscQueue, TwoThreadsPreserveOrderAndCount) {
  // Producer pushes 0..N-1, consumer must see exactly that sequence. A
  // missed release/acquire would show up as a reordered or stale value (and
  // as a TSan report in the thread-sanitizer build).
  constexpr uint64_t kN = 2'000'000;
  SpscQueue<uint64_t> q(1024);
  std::thread producer([&] {
    for (uint64_t i = 0; i < kN; ++i) {
      while (!q.try_push(i)) std::this_thread::yield();
    }
  });
  uint64_t expected = 0, sum = 0, v = 0;
  bool ordered = true;
  while (expected < kN) {
    if (!q.try_pop(v)) { std::this_thread::yield(); continue; }
    if (v != expected) ordered = false;
    sum += v;
    ++expected;
  }
  producer.join();
  EXPECT_TRUE(ordered);
  EXPECT_EQ(sum, kN * (kN - 1) / 2);
  EXPECT_FALSE(q.try_pop(v));
}

TEST(SpscQueue, TwoThreadsInPlaceBatches) {
  // The way streamkit uses it: producer fills a vector inside the slot,
  // consumer reads it in place. Every byte written must be visible to the
  // consumer, and the slot must not be overwritten while it is being read.
  constexpr int kBatches = 20'000;
  constexpr size_t kBatchLen = 64;
  SpscQueue<std::vector<uint32_t>> q(16);
  std::thread producer([&] {
    for (int b = 0; b < kBatches; ++b) {
      std::vector<uint32_t>* slot;
      while (!(slot = q.try_claim())) std::this_thread::yield();
      slot->clear();
      for (size_t i = 0; i < kBatchLen; ++i) slot->push_back(static_cast<uint32_t>(b));
      q.publish();
    }
  });
  int bad = 0;
  for (int b = 0; b < kBatches; ++b) {
    std::vector<uint32_t>* slot;
    while (!(slot = q.try_front())) std::this_thread::yield();
    if (slot->size() != kBatchLen) ++bad;
    for (uint32_t x : *slot) {
      if (x != static_cast<uint32_t>(b)) ++bad;
    }
    q.pop();
  }
  producer.join();
  EXPECT_EQ(bad, 0);
}
