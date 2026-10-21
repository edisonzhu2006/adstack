# colstore design notes

Running log of decisions and dead ends for the columnar store. Same rule as
the root NOTES.md: every entry answers a "why X instead of Y" question.

## 2026-10-19: m1, file format and ingest

**Footer at the end of the file instead of a header up front.**
The writer learns a chunk's offset, length, min and max only after writing
it. With metadata up front the writer would have to either buffer the whole
file in memory, or write a placeholder header and seek back to patch it.
Patching is the dangerous option: a writer killed between writing the data
and patching the header leaves a file whose header claims it is valid. With
the footer last, "no trailer with the right magic" means "incomplete file",
and the reader rejects it. Cost: a reader needs two seeks before the first
chunk (end of file for the trailer, then the footer). That is 20 bytes plus
74 KB for a 520 MB file, paid once per open. Parquet and ORC make the same
call for the same reason.

**64K rows per row group.** A row group is the unit of skipping (m3 uses the
per-chunk min/max to skip whole groups) and of I/O (a chunk is one fread).
Smaller groups skip at finer granularity but cost more footer metadata and
more, smaller reads; larger groups make each read efficient but force the
scanner to read 64K values to use even one of them. 64K is the point where a
wide column's chunk (64K * 8 B = 512 KiB) is already large enough that
per-read overhead does not matter, and the footer stays tiny (153 groups,
74 KB, 0.014% of the file at 10M rows). It is also what Parquet-style
systems default to, so the comparison in m5 is fair. To tune it I would
measure, for the three m4 benchmark queries, rows actually decoded versus
rows that satisfied the filter, at 8K / 16K / 64K / 256K rows per group,
plus footer bytes and open time. The knob is `ingest --row-group N`, so the
sweep is a shell loop.

**fseek + fread instead of mmap.** Chunks are large and contiguous, so one
read syscall per chunk costs nothing measurable next to decoding: verify
reads all 1,530 chunks of the 10M-row file in 0.34 s, including rebuilding
every Event. mmap would remove the copy into a `std::vector`, but it turns
a truncated or corrupted file into a SIGBUS instead of an exception, makes
memory use invisible to the process (page cache instead of RSS, which
confuses the memory numbers in m5), and still has to copy to decode any
non-PLAIN encoding in m2. Revisit only if m3 shows random point reads, which
the current query set does not have.

**PLAIN is a memcpy, and the format is little-endian by fiat.** The host
layout of `std::vector<int64_t>` is already the wire layout on every
machine this runs on (arm64, x86-64). A `static_assert` on
`std::endian::native` makes the assumption explicit; the alternative, a
byte-by-byte encoder like `common/event.cpp`, is portable but 8x more loop
iterations for no benefit here. Decoding a chunk is one `memcpy`.

**Encoding id per chunk, not per column or per file.** m2 will pick an
encoding by looking at the data (dictionary for `country` and `device`,
delta for `event_time_us`, plain for `video_id`), and the best choice can
differ between row groups of the same column (a quiet minute versus a
spike). Putting the id in `ChunkMeta` costs 4 bytes per chunk and lets the
reader dispatch per chunk with no format change.

**min/max stored as raw 64-bit patterns plus the column's declared type,
instead of two fields (int64 and uint64) or a type tag per chunk.** All ten
columns are integers of 8 bytes or less, so 8 bytes of raw bits plus the
static column type is enough to recover the value exactly. Two fields would
waste 8 bytes per chunk; a tag per chunk duplicates information already in
the column table. The one subtlety is sign extension for `i64`, covered by a
test with a negative `cost_micros`.

**`null_count` reserved but written as 0.** Event has no nullable field, so
there is nothing to count, but every real columnar format has nulls and
adding the field later would be a version bump. 4 bytes per chunk buys
forward compatibility.

**Ten typed vectors in the writer instead of ten byte buffers.** A student
can read `batch_.campaign_id.push_back(e.campaign_id)` and know what it
does. Byte buffers with a per-column width table are shorter but hide the
types, and the typed vectors are what the min/max and encoders operate on
anyway.

**Static library + CLI instead of one executable from `src/*.cpp`.** The
original CMake globbed all sources into the binary, which left the tests
unable to link the writer and reader. `colstore_core` holds everything
except `main.cpp`; the CLI and the tests both link it, so the tests exercise
the exact code that ships.

**Verification.** `colstore verify` reads both files and compares every
field of every row with `Event::operator==`; it exits 1 on the first
mismatch and prints which field differs. On the 10M-event file from
`gen --n 10000000 --seed 1`: PASS, 10,000,000 rows, 153 row groups. Unit
tests cover the footer round trip (in memory and through a file), a file
without a trailer being rejected, 0 / 65,536 / 65,537 rows producing
0 / 1 / 2 row groups, chunks tiling the data region with no gaps, a raw
fseek/fread at the footer's offset yielding the same bytes as the reader,
and signed min/max. All 15 pass in Release and under ASan+UBSan; the ASan
build's ingest, verify, info and dump are clean on the 10M file and its
output is byte-identical to the Release build's. Numbers from
`bench/colstore_bench.sh build-cs 10000000` on an Apple M5 Pro: ingest
best of 3 = 0.433 s (23.1 M rows/s), file 520,074,096 bytes for a
520,000,008-byte input (ratio 1.0001, as expected for PLAIN: the 74 KB
footer is the only overhead). Compression is m2's job.

## Open questions from m1 (status)

- m2, dictionary for `campaign_id` (100K distinct): answered above, it loses
  to DELTA_BP by 2x per chunk and a per-file dictionary is not worth the
  format change.
- m2, delta vs. frame-of-reference for `event_time_us`: answered above,
  delta, with the measured 5% cost on random columns.
- m3: is 64K rows the right group size for the time-range query? See the
  measurement plan above.
- m5: SQLite stores the same rows in ~600 MB with an index on
  `(campaign_id, event_time_us)`; is the fair comparison with or without
  that index?

## 2026-10-19: m2, per-chunk encodings

Numbers in this entry: 10M events (`gen --seed 1`), Apple M5 Pro, Release,
`bench/colstore_bench.sh build-cs 10000000`; per-chunk numbers are row
group 0 of the 1M file (65,536 rows) measured with the library's own
encoders.

**Four encodings, chosen per chunk by actually encoding and keeping the
smallest, instead of a per-column rule table.** The obvious alternative is
a static mapping (timestamps are delta, enums are dictionary) and it would
be right for this schema today, but it is wrong the moment the data
changes shape: a sorted ingest turns `campaign_id` into long runs, a
backfill makes `event_time_us` random, and streamkit's aggregate CSV (the integration step) has a `window_start_us` column that is one long run per window.
Trial encoding has no model that can drift from the encoder and never picks
something larger than PLAIN. Cost: CPU at ingest, 0.756 s per 10M rows
(76 ns/row) over the 0.427 s PLAIN baseline; ingest is 2.8x slower, the
file is 3.25x smaller. Verify (decode everything) goes from 0.330 s to
0.467 s.

**The expensive part was the dictionary build, and the fix is a lower
bound, not a heuristic.** The first version sorted a copy of every chunk
to find distinct values: 6.4 ms of the 6.8 ms per high-cardinality chunk
(the delta encoder is 0.08 ms), and 1M rows took 0.704 s. Options: (a) a
sampled cardinality estimate, cheap but it can be fooled by sorted input
and then changes the answer; (b) Parquet's approach, start dictionary
coding and fall back when the dictionary grows past a size cap, which is
the same idea with a fixed cap; (c) what I did: try DELTA_BP first (one
cheap pass), then scan values into a hash map and maintain the exact lower
bound of what DICT and RLE_DICT could still cost given the distinct count
and run count so far. Both bounds only grow as the scan continues, so
stopping when both are at or above the best size in hand is safe: the
result is identical to encoding all four, and `Selector.PruningMatchesTryingEveryCodec` checks that on 60 random shapes. After the change the
same chunk costs 0.95 ms and 1M rows take 0.121 s (PLAIN: 0.051 s). For
`campaign_id` (47.6K distinct in 64K rows) the scan gives up after a few
thousand values because the dictionary alone already exceeds the delta
size. Low-cardinality columns scan to the end and then sort only the d
distinct values (3 or 10), not n.

**Why dictionary encoding hurts on high-cardinality columns, with the
numbers.** A dictionary costs d*w bytes before a single code is written.
Measured on one 64K-row chunk: `campaign_id` has d = 47,599 of 65,536
rows, so DICT is 321,477 bytes against 262,144 PLAIN and 155,140 DELTA_BP;
`event_id` (d = 64,848) is 649,865 vs 524,288 PLAIN. Even `advertiser_id`
(d = 9,974, codes fit in 14 bits) comes out at 154,593 vs 130,628 for
delta, and `cost_micros` (d = 10,817) at 201,233 vs 187,892. The break-even
is roughly d*w < n*(w*8 - bits(d))/8, which for 4-byte values and 64K rows
means d below about 15K, and the dictionary has to be rebuilt per chunk
because each chunk must be decodable from its own bytes. The m1 open
question asked about a per-file dictionary for `campaign_id`: it would
remove the 190 KB per chunk but tie every chunk to a shared table (a
reader can no longer decode a row group without it, and a writer cannot
stream without buffering or back-patching), for a gain that DELTA_BP
mostly captures anyway at 2.37 bytes/row. Not worth the format change.

**Why timestamps are delta-coded, not frame-of-reference on the raw
values.** Both exploit locality within a block; FOR subtracts the block
minimum from each value, delta subtracts the previous value. For a clock
that ticks regularly, FOR still has to encode where in the block's span
each value sits (14 bits for a 12.8 ms block of `ingest_time_us` at 1 us
resolution) while the deltas are all 100: measured 123,396 bytes FOR-only
vs 8,708 bytes delta on the same chunk, 14x. For `event_id` (monotone with
1% duplicate jumps) delta is 99,750 vs 109,140. The cost of delta shows on
random columns, where a delta of two uniform values spans twice the range
of the values themselves, one extra bit per value: `campaign_id` 155,140
delta vs 147,972 FOR-only, about 5%. A per-block flag choosing delta or
FOR would recover that; left as a measured, not guessed, future option.
`event_time_us` compresses only 2.39x because 5% of events are up to 30 s
late and a block of 128 contains at least one with probability 99.9%, so
nearly every block needs ~26-bit deltas. Smaller blocks (32) help
`event_id` (68,710 vs 99,750 bytes: a duplicate's backward jump widens
only 32 values) and hurt `ingest_time_us` (34,820 vs 8,708: the 17-byte
block header dominates); 128 is the compromise where no column is far from
its best, and matches what a 1024-value scan vector in m3 will consume as
8 blocks. An adaptive block size per chunk is the next lever if the m5
storage number needs it.

**Wrapping 64-bit arithmetic for deltas instead of a wider type or
zigzag.** `value[i] - value[i-1]` can overflow int64 (INT64_MAX minus
INT64_MIN). Computing it in uint64 wraps, and because the decoder adds in
the same arithmetic the round trip is exact for every input, including
unsigned columns; `Codecs.Int64Extremes` covers it. Zigzag coding (map
signed to unsigned by interleaving) is what protobuf does for varints, but
here the frame of reference already removes the sign: subtracting the
block's minimum delta makes every packed value non-negative, so zigzag
would only add work. The one subtlety is picking the minimum: deltas are
compared as signed, which keeps the range small for the common case of
small positive and negative jitter.

**RLE over dictionary codes as two bit-packed arrays, not (code, length)
pairs.** Codes need `bits(d-1)` bits and lengths need `bits(max_run)`;
packing them separately lets each have its own width, and the decoder
reads them as two independent streams. Interleaving would force both to
the larger width or require a per-run varint. RLE_DICT won on
`event_type`, which I did not expect before measuring: 90% of rows are
impressions, so runs average about ten, and 13,288 bytes RLE beats 16,396
DICT (2 bits per value). On `device` (60/40 random) runs are short and
DICT wins 16,396 vs 30,446. The selector found that; a rule table would
have said "enum, use DICT" for both.

**Ties go to PLAIN, then DELTA_BP, then the dictionary codecs.** Strictly
smaller replaces the best so far. PLAIN decodes with one memcpy; DELTA_BP
is a dependent add per value; a dictionary lookup is a random access into
a table. The tie order is also the trial order, which is what lets the
delta size serve as the bar for the dictionary scan's early exit.

**Bit packing is a hand-written 64-bit accumulator, LSB first.** Writer
accumulates into one word and writes it when full; the tricky case is a
value straddling two words, handled by keeping the leftover high bits.
Reader loads up to 8 bytes at the bit position and, when shift + width
exceeds 64, one more byte. The layout is "bit k of the stream is bit k%8
of byte k/8", and `BitPack.EveryWidthRoundTripsAndMatchesReference`
compares every width 0..64 against a bit-by-bit reference extractor, so a
different implementation could read these files. The first version had
exactly the bug a reviewer would look for (it reset the pending-bit count
before using it to compute the leftover) and UBSan caught it on the first
ingest, which is the argument for building the sanitizer tree before the
first run rather than after.

**Decoders validate everything and never read outside the chunk.** Value
count against the footer, header lengths, code width against dictionary
size, codes against d, run lengths against the remaining rows, packed
sections against remaining bytes, and no trailing bytes. `Codecs.EveryTruncationThrowsAndNeverCrashes` truncates each encoding at every length
and expects an exception, under ASan. Narrowing a decoded 64-bit pattern
back to a u8/u16/u32 column checks the high bits too, so a corrupt chunk
cannot silently produce a wrong value. The cost is a branch per value on
the decode path, which verify's 0.137 s of total decode time for 10M rows
says is affordable.

**`--encoding plain` kept as a writer policy.** The PLAIN baseline in
every table is produced by the same binary and the same run, and the
tests that check chunk addressing arithmetically (`length == rows *
width`) force it instead of being deleted.

**Verification.** `colstore verify` PASS on the 10M file for both the
PLAIN and the auto file. 45 colstore unit tests (95 in the tree) pass in
Release and under ASan+UBSan: bit packing at every width 0..64 against a
reference, every codec on empty / one value / all equal / strictly
increasing / random full range / negative / INT64 extremes / widths 0 and
64, typed round trips for u8/u16/u32/u64/i64, truncation at every length,
hand-built corrupt DICT chunks, the selector's expected winner on
constructed inputs (random u64: PLAIN; 3 random values: DICT; 64 runs of
500: RLE_DICT; jittered timestamps: DELTA_BP; all equal: DICT; empty:
PLAIN), and pruning equivalence. The ASan+UBSan build's ingest, verify,
info and dump on the 10M file are clean (ingest 38.7 s under ASan) and its
output is byte-identical to the Release build's (`cmp`). Zero warnings
under the project flags. Sizes: 520,000,008-byte input, 160,200,997-byte
output, 3.25x; per-column table in README.md.

## Open questions after m2

- m3: zone maps exist for every column; for `event_time_us` with 5% late
  events, how many row groups does a 1-minute range query actually skip?
  The late events widen each group's [min, max] by up to 30 s.
- Delta block size 128 is a compromise (32 is 31% smaller for `event_id`,
  1024 is 8x smaller for `ingest_time_us`). Worth a per-chunk choice if m5
  needs the storage number; the format already carries the width per block.
- Per-block delta-vs-FOR flag: 5% on random integer columns.
- Decode throughput is not yet measured per codec; m3's scan will make it
  matter and `read_column` is where the numbers will come from.

## 2026-10-19: m3, vectorized scan / filter / project with zone maps

Numbers in this entry: 10M events (`gen --seed 1`, 153 row groups of
65,536), Apple M5 Pro, Release, `bench/colstore_bench.sh build-cs 10000000`;
scan times are the engine portion reported by `scan --count --stats` (file
already in page cache), three runs each, all three shown in the script
output. The four predicates used throughout:

| | predicate | rows passed | groups scanned | time |
|---|---|---:|---:|---:|
| P1 | `event_time_us BETWEEN t AND t+60s` | 599,888 | 15 of 153 | 0.006 s |
| P2 | `country=840 AND event_time_us BETWEEN t AND t+60s` | 180,421 | 15 of 153 | 0.008 s |
| P3 | `event_type=1 AND cost_micros >= 1000` | 899,878 | 153 of 153 | 0.061 s |
| P4 | `campaign_id BETWEEN 50000 AND 50100` | 10,284 | 153 of 153 | 0.042 s |

All four project `campaign_id, cost_micros`. Decoding those two columns for
all 10M rows with no predicate takes 0.036 s; the three columns P3 touches,
0.055 s. So filtering costs 6 ms per 10M rows (P4 minus decode-only, one
predicate) to 7 ms (P3, two predicates), under 1 ns per row, and
everything else is decode and I/O. That ordering, decode first and filter
a distant second, is the reason every design choice below is about
decoding less.

**Batches of 1024 rows instead of one row at a time, and instead of a
whole row group at a time.** Row-at-a-time execution pays the operator
dispatch (here: slicing ten column pointers, resetting the selection
vector, looping over the predicate list) per row, and the per-row compare
cannot be unrolled or kept in registers across rows. Measured with the
`--vector` knob, which runs the same operators at any batch size (1 is
literally row-at-a-time): P3 takes 0.137 s at 1, 0.068 s at 16, 0.065 s at
128, 0.062 s at 1024 and 0.062 s at 65,536. So 2.2x from batching, saturated
by 128, and no penalty yet for a whole row group. 1024 is still the default
because 1024 values * 8 B = 8 KiB per column keeps a handful of columns
resident in L1 while the next operator (m4's hash aggregate, which re-reads
the batch through the selection vector) consumes them; at 65,536 the
projected columns of one batch are 1 MB and every re-read comes from L2.
That claim is for m4 to measure; m3 only shows that nothing is lost at
1024. Why not smaller: at 16 the dispatch cost is already visible (10%).

**Selection vector instead of a materialized copy or a bitmap.** The
filter writes the indices of passing rows into `sel` and the consumer
reads `cols[c].at(sel[k])`; no value is copied. A compacted copy would
move every surviving value of every projected column, which at P1's 61%
selectivity is most of the data, for nothing: the consumer can read it
in place. A bitmap is compact but iterating survivors means scanning bits
and branching on each, and the next predicate would have to AND bitmaps
then rescan. The selection vector is produced branch-free: every index is
stored, the write cursor advances only on a pass (`sel[k] = i; k += pass`).
At 10-90% selectivity a conditional store mispredicts constantly; the
unconditional store costs one cycle and nothing to predict. Each later
predicate only visits the rows the previous ones kept, so putting the
selective term first (as P2 and P3 do) makes the second term nearly free.

**Every operator is one closed range over an "order key".** `<`, `<=`,
`=`, `>=`, `>` and `BETWEEN` all become `[lo, hi]` on a 64-bit key whose
unsigned order is the column's natural order: identity for unsigned
columns, sign bit flipped for `i64` (INT64_MIN maps to 0, INT64_MAX to
UINT64_MAX, every `<` preserved). The per-row test is then `(key - lo) <=
(hi - lo)`: one subtraction, one compare, no branch on the operator, and
the same code serves the filter loop, the zone-map check and the
dictionary mapping. The alternative is a switch on the operator inside
the row loop, or six copies of the loop, and then three more copies of
each for the zone map and the dictionary mapping. The edges need care: `x < 0` on an unsigned column
and `x > INT64_MAX` are empty ranges (`lo = 1, hi = 0`), not wrapped ones;
`Predicate.TypeEdgesProduceEmptyRanges` pins that. The parser refuses a
literal that does not fit the column (`country = 70000`) because that is
far more likely a typo than a deliberate "match nothing".

**Zone maps: three outcomes, not two.** Before decoding a row group the
scanner compares each predicate's range to the chunk's stored
`[min, max]`. Disjoint: skip the group, no byte read. Predicate contains
the chunk range: every row passes, so the predicate is dropped for this
group and, unless projected, its column is not decoded at all. Otherwise
decode and test. The second outcome is cheap to add and shows up in P1's
chunk count: 15 groups * 3 columns = 45, minus 2 groups whose
`event_time_us` range lay entirely inside the predicate = 43 decoded.
Measured skipping on `event_time_us`: a 10 s range scans 7 groups of 153,
60 s scans 15, 300 s scans 52. The ideal is 2, 10 and 46 (a group holds
6.55 s of ingest at 10K events/s). The gap is the late events: 5% of rows
are up to 30 s late, so every group's `min` is about 30 s below its ingest
window and each group spans about 36 s, not 6.55 s. The formula
`ceil((range + 30) / 6.55) + 1` gives 7, 15, 52, which is exactly what is
measured, so the overhead is a constant 5 groups per query, not a fraction
of the data. The fix, if a workload cared, is sorting each row group by
`event_time_us` at ingest, which costs a sort per 64K rows and would
scramble the arrival-order columns that compress best today
(`ingest_time_us` is 60x because consecutive deltas are all 100 us); not
done here, and not measured, because the honest number for a stream
stored in arrival order is the useful one.

**What happens on an unsorted column: nothing, and that is the point.**
P4 filters `campaign_id`, which is uniform random in every group, so every
`[min, max]` is nearly `[0, 100000]` and the zone map skips 0 of 153
groups; the test `ZoneMapScan.UnsortedColumnSkipsNothing` builds a
wrapping column to pin this (the test fixture's columns all turn out to be
monotone at 10K rows, including `campaign_id = 7i mod 100000`, which was
the first version of that test and failed by skipping 9 groups). Zone maps
cost one compare per group, so consulting them is free; they only pay off
on columns that are sorted or clustered, which in a time-series store is
the time column and anything correlated with it. The next lever for
unsorted columns is finer zone maps (per 128-value delta block, which the
format already has headers for; Parquet calls this a page index) or a
sort key chosen at ingest.

**Dictionary columns are filtered on codes.** Each DICT / RLE_DICT chunk
carries its own sorted dictionary, so a value range maps to a code range
with two binary searches, done once per row group (not once per query,
because the dictionaries differ per chunk), and the row test compares
`uint32` codes with the same subtract-and-compare. Two consequences. First,
a constant absent from the chunk's dictionary ("country = 500" when the
chunk only has 356 and 840) gives an empty code range and the group is
skipped before any projected column is read; the zone map cannot see this
because 356 < 500 < 840. It is counted separately in the stats. Second, a
dictionary column used only in the WHERE clause is never expanded to
values. Measured: `country=840` on the auto file (codes) takes 0.029 s
against 0.026 s to decode the column with no predicate, so 3 ms of filter;
on the PLAIN file (values) 0.024 s against 0.019 s, 5 ms. The compare is
not faster on codes in any way that matters at 0.5 ns per row; the saving
is structural (no expansion pass, no 8-byte values written for a 2-bit
column) and will matter more when m4 groups by a dictionary column, where
the code itself can be the hash key. `decode_dict_parts` /
`decode_rle_dict_parts` expose the (dictionary, codes) form; the
value-returning decoders are now those plus a table lookup, so no decode
logic was duplicated.

**Decode only the columns the query touches, filter columns first.**
The scanner decodes the predicate columns of a group, compiles the
predicates against them (which is when the dictionary mapping can skip the
group), and only then decodes the projected columns that are not already
in hand. A column that is both filtered and projected is decoded once.
The `chunks decoded` stat makes this checkable from the command line.

**`--count` exists because the first timings were wrong.** The first scan
measurements printed every passing row to `/dev/null`; a no-predicate scan
of one column came out at 0.68 s, of which 0.66 s was `printf`. With
`--count` (SELECT COUNT(*): no row formatting) the same scan is 0.026 s.
Every number in this entry is from `--count`. The lesson generalizes: time
the engine, not the terminal.

**Verification.** `colstore verify --col X --ads Y --where P --cols C`
runs P through the vectorized scan (zone maps, codes, selection vectors)
and through a brute-force loop over every `.ads` record using
`Predicate::matches` on the raw field bits, and asserts equal row counts
and equal per-column sums; the four predicates above all PASS on 10M rows
(the brute-force side takes 0.27-0.32 s, the scan 0.006-0.061 s). 21 new
unit tests (66 colstore tests, all passing in Release and under
ASan+UBSan): order-key monotonicity, every operator against the obvious
comparison on signed values, empty ranges at type edges, the parser (every
operator, BETWEEN mixed with AND, case, u64 literals, 14 error cases),
zone-map decisions at every boundary of `[100, 200]` and of a signed
`[-10, 5]` and a single-value chunk, scanner batches tiling row groups
(1024, 1024, 952, ...), selection vectors against brute force for ten
predicate sets on both an auto-encoded and a PLAIN-only file (so the code
path and the value path check each other), dictionary skipping of an
absent constant without decoding the projected column, identical answers
at batch sizes 1 / 7 / 1023 / 1024 / 1025 / 100000, and zone-map skip
counts on sorted, unsorted and always-true predicates. The ASan+UBSan
binary runs `scan` and `verify --where` on the 10M file clean. Zero
warnings in colstore sources under the project flags (the one warning in
the tree is inside GoogleTest's own `gtest-printers.h` with clang 21).

## Open questions after m3

- m4: with a hash aggregate re-reading each batch, does 1024 beat 65,536?
  Measure with `--vector` once the aggregate exists; this is where the
  L1 argument for 1024 is actually tested.
- m4: group by a dictionary column on codes (no value lookup until output).
  The scanner already hands codes to the consumer.
- Finer zone maps per 128-value delta block for range predicates on
  near-sorted columns; the late-event widening would shrink from 30 s per
  group to 30 s per block only when the late rows are clustered, so it
  needs measuring, not assuming.
- Sorting row groups by `event_time_us` at ingest (`ingest --sort`) would
  make the time-range skip ideal (10 groups, not 15, for 60 s) at the cost
  of `ingest_time_us` compression. Worth a flag if m5's benchmark query
  is the time range.
- The parser has no OR, NOT, parentheses or `!=`; `!=` is the one m4's
  queries might want.

## 2026-10-19: m4, hash aggregation, sort / limit, the query CLI

Numbers in this entry: 10M events (`gen --seed 1`, 153 row groups), Apple
M5 Pro (18 cores), Release, `bench/colstore_bench.sh build-cs 10000000`;
times are `total` from `q --stats` (scan + filter + aggregate + merge +
sort, no CSV formatting), median of 3, file in the page cache. The three
queries are defined in README.md: Q1 `country=840 AND event_type=1 ->
count()`, Q2 one-hour range `GROUP BY campaign_id -> sum(cost), count()`,
Q3 full `GROUP BY campaign_id -> impressions, clicks, conversions, sum(cost)`.

| threads | Q1 | Q2 | Q3 |
|---:|---:|---:|---:|
| 1 | 0.051 s | 0.092 s | 0.177 s |
| 2 | 0.026 s | 0.055 s | 0.113 s |
| 4 | 0.014 s | 0.042 s | 0.084 s |
| 8 | 0.008 s | 0.040 s | 0.068 s |

All three `--check` PASS (1 / 100,000 / 100,000 groups identical to the
brute force), and with `--order ... --limit 20 --threads 8` the 20
emitted rows are identical too.

**A hand-written open-addressing table instead of `std::unordered_map`.**
`unordered_map` is a node per key behind a bucket array: every lookup is
two dependent cache misses (bucket, then node), each insert is a malloc,
and growth and layout are not mine to explain. `GroupTable` is one
power-of-two array of 24-byte slots (16-byte key, 4-byte group index, 4
bytes padding); a hit is one memory access, and a probe count is exposed
so the cost is a number, not a feeling: 1.32 probes per row on Q2 and Q3
(100,000 groups in 262,144 slots, load 0.38, after 8 doublings from
1,024). Measured on Q3: decoding the three columns alone is 0.055 s,
`GROUP BY campaign_id` with a bare `count()` is 0.104 s, so the lookup
costs about 5 ns per row, and the four aggregates add 0.073 s, 1.8 ns per
row per aggregate. The table maps a key to a dense GROUP INDEX and knows
nothing about aggregates; the aggregate state lives in a flat `uint64_t`
array, `n_aggs` cells per group. That split is what makes each aggregate's
update a plain array loop and the thread merge a loop over one table's
keys. Storing the aggregates inside the slot would make the slot grow
with the query and make a new aggregate kind a table change.

**Linear probing, doubling at half load, and a mixer.** Linear probing
(next slot = adjacent slot) keeps a probe sequence inside one or two cache
lines; its weakness is clustering, which the load cap bounds: Knuth's
estimates at load 0.5 are 1.5 probes per hit and 2.5 per miss, and the
measured 1.32 sits where it should for a table running between 0.25 and
0.5 full. Why not a higher cap: at 0.9 a miss costs ~50 probes with
linear probing; the slot array is the cheap part (6 MB for 100K groups)
and memory is not the constraint here. Why a mixer (splitmix64's
finalizer) instead of the identity hash that `std::hash` uses for
integers on libc++: the slot index is the LOW bits of the hash, and a
packed key puts the second group column in the high bits, so with an
identity hash every `(campaign, country)` pair with the same campaign
would collide. The mixer costs three multiply/shift pairs; the probe
numbers above are with it, and `GroupTable.MeanProbeLength...` pins the
bound for dense sequential keys and for random ones. Growth rehashes from
the key rather than caching the hash in the slot: 8 bytes per slot saved
for one multiply per key per doubling, and doublings are rare (8 for the
whole of Q3).

**Group keys packed into 128 bits, each column kept inside one word.**
Each group column takes its own width (8/16/32/64 bits) in two 64-bit
words, laid out back to back; a column that does not fit in the rest of
the current word starts the next one, so no field straddles a word
boundary and pack / unpack are a shift and a mask. The alternatives were
one word per column (then at most two columns, and `campaign_id,
country, device` would not fit although it is 56 bits), or hashing each
column separately and comparing them one by one on every probe (slower
and the table would need to know the column list). Three wide columns do
not fit in 128 bits and are rejected at plan time with a message that
says why; that is a documented limit, not a silent fallback, because the
fix (a variable-length key) would cost every query for a case this
workload does not have. `KeyPacker.*` tests check the layout bit for bit,
including a negative i64 in the high word.

**Two passes per batch: assign groups, then one tight loop per
aggregate.** The row-at-a-time shape (`for row: for agg: switch(kind)`)
pays the switch per row per aggregate. Here the batch is first mapped
to group indices (one probe per selected row, kept in a vector), and
then each aggregate runs its own loop over the batch with the kind
switched once outside the loop. The group-index vector is 4 KB per
batch and stays in L1 across the aggregate loops. `count_if` adds 0 or 1
without a branch, the same reasoning as the m3 selection vector. The
per-aggregate cost of 1.8 ns per row is the random write into the state
array (a group's cells are adjacent, so the four aggregates of one row
touch one or two cache lines).

**Conditional aggregates (`count_if`, `sum_if`) instead of running Q3
as three filtered queries.** Three queries would decode `campaign_id` and
`cost_micros` three times and probe the table three times; the
conditional aggregates read `event_type` once and cost 1.8 ns per row
each, so Q3 is one scan at 0.177 s instead of roughly three at 0.104 s.
The condition is one `--where` term reusing the parser and
`Predicate::matches`, so `count_if(cost_micros BETWEEN 1 AND 9)` works
without new code. This is SQL's `FILTER (WHERE ...)`.

**MIN / MAX kept as order keys, SUM as a wrapping `uint64_t`.** The
m3 order key (sign bit flipped for i64) makes one unsigned compare the
natural order for every column type, so MIN / MAX are one branch with no
type dispatch in the loop and the initial values are simply all-ones and
zero. SUM adds the raw patterns: for i64 that is two's complement, so the
wrapped sum reinterpreted as signed is the exact sum (INT64_MAX + (-1) +
INT64_MIN + 0 = -2 is a test), and for narrow unsigned columns the
zero-extended values sum exactly. The output type says how to print each
column (sum of a signed column is i64, of an unsigned one u64).

**Sort after aggregation, as a permutation, partial when there is a
LIMIT.** The sort input is the result (100K rows), never the events (10M).
`sort_limit` returns row indices rather than moving rows: the table is
columnar, so moving a row would touch every column. With `LIMIT N < rows`
it uses `std::partial_sort`, a heap of N: every row past the first N is
one compare against the heap top and a log N sift only when it beats it.
Measured on the 100K-group Q3 result: full sort 7.1 ms, `LIMIT 20` 0.2 ms,
`LIMIT 1000` 0.6 ms. `std::nth_element` plus a sort of the first N would
be O(rows) on average but reorders the whole index array for a gain that
is microseconds here; the heap is the simpler story. Ties fall through to
the next ORDER BY key and finally to the row index, and the query layer
appends the group columns to the ORDER BY, so the output is a function
of the data and not of which thread saw a group first;
`QueryFiles.ThreadsDoNotChangeTheAnswer` compares the emitted rows at 1,
2, 3, 5 and 17 threads (17 is more threads than row groups, so some are
idle). Without ORDER BY or LIMIT the rows come out in first-seen order
and nothing is sorted, which is also what SQL promises.

**Threads: row groups interleaved, one reader and one table per thread,
serial merge.** Thread t of T scans row groups t, t + T, ...; interleaving
rather than contiguous ranges matters because the zone map makes the
matching groups of a time-range query contiguous (Q2 skips the first 45
groups), and a contiguous split would leave the first threads idle. Each
thread opens its own `ColReader` (a `FILE*` with fseek + fread is not
shareable) and owns its `HashAggregate`; nothing is shared while they
run, so there are no locks and TSan has nothing to say (the TSan build
runs the 105 tests and Q2 / Q3 at 8 threads on the 10M file clean). After
join, tables 1..T-1 are merged into table 0: one probe per group per
table. Scaling: Q1 (one group) is 6x on 8 threads; Q3 is 2.6x. Two
reasons, both measured. Each thread builds a FULL 100,000-group table
because every thread sees every campaign, so the table work is done T
times (8 tables of 6 MB instead of one), and the merge is serial and
linear in T: 1.2 ms per merged table, 8.4 ms at 8 threads, 18.5 ms at 16
(28% of the total; 16 threads is no faster than 8). The fixes, in the
order I would do them: merge in a tree (pairs in parallel, log T rounds),
then hash-partition the groups so thread t only ever owns keys with
`hash % T == t` (no duplicated tables, no merge, but an exchange step
between scan and aggregate). Both are a day's work each and belong in m5
if the SQLite comparison turns out to be bounded by them; the honest
single-threaded numbers are the ones above.

**Batch size with the aggregate attached: the m3 hypothesis did not
hold.** m3 chose 1024-row batches partly on the argument that the next
operator re-reads the batch and 8 KB per column stays in L1. With the
hash aggregate attached, `--vector 1024 / 8192 / 65536` give 0.180 /
0.184 / 0.184 s on Q3, a 2% spread. The dominant memory traffic is the
table's random access (6 MB of slots plus 3.2 MB of state), not the
batch, so batch residency does not show. 1024 stays the default (nothing
is lost and the dispatch cost at small batches is real), but the claim
in the m3 entry is now "measured, no effect" rather than "expected to
help".

**Group by a dictionary column goes through the dictionary.** The m3 open
question was whether to group on codes. Codes differ per chunk, so a
code-keyed table would need remapping at every row group; grouping on
values via `ColumnVector::at` (one table lookup per row) costs `GROUP BY
country` 0.094 s for 10 groups against 0.104 s for `GROUP BY campaign_id`
with 100K groups, so the lookup is not where the time goes. Not done.

**Verification.** `q --check events.ads` recomputes the whole plan from
the `.ads` file with a `std::map<vector<uint64_t>, vector<uint64_t>>`,
plain signed compares and no code shared with the table, the packer or
the scanner, then diffs group by group; with an ORDER BY / LIMIT it also
sorts the brute-force table with `std::stable_sort` and compares the
emitted rows one by one. It PASSes for Q1, Q2, Q3 at 1 thread and for Q2
/ Q3 top-20 at 8 threads on 10M rows (brute force 0.3 / 1.5 / 2.1 s,
engine 0.05 / 0.09 / 0.18 s), and for a two-column `GROUP BY campaign_id,
country` with 991,852 groups (2,097,152 slots, 1.34 probes per row, 0.426
s). `QueryFiles.CheckDetectsAWrongResult` corrupts a cell, swaps two
output rows and drops a group and expects FAIL each time, so the check
is known to be able to fail. 39 new unit tests (105 colstore tests, all
passing in Release, ASan+UBSan and TSan): table growth staying at or
under half load with power-of-two capacity (2 -> 32,768 in 14
doublings), collisions resolved by probing with absent keys terminating,
mean probe length under 2.5 for sequential and random keys, agreement
with `std::unordered_map` on 300K random keys with duplicates, key
packing layouts and the 128-bit limit, the `--agg` parser (every
function, aliases, default names, 14 error cases), every aggregate on an
eight-row batch with hand-checkable sums, selection vectors, one global
group, two-column keys, signed and unsigned extremes, merge equal to one
pass (including a group only the second half saw), 5,000 groups over 50
batches against a map, the `--order` parser, signed ordering, ties,
partial sort equal to the full sort's prefix for nine LIMITs on a 2,000-row
table with heavy ties, the three query shapes against an in-test brute
force, identical answers at 1 / 2 / 3 / 5 / 17 threads and at batch sizes
1 and 1024, the Scanner stride covering every row group exactly once, CSV
text, and bad plans rejected. Zero warnings under the project flags in
colstore sources (the one warning in the tree is still GoogleTest's own
`gtest-printers.h` under clang 21).

## Open questions after m4

- m5: the SQLite comparison. With an index on `(campaign_id,
  event_time_us)` SQLite can answer Q2 by index range scan; the fair
  comparison is probably both with and without the index, and Q3 is the
  one where a column store should win outright.
- Parallel merge (tree) and hash-partitioned aggregation, if m5 shows the
  8-thread numbers matter. Today 16 threads are no faster than 8.
- The two-column 991K-group query spends 0.0075 s in `finish()` and its
  slots are 48 MB; a table of 32-bit group indices with keys in a separate
  dense array would be 8 MB of slots and is worth measuring at that size.
- Cold-cache numbers need `sudo purge`; the script reports that it could
  not run it. Worth one manual run before the final docs.
- `--where` still has no OR / NOT / `!=`; none of the three queries needed
  them.
