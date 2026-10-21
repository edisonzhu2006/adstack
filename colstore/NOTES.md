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

## 2026-10-21: m2 re-verified from a clean build (no code change)

The resumed workflow handed over "colstore m2" as the current milestone,
but the tree was already committed through m4 (`ada8076`). Rewriting
working, tested encoders to satisfy a stale task description would have
been the wrong call, so this session re-ran m2's "done when" from a clean
rebuild and recorded what, if anything, had drifted. Nothing in
`colstore/` changed.

**What was re-run.** `ninja -t clean colstore colstore_tests` then rebuild
in `build-cs` (Release) and `build-cs-asan` (ASan+UBSan); `colstore_tests`
105/105 in both; `gen --n 10000000 --seed 1`; three timed ingests each of
`--encoding plain` and auto; `verify` on both files; `info`; ASan ingest +
verify + dump on the same 10M file and `cmp` against the Release output.

**Numbers today vs 2026-10-19.** PLAIN ingest best of 3: 0.480 s (was
0.427); auto: 1.257 s (was 1.183). Encoding therefore costs 0.777 s per
10M rows, 78 ns per row, against 0.756 s two days ago: 3% drift, within
what a different background load produces, and the ratio (auto 2.6x
slower than PLAIN) is unchanged. Verify: 0.310 s PLAIN, 0.469 s auto
(was 0.330 / 0.467). Output sizes are identical byte for byte to the
earlier run (520,074,096 PLAIN, 160,200,997 auto, 3.25x) and the
per-column table from `info` matches the README exactly, which is the
useful finding: the selector is deterministic, so the compression number
on the resume is reproducible, not a lucky run. ASan ingest 6.73 s, ASan
verify PASS 2.01 s, ASan output `cmp`-identical to Release.

**The one compiler warning, and why it is left alone rather than
suppressed from here.** With Apple clang 21 a clean build prints exactly
one warning: `gtest-printers.h:524`, `-Wcharacter-conversion` (char8_t to
char32_t), emitted while compiling GoogleTest's own `gtest-all.cc`. It is
not in a colstore translation unit (zero warnings in `colstore/src` and
`colstore/tests`); the gtest library target inherits the root
`add_compile_options` because `FetchContent_MakeAvailable(googletest)`
runs after the flags are set. Options: (a) `-Wno-character-conversion` on
the `gtest` target in the root CMakeLists, the right fix, but root CMake
is not this track's to edit; (b) reach into the `gtest` target from
`colstore/CMakeLists.txt` with `target_compile_options(gtest ...)`,
which works mechanically but is exactly the cross-ownership edit the plan
forbids and would surprise the streamkit track, whose tests link the same
target; (c) weaken the project flags, ruled out. I also tried re-adding
gtest's include directory as `SYSTEM` on `colstore_tests`; it had no
effect, because googletest already exports its headers as SYSTEM and the
diagnostic is raised in gtest's own compilation unit, not in ours, so the
change was reverted. Recorded as an open item for the root-CMake pass.

## 2026-10-21: m3 re-verified from a clean build (one comment-level edit)

Same situation as the m2 entry above: the resumed workflow handed over
"colstore m3" with "m1-m2 done", but the tree is committed through m4
(`ada8076`), and m3's scanner, filter, zone maps, dictionary-code
predicates, `--where` parser, `scan` CLI, `verify --where` and tests are
all in place. Rather than rewrite them to match a stale description, this
session (1) walked the m3 task list against the code item by item,
(2) re-ran m3's "done when" from a clean rebuild, and (3) recorded the
drift. The only source change is a two-line reorder in
`Scanner::decode_column` (below).

**Item-by-item check against the m3 brief.** Batches of up to 1024 values
per column with pointers into the decoded row group, not copies: `Batch` /
`ColumnVector` in `exec.h`. Scan reads only the projected and filtered
columns of one row group at a time: `Scanner::load_row_group` steps 2 and
3; `chunks decoded` in `--stats` proves it (43 chunks for 15 groups x 3
columns, minus 2 dropped predicates). Filter produces a selection vector
of passing indices, branch-free: `refine()` in `exec.cpp`. Zone-map skip
before decode, counted and printed: `row_groups_skipped_zone`. Equality
on a dictionary column maps the constant to a code once per row group:
`compile_predicate`, two binary searches on the chunk's sorted dictionary.
CLI `scan --in --where --cols --limit --stats`: `cmd_scan`. Brute-force
check against the raw `.ads` for the predicates with identical row counts
and column sums: `cmd_verify_where`, run on four predicates by
`bench/colstore_bench.sh`. Tests for selection vectors, zone-map boundary
values and the parser: `exec_test.cpp`, `predicate_test.cpp` (21 tests).
Nothing on the list is missing, so there was nothing to build.

**The one edit, and why it is not a behavior change.** `decode_column`
cast `m.encoding` to `Encoding` on one line and range-checked it on the
next. That order is legal (the enum has a fixed `uint32_t` underlying
type, so an out-of-range cast is well-defined, and the value was never
used before the check), but a reader has to know that rule to see the
code is safe. The check now comes first with a comment saying why. The
alternative was to leave it, since it was correct; the reorder was chosen
because "explainable without a language-lawyer argument" is this
project's bar. Cost: a rebuild of one translation unit. Verified by
rebuilding all three trees and rerunning the tests (below).

**What was re-run.** `ninja -t clean` then full rebuild of `build-cs`
(Release) and `build-cs-asan` (ASan+UBSan); `colstore_tests` 105/105 in
both (ctest reports 106 because gtest discovery adds one entry);
`gen --n 10000000 --seed 1` (9,899,652 unique, 100,348 dups, 496,040
late); `bench/colstore_bench.sh build-cs 10000000` end to end; ASan
`scan --count --stats` on the four bench predicates plus `--vector 1` and
`--vector 65536`; ASan `verify --where` on three of them; the README's
`--limit 10` example under ASan; `colstore_tests` under TSan and an
8-thread `q` on the 10M file under TSan (m3's scanner is what each thread
runs). Log: `/private/tmp/adstack-cs/bench_rerun_2026-10-21.txt`. Load
average was 4-5 throughout because the streamkit track was building in
parallel on the same 18-core machine.

**Numbers today vs 2026-10-19 (10M rows, 153 groups, `--count`).** Every
count is identical: rows passed 599,888 / 180,421 / 899,878 / 10,284 for
P1-P4; groups scanned 15 / 15 / 153 / 153; chunks decoded 43 / 58 / 459 /
306; zone-map skipping for 10 s / 60 s / 300 s ranges 146 / 138 / 101 of
153 (7 / 15 / 52 scanned). As with m2, the useful finding is that the
pruning and decode counts are deterministic, so the "138 of 153 skipped"
headline reproduces exactly. Times: P1 0.006 s (was 0.006), P2 0.008
(0.008), P3 0.062-0.063 (0.061-0.063), P4 0.042-0.044 (0.041-0.042),
decode-only two columns 0.036-0.039 (same), three columns 0.056 (0.055).
Batch sweep on P3: 1 row 0.140-0.145 (was 0.136-0.141), 16 0.069-0.070,
128 0.063, 1024 0.062-0.063, 8192 0.062, 65536 0.062-0.065; the 2.2x
row-at-a-time penalty and the saturation by 128 both hold. Dictionary
codes vs values, `country=840`: auto 0.029-0.030 vs plain 0.023-0.024;
no-predicate decode 0.024-0.025 vs 0.019-0.020, so the filter still adds
about 5 ms either way. Largest drift anywhere is 5% (P4), within the
background-load noise. All four `verify --where` PASS with the same sums
as on the 19th (e.g. P3 `sum(cost_micros)` 944,501,914,095).

**Sanitizers.** ASan+UBSan: the four scans run clean at 0.037 / 0.037 /
0.284 / 0.220 s (6-7x the Release time, the usual ASan tax), `verify
--where` PASS for P2 / P3 / P4 with brute force at 0.48-0.50 s, the
`--limit 10` example prints 10 rows and stops after 1 group (the stats
line says so; "rows passed" is then only the batches seen). TSan: 105/105
tests, and the 8-thread query over P2 reports 138 of 153 groups skipped
and no race. The one compiler warning is still gtest's own
`gtest-printers.h:524` and is handled as recorded in the m2 entry.

**What a reviewer should take from this entry.** m3's claims are
reproducible from one script with no parameter hidden in a shell
history; the counts are exact and the times move by at most 5% between
days; and the "unsorted column skips nothing" explanation in the original
m3 entry (zone maps need the filtered column to be sorted or clustered;
`campaign_id` is uniform within every group, so every `[min, max]` covers
the whole domain and 0 of 153 groups are skipped for P4) is still what
the data shows.

## 2026-10-21: m4 re-verified from a clean build (one test-helper fix, one wording fix)

Same situation as the m2 and m3 entries: the resumed workflow handed over
"colstore m4" with "m1-m3 done", but the hash aggregate, sort / limit,
`colstore q`, `--check`, the thread-parallel scan and 39 tests were
committed on the 19th (`ada8076`). This session walked the m4 brief
against the code item by item, re-ran the "done when" from a clean rebuild
of all three trees, and found one real bug, in the test helper, not the
engine.

**Item-by-item check against the m4 brief.** GROUP BY on one or more
integer columns with `count() / sum / min / max`: `HashAggregate` in
`aggregate.h`, keys packed by `KeyPacker`, rejected at plan time past 128
bits. Own open-addressing table with the probing (linear) and growth
(power of two, doubling at half load) policy commented: `hash_table.h`,
first 40 lines. Consumes selection vectors: `consume()` reads rows through
`Batch::row(k)`. Sort and limit with a partial sort when N is small, and
why: `sort.h`. CLI `q --in --where --group --agg --order --limit --stats`
with CSV on stdout: `cmd_q` in `main.cpp`. Q1 / Q2 / Q3 documented with
their SQL: README "Benchmark queries (m4)". Brute-force verification from
the `.ads` file: `q --check`, `std::map` and plain signed compares, no
code shared with the table or the scanner, and `CheckDetectsAWrongResult`
proves it can fail. Times on 10M, median of 3, cold vs warm noted: the
bench script, and the cold run is still "not measured" (below).
Multithreaded scan, row-group-parallel with per-thread tables merged at
the end, TSan clean: `run_aggregate_query`. Tests for growth and
collisions (`GroupTable.*`), aggregates on constructed data
(`HashAggregate.*` on an eight-row batch), ORDER BY / LIMIT
(`SortLimit.*`, nine LIMITs against the full sort). Nothing on the list
was missing, so nothing new was built.

**The bug: fixture temp files collided under `ctest -j 8`.** The direct
`colstore_tests` run passed 105/105 in every tree, but `ctest -j 8`
failed 10 of 106 on the first try (`AutoFixture.MinMaxStoredForEncodedChunks`,
`AutoFixture.ReadColumnBitsMatchesTypedRead`, two `ExecFiles.*`, six
`QueryFiles.*`), all with "unknown file: Failure", and every one of them
passed under `--rerun-failed`, which runs them one at a time. Cause:
`TempFile` in `tests/test_util.h` built its path from a fixed name
(`colstore_test_query.col`) in the shared temp directory. gtest discovery
makes every test its own ctest entry, and ctest runs them as separate
PROCESSES; with `-j 8`, two `QueryFiles` fixtures in two processes wrote
and deleted the same file. The engine was never wrong; a test read a file
another test had just removed. Why it was not seen on the 19th: every run
then was the single-process binary or serial ctest. Fix: every temp name
now carries a per-process token drawn once from `std::random_device`
(64 bits, so a collision between two concurrent test processes is not a
realistic event). Alternatives: `getpid()` is the conventional answer and
would have been one line, but it is POSIX and the project rule is standard
library only, and the rule is cheaper to keep than to explain an
exception to; a per-TEST unique name (counter) would not help, because the
colliding tests are the same test code in different processes. `std::tmpfile`
cannot be used because the code under test opens files by path. Cost:
one `std::to_string` per process. Verified: 8 consecutive `ctest -j 8`
runs per tree (Release, ASan+UBSan, TSan), 24 runs, all 100%, where the
first parallel run before the fix failed 10. Also recorded, correcting the
m3 entry's guess: ctest shows 106 entries in the Release and ASan trees
and 105 in the TSan tree because the README's `-R` pattern contains `Agg`,
which also matches `Agg.CountsByTypeAndSumsCost` from `tests/` (common),
and the TSan tree built only the colstore targets. The extra entry is a
real `common/` test, not a discovery artifact.

**The wording fix.** At 8 threads `--stats` printed "table 262144 slots,
64 doublings", which reads as a 1,024-slot table that doubled 64 times.
`AggStats` sums doublings and probes over every thread's table and the
merge (so that probes per row covers all the work), and `aggregate.h` says
so, but the printed line did not. It now ends with "(doublings and probes
summed over all threads' tables and the merge)"; at 16 threads on the
two-column query the line is 181 characters, inside the 256-byte buffer.
The alternative was to print thread 0's counts only, which would hide 7/8
of the probes. Nothing parses that line (the bench script prints it as
is).

**What was re-run.** `ninja -t clean` then full rebuild of `build-cs`
(Release, 62 steps) and `build-cs-asan` (ASan+UBSan, 62 steps), and the
colstore targets in `build-cs-tsan` (33 steps): one warning in each,
GoogleTest's own `gtest-printers.h:524` (handled as recorded in the m2
entry), zero in colstore sources, including after the two edits above.
`colstore_tests` 105/105 direct in all three trees; `ctest -j 8` as
above. `bench/colstore_bench.sh build-cs 10000000` end to end (log:
`/private/tmp/adstack-cs/bench_m4_rerun_2026-10-21.txt`). ASan+UBSan on
the 10M file: `q --check` for Q1, Q2, Q3 at one thread and Q2 / Q3 top-20
at 8 threads, plus `GROUP BY campaign_id, country` at 4 threads. TSan on
the 10M file: Q2 and Q3 top-20 at 8 threads with `--check`, and Q3 at 16
threads. Load average 3.7-5.9 throughout: the streamkit track was
building and benchmarking on the same machine, as on the m3 re-run.

**Numbers today vs 2026-10-19 (10M rows, 153 groups, `q --stats` total,
median of 3, page cache warm).** Q1 / Q2 / Q3 at 1 thread 0.053 / 0.098 /
0.200 s (were 0.051 / 0.092 / 0.177); at 2 threads 0.028 / 0.060 / 0.121
(0.026 / 0.055 / 0.113); at 4 threads 0.014 / 0.045 / 0.087 (0.014 /
0.042 / 0.084); at 8 threads 0.008 / 0.040 / 0.070 (0.008 / 0.040 /
0.068). The final binary (after the wording edit) re-timed at 1 thread
0.054 / 0.102 / 0.201 and at 8 threads 0.008 / 0.040 / 0.069 with load
5.9. The largest drift is Q3 at one thread, +13%. It is not an aggregate
regression: the same run's m2 / m3 numbers moved by the same amount
(auto ingest best of 3 1.289 s vs 1.183; the full-scan predicate P3
0.066-0.070 s vs 0.061-0.063; three-column decode-only 0.060-0.061 vs
0.055-0.056), i.e. everything single-threaded is 8-13% slower under
today's background load, and the 8-thread numbers, which depend less on
any one core being free, are within 3%. Every count is identical: Q1
269,609 rows; Q2 45 of 153 groups skipped, 6,990,857 rows in 6,837
batches, 100,000 groups in 262,144 slots after 8 doublings, 1.32 probes
per row; Q3 9,766 batches, same table, same probes. Full sort of the
100K-row Q3 result 7.4-7.7 ms (was 7.0-7.1), `LIMIT 20` 0.2 ms (same).
Batch size with the aggregate attached 0.202 / 0.206 / 0.205 s for 1024 /
8192 / 65536, the same 2% spread, so the "no effect" finding holds. All
five `--check` runs PASS (brute force 0.337 / 1.878 / 2.497 s for Q1 / Q2
/ Q3, engine 0.054 / 0.105 / 0.202 s).

**Sanitizers.** ASan+UBSan: Q1 / Q2 / Q3 `--check` PASS at 0.197 / 0.360
/ 0.895 s engine time (3.7-4.5x Release, the usual tax), Q2 / Q3 top-20
at 8 threads PASS at 0.138 / 0.240 s, and the two-column query with
991,852 groups in 2,097,152 slots (run for the table size, no `--check`)
finished clean at 0.717 s; no reports. TSan: Q2 / Q3 top-20 at 8 threads `--check` PASS at 0.575 /
1.214 s (the brute force takes 17.7 / 26.7 s under TSan), Q3 at 16 threads
1.184 s, 105/105 tests, 8 parallel ctest runs; no reports. The cold-cache
measurement is still not possible from this account (`sudo -n true`:
"a password is required"), so every number above is warm; the script
says so in its output.

**Open items noticed but left alone.** `effective_order()` takes a
`schema` argument it does not use (`(void)schema;`); it is part of the
signature the CLI and a test call, and removing it is churn with no
behavior change, so it stays until the m5 pass touches that file.
`HashAggregate::consume` rebuilds `columns_needed()` (a small vector) per
batch, 9,766 times on Q3; hoisting it is a micro-optimisation that would
need measuring to justify and is below the noise measured above.

## 2026-10-21: m5, the SQLite comparison and the write-up

Numbers in this entry: 10M events (`gen --seed 1`), Apple M5 Pro, Release,
`bench/colstore_bench.sh build-cs 10000000 /private/tmp/adstack-cs m5`
(log `/private/tmp/adstack-cs/bench_m5_final_2026-10-21.txt`); medians of
3, page cache warm. The full tables, the machine, the PRAGMAs and the
plans are in BENCHMARKS.md; this entry is about the decisions.

| | SQLite best plan | colstore 1 thread | colstore 8 threads |
|---|---:|---:|---:|
| Q1 point filter + count | **0.0028 s** (covering index) | 0.055 s | 0.008 s |
| Q2 1 h range GROUP BY | 4.008 s (`NOT INDEXED`) | **0.094 s** (42.6x) | 0.042 s |
| Q3 full GROUP BY | 5.267 s (index-ordered scan) | **0.188 s** (28.0x) | 0.069 s |
| bytes | 899,682,304 (1.73x raw, with indexes) | 160,200,997 (0.31x) | |
| load | 16.5 s (3.1 insert + 12.9 indexes + 0.6 ANALYZE) | 1.3 s | |

Answers identical on all three (CSV diff, 1 / 100,000 / 100,000 rows).

**A C++ loader over the sqlite3 C API instead of the `sqlite3` shell's
`.import`.** The shell route would be: export the `.ads` rows to CSV,
`.import` them, type the columns by affinity, and time with `.timer`
from a script. That is three text conversions of 10M rows and a
measurement whose settings live in a shell transcript. The C API gives
the documented fast load (one transaction, one prepared `INSERT` bound
per row, 3.1 s for 10M rows), the PRAGMAs as code the binary prints, and
the rows fed from the same `event_field_bits()` view that `verify` and
`q --check` use, so both engines provably received the same 64-bit
patterns. A Python loader (the bundled interpreter has sqlite3) would
have been less code but a second language in a C++ project and a
10x-slower insert loop that would have muddied the load comparison.
Cost: the only third-party dependency in colstore. It is fenced: its own
static library (`colstore_sqlite_loader`) and binary (`colstore_sqlite`),
found with `find_path`/`find_library`, and when sqlite3 is absent the
targets and the test file are skipped with a `message(STATUS)` while
`colstore_core`, the CLI and the other 105 tests build unchanged. The
`src/*.cpp` glob excludes `sqlite` so the core library cannot pick the
loader up by accident.

**The schema is generated from colstore's column table.** `CREATE TABLE`
is built from `all_columns()`, the bind loop iterates the same table, and
the test checks the statement lists every column in `ColumnId` order. A
hand-written DDL would silently fall behind a schema change in
`common/event.h`. All ten columns are `INTEGER NOT NULL`, which lets
SQLite store each as a 1-9 byte varint; that is its row-level
compression and it is worth 6% here (488 MB against 520 MB raw).

**u64 stored as the signed reinterpretation.** SQLite integers are signed
64-bit. `event_id` and `video_id` are `u64`; they go in as
`static_cast<int64_t>(bits)` and come back exactly through
`static_cast<uint64_t>`, as `U64AboveInt64MaxRoundTrips...` pins with
`UINT64_MAX` and `2^63 + 5`. The alternative, 8-byte BLOBs, keeps the
unsigned order but makes every comparison and index on those columns a
bytewise memcmp and would have handicapped SQLite for nothing: no
benchmark query reads either column. Documented in the header rather
than hidden.

**What "fair to SQLite" meant in practice, and what it cost.** Load with
`journal_mode = OFF` and `synchronous = OFF` (a fresh bulk load has
nothing to protect; colstore's writer has no durability either), 1 GiB
cache, `temp_store = MEMORY`, exclusive locking; indexes created after
the rows (faster than maintaining them per insert), then `ANALYZE`.
Query with a 1 GiB cache and `mmap_size` at the 1 GiB maximum this build
allows, three timed runs on one connection after an untimed first run,
so SQLite's own page cache is warm; colstore gets no equivalent (each run
is a fresh process reading through `fread`). Both were checked before
being fixed: a fresh connection per run and `mmap_size = 0` each moved
Q2 by less than the run-to-run spread (numbers in BENCHMARKS.md). The
one rule that mattered: every query runs both as the planner chooses and
with `NOT INDEXED`, and the better one is the SQLite number. It mattered
because the planner picked the index range scan for Q2 (5.0 s) when a
table scan was faster (4.0 s), and the index-ordered scan for Q3
(5.3 s) when that was the better one (scan 7.5 s). Reporting only the
planner's choice would have been 53x instead of 42.6x on Q2; reporting
only `NOT INDEXED` would have been 40x instead of 28x on Q3. Not done:
a covering index for Q2 or a 64 KiB page size, both of which would help
SQLite and neither of which the brief named; BENCHMARKS.md says so.

**The cross-engine diff is a second oracle, not a replacement for
`--check`.** The script writes each query's CSV from both engines, sorts
and diffs them (colstore aliases nothing; the SQL aliases its output
columns to colstore's default names so the header lines match too). It
runs in milliseconds against SQLite's answer, where `--check`'s brute
force takes seconds, and it catches a class of error `--check` cannot:
both the engine and the brute force sharing a misreading of the
predicate semantics (`BETWEEN` inclusive at both ends, `count_if` on
`event_type`), since SQLite's interpretation of the same SQL is
independent of any colstore code. Q2 had 0 matching rows on the 1M
sanitizer file (1M events span 100 s, `T0` is at 300 s) and the diff of
the two one-line CSVs still passes, which is the right answer, not a
vacuous one.

**Q1 is lost, and the reason is the design, not a bug.** 2.8 ms against
55 ms. The `(country, event_type)` index is a covering index for
`COUNT(*)`: SQLite counts 269,609 B-tree entries at 10 ns each and never
opens the table. colstore has no secondary index; every row group holds
country 840, so the zone map skips nothing, and it decodes two columns of
all 10M rows at 5.5 ns per row. The m4 "Open questions" guessed SQLite
would win Q2 with a `(campaign_id, event_time_us)` index; it won Q1
instead, for the more basic reason that an equality filter with a small
answer is what a B-tree is for. The honest statement for the resume is
"28x to 43x faster on GROUP BY", not "faster than SQLite". The fix is
described below (small-domain bitmaps); it was not built because the
brief for m5 is the comparison, and because a number that was tuned
after seeing the opponent's result is a different kind of number.

**Why Q2 and Q3 are won by 28x to 43x: three effects that stack.** (1) No
row reconstruction: SQLite walks each row's record header to extract 3
of 10 columns (527 ns per row on the index-ordered scan, 753 ns on the
plain scan); colstore reads three chunk sequences and never sees the
other seven (18.8 ns per row on Q3, 13.3 ns per scanned row on Q2). (2)
Encoding-aware scanning: `event_type` is filtered as RLE dictionary
codes, `campaign_id` and `cost_micros` are delta-coded and decode at
about a nanosecond a value; Q3's input is 54 MB of chunks against 488 MB
of records. (3) Zone maps: Q2 drops 45 of 153 groups (2.95M rows) from
the footer, where SQLite's index plan walks the time index and then does
7M rowid lookups plus a temp B-tree for the GROUP BY, 716 ns per matched
row, slower than its own full scan. The hash aggregate with a dense group
index (m4) is the fourth, smaller, factor against SQLite's sorted temp
B-tree over 100,000 groups. Storage follows from (2): 16 bytes per row
against 49 for the table and 90 with the indexes, 5.6x smaller than the
database the queries ran on. Load follows from having no B-trees to
build: 1.3 s against 16.5 s, of which 12.9 s is the three `CREATE
INDEX`.

**Script changes.** The generated `.ads` is now cached in the work
directory (default `/private/tmp/adstack-cs/`), keyed by N in the file
name and checked by size (8 + 52 N bytes) before reuse, because `gen` is
deterministic and the file took the longest to produce; everything else
the script writes is deleted on exit. A fourth argument selects the
stage (`all` or `m5`) so the SQLite section, which is three minutes
mostly spent inside SQLite, can be rerun alone. The median helper and the
awk ratios are the only arithmetic in the script; the ratio prints two
decimals below 1 so "SQLite is faster" is still a number (`0.05x`)
rather than `0.0x`, which the first full run printed.

**Verification.** Release, ASan+UBSan and TSan trees reconfigured and
rebuilt with the new targets, zero warnings in every tree (the SDK's
`sqlite3.h` is included as a SYSTEM header; the one warning the project
has ever shown, GoogleTest's `gtest-printers.h`, did not appear in the
colstore-only sanitizer builds). 9 new tests (`SqliteLoader.*`, 114
colstore tests in all): schema from the column table, every row loaded
with the sums and the minimum (negative) cost matching a brute force,
three indexes and `sqlite_stat1` present, `--no-index`/`--no-analyze`
honoured, u64 extremes round-tripping, a Q3-shaped GROUP BY against a
`std::map`, the CSV shape, the covering-index plan for Q1, `dbstat`
sizes covering the four objects within the file size, repeat runs and
error paths (bad SQL throws, a write on the read-only connection
throws). `ctest -j 8` in Release: 193/193. ASan: 114/114 tests, the
loader and all three queries on a 1M-row file with the answers diffed
against colstore's (identical), `verify` on the 10M file (PASS, 2.0 s),
Q1 / Q2 `--check` at one thread and Q3 top-20 at 8 threads (PASS); no
reports. TSan: 114/114, Q3 at 8 threads on 10M (1.17 s), the loader on
1M; no reports. The full script (m1-m5) ran end to end once (exit 0,
`bench_m5_2026-10-21.txt`, m1-m4 counts identical to the m4 re-run,
Q1 / Q2 / Q3 0.051 / 0.094 / 0.183 s at one thread), then the m5 stage
alone for the tables (exit 0). Load average 2.8-4.2 throughout.

**What I would change next, in the order I would do it.**

1. *Small-domain bitmaps for dictionary columns, to stop losing Q1.* For
   each DICT / RLE_DICT chunk, store one bitmap per dictionary value
   (10 values for `country`, 3 for `event_type`: 13 bitmaps of 8 KiB per
   row group, 16.3 MB for 153 groups, a tenth of the file before
   compressing them; the `event_type` bitmaps are long runs and would
   shrink to almost nothing). `country = 840 AND event_type = 1` becomes
   an AND of two 8 KiB bitmaps and a popcount per row group, with no
   column decoded and no per-row work at all; the same bitmaps drive the
   selection vector for a subsequent GROUP BY. The cost to measure is
   the ingest time of building them and the footer growth. This is the column-store answer to an equality
   filter and it is cheaper than a B-tree because the domain is small.
   It does nothing for high-cardinality columns (`campaign_id`), which
   is the right limit: those are the GROUP BY keys, not the filters.
2. *Hash-partitioned parallel aggregation with a tree merge.* 8 threads
   give 2.7x on Q3 because every thread builds the full 100K-group table
   and the merge is serial (m4 measured 1.2 ms per merged table). First
   the cheap fix, merging pairs in parallel (log T rounds), then the
   real one: an exchange step that routes each selected row to the
   thread owning `hash(key) % T`, so each thread holds a disjoint 1/T of
   the groups, there is no merge and the tables fit in cache. The cost
   is a partition buffer per thread pair. The ceiling is known: Q1 (one
   group, nothing to merge) already scales 6.9x on 8 threads (0.055 s to
   0.008 s), and that is what Q3 would approach instead of 2.7x.
3. *LZ4 or zstd on top of the encodings, with a measured crossover.*
   The encodings leave `campaign_id` at 1.7x and `video_id` at 2.9x,
   and bit-packed deltas still have byte-level structure a general
   compressor finds. On this machine every query is CPU-bound with the
   file in the page cache, so block compression would make warm queries
   slower (a byte-oriented decompressor adds a pass of the same order as
   the delta decode itself) and cold ones faster, in proportion to the
   bytes not read from disk. The right design is per-chunk and
   optional, chosen by the same "try it and keep the smaller" selector,
   with the cold-cache run that this account cannot do yet (`sudo
   purge`) as the deciding measurement.
4. *Statistics-based encoding choice.* Auto ingest takes 1.31 s against
   0.41 s for PLAIN: two thirds of ingest is encoding every candidate and
   keeping the smallest. A pass that computes distinct count, max delta
   width and mean run length (one scan, no output) could pick the
   encoding without materialising the losers. The target is easy on this
   file: `info` shows every column ended up with ONE encoding across all
   153 chunks, so a per-column rule would have been right 1,530 times
   out of 1,530. The upper bound on the saving is the 0.90 s between
   auto and PLAIN ingest (1.313 s vs 0.412 s); how much of it a
   statistics pass keeps is the measurement. The risk is a wrong
   prediction on data unlike this generator's, so keep the exhaustive
   mode behind a flag and test that the predictor matches it on the
   real-data adapters' output.
5. *Late materialization for selective queries.* The scanner decodes a
   whole chunk for every column a query touches, then filters. For a
   predicate that passes 1% of rows and projects five columns that is
   99% wasted decode on four of them. DELTA_BP's 128-value blocks allow
   decoding only the blocks that contain selected rows, and DICT chunks
   can look up single codes; the batch would carry row positions plus
   lazily decoded columns. Q1-Q3 do not need it (they project what they
   filter), which is why it was not built; a `scan --cols` with ten
   columns and a 60 s range would show it.
6. *A SQL frontend.* Q3 needed a bespoke `count_if(...) AS name` syntax
   where SQL has `COUNT(*) FILTER (WHERE ...)`. The operators exist
   (scan, filter, project, hash aggregate, sort, limit); a parser for
   `SELECT ... FROM events WHERE ... GROUP BY ... ORDER BY ... LIMIT`
   over them is a few hundred lines and would let the benchmark feed
   the same text to both engines, removing the hand translation in the
   script as a source of error. OR and NOT in the WHERE clause would
   come with it; today a disjunction has to be two queries.

Also worth doing, smaller: sort each row group by `event_time_us` at
ingest so the zone map stops paying the constant 5 extra groups per time
range (m3), at the cost of scrambling the arrival-order columns that
compress best today, which makes it a measurement rather than an
obvious win; and the two leftovers from the m4 re-verify
(`effective_order()`'s unused parameter, `columns_needed()` rebuilt per
batch), still untouched because neither changes a number.

## 2026-10-21: integration, the schema in the footer and the CSV boundary

**Problem.** The plan's integration step: `colstore ingest` takes
streamkit's aggregate CSV as a second input kind, so `gen -> streamkit ->
colstore -> query` runs end to end, with `bench/e2e.sh` proving every
stage in one report. The m1 format hardcoded the ten `adstack::Event`
columns: `kNumColumns = 10`, `std::array<ChunkMeta, 10>` in every row
group, a global `column_spec(ColumnId)` table that the writer, reader,
scanner, predicate parser, key packer and aggregate parser all consulted.
A six-column table of `(window_start_us, campaign_id, impressions,
clicks, conversions, cost_micros)` does not fit that, so the question was
how to generalize without rewriting the engine or slowing the event path.

### Decision 1: the schema is data in the file, format version 2

The footer now starts with the column list (per column: `uint32 type`,
`uint32 name_len`, the name), before the row-group records, and
`kColVersion` is 2. A `Schema` is a `std::vector<ColumnSpec{name, type}>`;
a `ColumnId` is the column's INDEX in the schema (the named `ColumnId::`
members are the event table's indices, kept so the existing code and 100+
test call sites still read as before). Every component that used to call
`column_spec(id)` now gets the schema from the one place that owns it: the
writer from its constructor, the reader from the footer, the scanner from
the reader, and the two parsers from an argument (defaulting to the event
schema, which is what nine of ten callers mean).

Alternatives:

- *Keep version 1 and add a second hardcoded table* (`kNumColumns` becomes
  a per-table constant, `std::variant` of two column enums). Smallest
  diff, but every third table is another fork, the reader cannot open a
  file without being told which table it is, and "what columns does this
  file have" stays a compile-time fact about the binary rather than a
  fact about the file. Parquet, ORC and Arrow all carry the schema; the
  reason is exactly that a reader must be able to open a file it was not
  compiled for.
- *A schema file next to the .col file* (sidecar). Two files to keep
  together; a copy of one without the other is unreadable. The footer
  already exists and is already variable-length, so 8 bytes plus the name
  per column is the cheapest possible place: 181 bytes for the event
  table, 113 for the aggregate table, against a 74 KB footer for 10M
  rows.
- *Infer the schema from the CSV header* rather than requiring streamkit's
  exact header. Rejected for now: the types cannot be inferred from
  decimal text (is `campaign_id` u32 or u64?), and a wrong guess would
  silently change what `--where campaign_id < 0` means. The CSV reader
  takes the schema it must match (`aggregate_schema()`), and the header
  string is the check that the file is streamkit's. A `--schema` flag
  naming the types would make the CSV path general; nothing needs it yet.

What it costs: one version bump (version-1 files are refused with a clear
message; none were persistent, the benches regenerate them in seconds);
`RowGroupMeta::chunks` and `Batch::cols` are `std::vector`s instead of
`std::array`s (sized once per scanner, not per batch, so the hot loops
see no difference: Q1-Q3 at 1 thread are 0.050 / 0.094 / 0.180 s against
0.051 / 0.092 / 0.177 s before); and 181 extra footer bytes per file.

### Decision 2: a predicate and an aggregate spec carry their column's type and name

`Predicate` gained `type` and `column_name`, `AggSpec` gained `col_type`
and `col_name`, copied from the schema they were parsed against. Two
reasons. `Predicate::matches()` runs per row inside `count_if` / `sum_if`
and must not look a type up; before, it called the global table, which
no longer exists. And the copies are what make cross-table mistakes
detectable: the scanner checks each predicate's index, type and name
against the file's schema, `HashAggregate` does the same for every spec,
and `run_aggregate_query` compares the whole `plan.schema` with the file.
So a `--where campaign_id = 7` parsed for the event table (index 5, u32)
run on an aggregate file (where index 5 is `cost_micros`, i64) is an error
naming the column, not a comparison of the wrong bytes. The alternative,
threading a `const Schema&` into every `matches()` call, would have put
a pointer dereference per row in the two conditional-aggregate loops and
still not caught the mismatch.

Cost: `Predicate` and `AggSpec` hold a `std::string`, so they are no
longer trivially copyable. Neither is copied on a hot path (a
`CompiledPredicate`, which is, holds only integers).

### Decision 3: the writer buffers bytes at natural width, any schema, one code path

`ColWriter` replaced its ten typed `std::vector`s with one
`ColumnBuffer{type, width, bytes, used}` per schema column: a byte array
sized once for a full row group, filled at the column's own width. A
row arrives as `const uint64_t*` (one widened pattern per column, the
same representation the predicates and the brute-force checks use), and
`append(Event)` is `event_row_bits()` plus `append_row()`. The point of
natural width is that the buffer IS the PLAIN chunk: flushing PLAIN is a
`write()` of the buffer, no narrowing pass, and the typed `min/max` for
the zone map runs on the buffer as it stands (`min_max_bytes`, one
`std::minmax_element` at the right width). The 64-bit view that the
codecs need is built only under `--encoding auto`.

The first version of this cost real time, which the harness caught:
`vector::insert` of a 1 to 8 byte range per value, a widen pass for
PLAIN chunks that never used it, a branchy 64-bit min/max, and ten switch
dispatches per row in `event_field_bits()` took the 10M-row PLAIN ingest
from 0.47 s to 0.83 s and auto from 1.26 s to 1.59 s. Fixed in the same
session by pre-sizing the buffers and writing with a fixed-size `memcpy`
per width, computing min/max typed from the bytes, widening only under
auto, and writing `event_row_bits()` out field by field: PLAIN 0.467 s
(best of 3; 0.412 to 0.480 s across earlier sessions) and auto 1.226 s
(1.183 to 1.313 s before). The files are byte-identical to the typed
writer's apart from the 181-byte schema in the footer (520,074,277 vs
520,074,096 PLAIN; 160,201,178 vs 160,200,997 auto), and a test writes
1,000 events through `append(Event)` and through `append_row()` and
compares the two files byte for byte.

Alternative kept in mind: a `std::variant` of typed vectors per column.
It keeps everything typed but needs a visitor at every append, which is
the switch I just removed, and it is a page of template code a student
would have to explain. The byte buffer is three lines.

### Decision 4: one RowSource for "the original rows", so every proof covers both tables

`verify`, `verify --where`, `q --check` and ingest all walk the original
rows once. `RowSource` (`next(std::vector<uint64_t>&)` in some schema) has
two implementations, `AdsRowSource` over `EventReader` and `CsvRowSource`
over the CSV, and the four consumers are written once against it. That is
why the aggregate table got `verify --csv` and `q --check-csv` for free,
and why `bench/e2e.sh` can prove the aggregate query the same way it
proves Q1-Q3. The CSV reader is strict on purpose (exact header, exactly
six decimal integers per line, each fitting its type, line number in
every error): it exists to prove a pipeline, and a tolerant parser would
turn a corrupted row into a plausible number.

### Decision 5: the boundary between the two engines stays streamkit's CSV

Why not a binary record or an Arrow stream, and what either would buy,
is written up in `streamkit/NOTES.md` (same date) from the producer's
side. From colstore's side the measurement is: 3,735,840 aggregate rows
(127 MB of text, 7.7x the 16.6 MB `.col`) parse at 7.2M rows/s, 0.52 s,
of which the `from_chars` calls are roughly half; a 36-byte binary record
would cut that to a `memcpy` per row but save almost no bytes (134 MB:
decimal integers are short), and an Arrow IPC stream would additionally
let colstore take columns without a transpose and ingest as windows
close. None of that matters while the stages run one after another on
one machine and the oracle, `compare.sh` and the e2e's awk step all read
text; the first live-ingest requirement or the first column added on one
side only would change the answer.

### What `bench/e2e.sh` proves, and how

Default 5M events at `gen --rate 1000` (83 one-minute windows, so the
"chosen hour" [2026-01-01T00:00, 01:00) selects 60 of them and the zone
map on `window_start_us` skips 13 of the 58 aggregate row groups; at the
generator's default rate the whole file would fit in one hour and every
row would pass). Steps: gen, oracle, streamkit at 4 threads with
checkpoints every 1M events (5 epochs committed), `compare.sh`, ingest
and verify both tables, Q1-Q3 with `--check`, the top-10-by-spend query
with `--check-csv`, and the same top 10 recomputed by awk from the
oracle's CSV (sorted by spend desc, campaign asc, the engine's tiebreak).
Every step records PASS/FAIL and the script exits 1 if any failed, which
a one-off run with a wrapper `oracle` that added 1 to one row's cost
confirmed (`compare` FAIL, the awk check FAIL, `E2E FAIL`, exit 1).

### Verification (all from this session, build trees `build-int*`, Apple M5 Pro, load 3 to 7)

- `cmake --build build-int --clean-first`: 1 warning in the whole tree, in
  GoogleTest's own `gtest-all.cc` (the known `-Wcharacter-conversion` one
  from the m2 entry), 0 in any project translation unit; the ASan+UBSan
  and TSan trees rebuilt with 0 warnings.
- `ctest -j 8`: 207/207 in Release, ASan+UBSan and TSan (128 colstore
  tests, 14 of them new: schema footer for both tables and bad schemas,
  `CsvRowSource` on `write_csv_row`'s own output and every malformed-row
  case, aggregate-table round trip, writer guards, generic-vs-typed
  writer byte equality, the e2e query with `--check-csv` on an in-test
  CSV, cross-table guards at every layer, scan on the aggregate table).
- `bench/e2e.sh build-int` (5M events, 4 threads): E2E PASS, exit 0.
  Report (wall seconds around each process):

  | step | result | s |
  |---|---|---:|
  | gen | 5,000,000 events, 49,791 dups, 246,957 late | 0.38 |
  | oracle | 3,735,840 windows | 1.67 |
  | streamkit | same counts, 0 late-dropped, 5 checkpoint epochs, 7.6M events/s | 0.82 |
  | compare.sh | 3,735,840 windows identical | 10.4 |
  | ingest events.ads | 260,000,008 -> 80,386,496 bytes (0.31x), 77 groups | 0.78 |
  | verify events | all 10 columns of 5,000,000 rows identical | 0.27 |
  | ingest aggregates.csv | 127,086,825 -> 16,587,534 bytes (0.13x), 58 groups | 0.47 |
  | verify aggregates | all 6 columns of 3,735,840 rows identical | 0.35 |
  | Q1 / Q2 / Q3 (--check) | engine 0.007 / 0.026 / 0.045 s; 1 / 100,000 / 100,000 groups identical | 0.17 / 0.82 / 1.14 |
  | Q4 top-10 spend in the hour (--check-csv) | engine 0.019 s; 13 of 58 groups skipped; 100,000 groups and 10 ordered rows identical | 0.53 |
  | awk from the oracle's CSV | top 10 identical (20307 first, spend 15,931,296) | 2.49 |

  The same script at 1M events: ASan+UBSan tree E2E PASS, TSan tree E2E
  PASS, 0 sanitizer reports in either. With a wrapper `oracle` that adds 1
  to one row's cost (a one-off, not kept): `compare` FAIL, `E2E FAIL`,
  exit 1.
- `bench/colstore_bench.sh build-int 10000000 /private/tmp/adstack-cs all`
  on the final binary (`/private/tmp/adstack-cs/int_bench_final.txt`):
  PLAIN ingest best 0.486 s, auto 1.242 s (medians in the m5 table 1.236
  s vs SQLite 15.229 s); sizes 520,074,277 / 160,201,178 bytes (3.25x);
  all four `verify --where` and all five `q --check` PASS with the counts
  of the 19th; Q1 / Q2 / Q3 medians 0.051 / 0.095 / 0.180 s at 1 thread
  and 0.008 / 0.040 / 0.065 s at 8; SQLite comparison 42.3x (Q2) and
  27.5x (Q3) single-threaded, Q1 0.05x, answers identical.

### What I would change next

1. *`--schema` for the CSV path.* `CsvRowSource` takes a `Schema`; the CLI
   only ever passes `aggregate_schema()`. A `--schema "name:type,..."`
   flag is twenty lines and makes any integer CSV a `.col` file.
2. *Ingest from a pipe.* Both sources accept `-`, so `streamkit --out - |
   colstore ingest --in-csv - --out agg.col` works today; the e2e does not
   use it because the CSV must also feed `compare.sh`. A `tee` would do.
3. *The aggregate table's own encodings.* `window_start_us` lands on
   RLE_DICT at 6,248x in the 200K smoke run (a handful of distinct windows
   in long runs) and `cost_micros` on DELTA_BP at 2.8x; nothing was tuned
   for it, and the per-chunk selector just worked. Worth a table in
   BENCHMARKS.md once the aggregate queries are part of the benchmark.
4. *Dedup-aware comparison of the two tables.* Q2 on the event table and
   Q4 on the aggregate table answer the same question except that the raw
   events still contain the 1% duplicates streamkit removed; an e2e step
   that computes both and diffs them after a `--where`-free dedup would
   tie the two engines' answers together directly instead of each to its
   own brute force.
5. *The 10 s `compare.sh`* is now the slowest step of a pipeline whose
   engines take 0.8 s each; it sorts 3.7M rows twice through `sort(1)`.
   The oracle could write in sorted order and a `cmp` would do.
