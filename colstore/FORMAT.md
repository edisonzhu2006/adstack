# .col file format, version 1

The on-disk format of colstore. `src/format.h` is the executable version of
this document; if they disagree, the header wins and this file has a bug.

All integers are little-endian. There is no alignment padding between
sections; every byte position is recoverable from the footer.

## Overall layout

```
offset 0        +--------------------------------------+
                | header: magic "COL1", uint32 version |  8 bytes
                +--------------------------------------+
                | row group 0                          |
                |   chunk: event_id       (DELTA_BP)   |
                |   chunk: event_time_us  (DELTA_BP)   |
                |   ... 10 chunks, one per column ...  |
                +--------------------------------------+
                | row group 1                          |
                +--------------------------------------+
                | ...                                  |
                +--------------------------------------+
                | footer (see below)                   |  variable
                +--------------------------------------+
                | trailer: uint64 footer_len,          |  12 bytes
                |          magic "COL1"                |
end of file     +--------------------------------------+
```

A reader opens the file by:

1. reading the 8-byte header and checking magic + version,
2. reading the last 12 bytes, checking the magic, taking `footer_len`,
3. reading `footer_len` bytes ending 12 bytes before EOF and decoding the
   footer,
4. from then on, seeking directly to any `(row group, column)` chunk using
   the offset and length recorded in the footer.

A file without a valid trailer (writer crashed, disk full) is rejected as a
whole. There is no partial recovery in v1; the ingest is re-run.

## Header (8 bytes)

| bytes | type   | value                          |
|-------|--------|--------------------------------|
| 0..3  | uint32 | `0x314C4F43` (`"COL1"` as LE)  |
| 4..7  | uint32 | version, currently `1`         |

## Row groups

Rows are split into row groups of at most `rows_per_group` rows (default
65,536, settable with `colstore ingest --row-group N`). Only the last group
may be shorter. A row group holds exactly 10 column chunks, in `ColumnId`
order, back to back. A file with zero rows has zero row groups.

## Columns

One column per field of `adstack::Event`, in declaration order.

| id | name             | physical type | notes                             |
|----|------------------|---------------|-----------------------------------|
| 0  | `event_id`       | u64           |                                   |
| 1  | `event_time_us`  | i64           | microseconds since epoch          |
| 2  | `ingest_time_us` | i64           |                                   |
| 3  | `event_type`     | u8            | 0 impression, 1 click, 2 conversion |
| 4  | `advertiser_id`  | u32           |                                   |
| 5  | `campaign_id`    | u32           |                                   |
| 6  | `video_id`       | u64           |                                   |
| 7  | `country`        | u16           | ISO 3166-1 numeric                |
| 8  | `device`         | u8            | 0 mobile, 1 desktop, 2 TV         |
| 9  | `cost_micros`    | i64           | may be negative (refunds)         |

## Column chunk

The encoded values of one column for one row group. The bytes are
interpreted according to the chunk's `encoding` id in the footer.

The writer picks the encoding per chunk (smallest encoded size wins, see
`src/encoding.h`), so one column can use different encodings in different
row groups. Every encoding except PLAIN starts with `uint32 n`, the value
count, which a reader checks against the footer's `rows`. Bit-packed
sections use one LSB-first bit stream: bit k of the stream is bit `k % 8`
of byte `k / 8`; value i occupies bits `[i*width, (i+1)*width)`; the
section is padded to a whole byte. "width" below is bits per value.

### Encoding 0: PLAIN

`rows * width` bytes: each value at its natural width, little-endian, in
row order. Decoding is a single `memcpy`. The baseline every other
encoding is measured against, and the fallback when nothing beats it.

### Encoding 1: DICT

For low-cardinality columns (`country`, `device`).

```
uint32 n
uint32 d                      dictionary size
d values at natural width     distinct values, ascending in the column's
                              natural order (signed for i64)
uint8  code_width             bits needed for d-1; 0 when d <= 1
ceil(n * code_width / 8)      bit-packed codes, code i indexes the dictionary
```

Size: `9 + d*w + ceil(n*code_width/8)`. Sorted dictionary means code order
equals value order, so a range predicate on the column is a range of codes.

### Encoding 2: DELTA_BP

For near-monotone integers (`event_id`, `event_time_us`, `ingest_time_us`)
and, by default, any high-cardinality integer column. Values are split
into blocks of 128 (the last block may be shorter); block j holds values
`[128j, min(128j + 128, n))`:

```
uint32 n
repeat ceil(n / 128) times:
    uint64 first              raw bits of the block's first value
    uint8  width              bits per packed delta, 0..64
    uint64 base               smallest delta in the block (raw bits)
    ceil((m-1) * width / 8)   packed (delta - base) for the other m-1 values
```

`delta[i] = value[i] - value[i-1]` in wrapping 64-bit arithmetic, and the
decoder adds in the same arithmetic, so the round trip is exact across
INT64_MIN/INT64_MAX and for unsigned columns. `base` is the signed minimum
of the block's deltas (frame of reference), so the packed numbers are
`0 .. (max_delta - min_delta)` and `width` is the bit width of that range.
Each block is decodable on its own given its 17-byte header.

### Encoding 3: RLE_DICT

Dictionary plus run-length coding of the codes, for columns with long runs
of equal values (`event_type`, where 90% of rows are impressions).

```
uint32 n
uint32 d                      dictionary size
d values at natural width     as in DICT
uint32 r                      number of runs
uint8  code_width             as in DICT
uint8  len_width              bits needed for the longest run
ceil(r * code_width / 8)      packed run codes
ceil(r * len_width / 8)       packed run lengths (each >= 1; they sum to n)
```

Two parallel packed arrays rather than interleaved pairs, so codes and
lengths each get their own width.

### Reader validation of chunks

Decoders read through a bounds-checked cursor and reject a chunk if: the
chunk's `n` differs from the footer's `rows`; any header is truncated; a
`code_width` disagrees with `d`; a code is `>= d`; a run length is 0 or
runs do not sum to exactly `n`; a bit width is above 64; a packed section
is shorter than `n * width` bits; or bytes remain after the last section.
Values decoded for a narrow column that do not fit its width are rejected
too. A corrupt chunk therefore throws; it never reads outside the chunk.

## Footer

```
uint32 num_row_groups
uint32 num_columns          (always 10 in v1; checked on read)
uint32 rows_per_group       (writer setting)
uint32 reserved             (0)
uint64 total_rows
repeat num_row_groups times:
    uint32 rows             (rows in this group)
    repeat 10 times (ColumnId order):
        ChunkMeta           (48 bytes, below)
```

Footer size = 24 + num_row_groups * (4 + 10 * 48) bytes. For 10M rows at
65,536 rows per group that is 153 groups and 74,076 bytes, 0.014% of the
file.

### ChunkMeta (48 bytes)

| bytes  | type   | field        | meaning                                           |
|--------|--------|--------------|---------------------------------------------------|
| 0..7   | uint64 | `offset`     | absolute byte offset of the chunk in the file      |
| 8..15  | uint64 | `length`     | encoded byte length                                |
| 16..19 | uint32 | `rows`       | values in the chunk (equals the group's `rows`)    |
| 20..23 | uint32 | `encoding`   | 0 PLAIN, 1 DICT, 2 DELTA_BP, 3 RLE_DICT             |
| 24..31 | uint64 | `min_bits`   | minimum value, raw 8 bytes                         |
| 32..39 | uint64 | `max_bits`   | maximum value, raw 8 bytes                         |
| 40..43 | uint32 | `null_count` | reserved, always 0 (Event has no nullable fields)  |
| 44..47 | uint32 | `reserved`   | 0; keeps the record 8-byte aligned                 |

`min_bits` / `max_bits` hold the value's two's-complement bits widened to
64 bits: sign-extended for `i64` columns, zero-extended for unsigned ones.
A reader uses the column's physical type to decide how to compare them. For
an empty chunk both are 0. These are the zone maps milestone 3 uses to skip
row groups. They are computed for every integer column regardless of the
chunk's encoding.

## Trailer (12 bytes)

| bytes | type   | value                      |
|-------|--------|----------------------------|
| 0..7  | uint64 | `footer_len` in bytes      |
| 8..11 | uint32 | `0x314C4F43` (`"COL1"`)    |

## Reader validation

On open, the reader rejects the file if any of these fail: file shorter than
20 bytes, header magic or version wrong, trailer magic wrong, footer length
larger than the space between header and trailer, footer length inconsistent
with `num_row_groups`, `num_columns != 10`, any chunk `rows` differing from
its group's `rows`, sum of group rows differing from `total_rows`, or any
chunk lying outside `[8, footer_offset)`.

## Versioning

Any change to the layout above bumps `kColVersion`; readers refuse files
with a version they do not know. Adding an encoding id does not bump the
version.
