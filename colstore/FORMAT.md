# .col file format, version 3

The on-disk format of colstore. `src/format.h` is the executable version of
this document; if they disagree, the header wins and this file has a bug.

All integers are little-endian. There is no alignment padding between
sections; every byte position is recoverable from the footer.

Version 2 (2026-10-21) differs from version 1 in one way: the footer
carries the table's SCHEMA (column names and physical types), where
version 1 hardcoded the ten `adstack::Event` fields. A `.col` file is now
any table of integer columns; the two tables colstore writes are the event
table and streamkit's aggregate table (both below).

Version 3 (2026-10-24) adds checksums and changes no offset: every
ChunkMeta's last field, a reserved zero in version 2, is now the CRC-32C
of that chunk's bytes, and the footer's fourth field, also a reserved zero,
is now the CRC-32C of the footer itself. Together with the header and
trailer checks, every byte of the file is covered, so a single flipped bit
anywhere is reported as corruption instead of being read as a different
value (version 2 could not tell; NOTES.md, "Review fixes", has the case).
Older files are refused by the reader (`unsupported .col version N`) and
must be re-ingested; nothing persistent depended on them.

CRC-32C here is the Castagnoli CRC with the usual conventions (reflected
polynomial `0x82F63B78`, initial value and final xor `0xFFFFFFFF`; the
CRC of the ASCII bytes `123456789` is `0xE3069283`), as in iSCSI and ext4.

## Overall layout

```
offset 0        +--------------------------------------+
                | header: magic "COL1", uint32 version |  8 bytes
                +--------------------------------------+
                | row group 0                          |
                |   chunk: column 0       (DELTA_BP)   |
                |   chunk: column 1       (DELTA_BP)   |
                |   ... one chunk per schema column ...|
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
whole. There is no partial recovery; the ingest is re-run.

## Header (8 bytes)

| bytes | type   | value                          |
|-------|--------|--------------------------------|
| 0..3  | uint32 | `0x314C4F43` (`"COL1"` as LE)  |
| 4..7  | uint32 | version, currently `3`         |

## Row groups

Rows are split into row groups of at most `rows_per_group` rows (default
65,536, settable with `colstore ingest --row-group N`). Only the last group
may be shorter. A row group holds exactly one chunk per schema column, in
schema order, back to back. A file with zero rows has zero row groups.

## Schema

The footer lists the table's columns: a name and a physical type each.
Column `i` of the schema is chunk `i` of every row group. Names are
identifiers (`[A-Za-z_][A-Za-z0-9_]*`, at most 255 bytes, unique within
the file), because they are what `--where`, `--group` and `--agg` name.
Physical types are the five integer widths below; there is no other kind
of column, which is what keeps every codec one implementation.

| type id | name | bytes | signed |
|---|---|---|---|
| 0 | u8  | 1 | no  |
| 1 | u16 | 2 | no  |
| 2 | u32 | 4 | no  |
| 3 | u64 | 8 | no  |
| 4 | i64 | 8 | yes |

A reader learns everything it needs from the footer; the two tables below
are simply the schemas colstore's own ingest paths write (`event_schema()`
and `aggregate_schema()` in `src/format.h`), and `colstore info` names a
file "events", "aggregates" or "custom" by comparing against them.

### The event table (`colstore ingest --in events.ads`)

One column per field of `adstack::Event`, in declaration order.

| index | name             | physical type | notes                             |
|-------|------------------|---------------|-----------------------------------|
| 0     | `event_id`       | u64           |                                   |
| 1     | `event_time_us`  | i64           | microseconds since epoch          |
| 2     | `ingest_time_us` | i64           |                                   |
| 3     | `event_type`     | u8            | 0 impression, 1 click, 2 conversion |
| 4     | `advertiser_id`  | u32           |                                   |
| 5     | `campaign_id`    | u32           |                                   |
| 6     | `video_id`       | u64           |                                   |
| 7     | `country`        | u16           | ISO 3166-1 numeric                |
| 8     | `device`         | u8            | 0 mobile, 1 desktop, 2 TV         |
| 9     | `cost_micros`    | i64           | may be negative (refunds)         |

### The aggregate table (`colstore ingest --in-csv aggregates.csv`)

One column per field of streamkit's output row (`common/agg.h`:
`WindowKey` then `Agg`), in the CSV's header order. The CSV header must be
exactly `window_start_us,campaign_id,impressions,clicks,conversions,cost_micros`.

| index | name              | physical type | notes                              |
|-------|-------------------|---------------|------------------------------------|
| 0     | `window_start_us` | i64           | start of the window, microseconds  |
| 1     | `campaign_id`     | u32           |                                    |
| 2     | `impressions`     | u64           |                                    |
| 3     | `clicks`          | u64           |                                    |
| 4     | `conversions`     | u64           |                                    |
| 5     | `cost_micros`     | i64           | summed over the window's events    |

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
row order. Decoding into a typed array (`ColReader::read_column<T>`) is a
single `memcpy`; decoding into the 64-bit view the query engine uses is one
load-and-widen loop. The baseline every other encoding is measured
against, and the fallback when nothing beats it.

### Encoding 1: DICT

For low-cardinality columns (`country`, `device`).

```
uint32 n
uint32 d                      dictionary size
d values at natural width     distinct values, strictly ascending in the
                              column's natural order (signed for i64);
                              a reader rejects a dictionary that is not
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

Before any decoder sees a chunk, the reader checks its CRC-32C against
the footer's `crc32c`; a mismatch throws, naming the row group and column.
The decoders then check structure, through a bounds-checked cursor, and
reject a chunk if: the chunk's `n` differs from the footer's `rows`; any
header is truncated; a `code_width` disagrees with `d`; a dictionary is
not strictly ascending; a code is `>= d`; a run length is 0 or runs do not
sum to exactly `n`; a bit width is above 64; a packed section is shorter
than `n * width` bits; bytes remain after the last section; or a DELTA_BP
value decoded for a u8/u16/u32 column does not fit its width. A decoder
never reads outside the chunk. The checksum is what catches damage that
leaves the structure valid (one flipped bit in a value); the structural
checks are what catch a buggy writer, whose checksum would be "correct".

## Footer

```
uint32 num_row_groups
uint32 num_columns          (1 .. 4096)
uint32 rows_per_group       (writer setting)
uint32 footer_crc32c        (CRC-32C of the whole footer with these 4 bytes read as 0)
uint64 total_rows
repeat num_columns times (the schema, in chunk order):
    uint32 type             (type id, table above)
    uint32 name_len         (1 .. 255)
    name_len bytes          (the name, no terminator)
repeat num_row_groups times:
    uint32 rows             (rows in this group)
    repeat num_columns times (schema order):
        ChunkMeta           (48 bytes, below)
```

Footer size = 24 + sum over columns of (8 + name length) + num_row_groups *
(4 + num_columns * 48) bytes. For the event table (schema 181 bytes) with
10M rows at 65,536 rows per group that is 153 groups and 74,257 bytes,
0.014% of the file; for the aggregate table (schema 113 bytes, 292 bytes
per group) it is smaller still.

### ChunkMeta (48 bytes)

| bytes  | type   | field        | meaning                                           |
|--------|--------|--------------|---------------------------------------------------|
| 0..7   | uint64 | `offset`     | absolute byte offset of the chunk in the file      |
| 8..15  | uint64 | `length`     | encoded byte length                                |
| 16..19 | uint32 | `rows`       | values in the chunk (equals the group's `rows`)    |
| 20..23 | uint32 | `encoding`   | 0 PLAIN, 1 DICT, 2 DELTA_BP, 3 RLE_DICT             |
| 24..31 | uint64 | `min_bits`   | minimum value, raw 8 bytes                         |
| 32..39 | uint64 | `max_bits`   | maximum value, raw 8 bytes                         |
| 40..43 | uint32 | `null_count` | reserved, always 0 (no table here has a nullable field) |
| 44..47 | uint32 | `crc32c`     | CRC-32C of the chunk's `length` bytes (version 3)  |

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
larger than the space between header and trailer, the footer's CRC-32C
(checked before any other footer field is trusted), `num_columns` zero or
above 4096, a schema entry truncated, a name that is not an identifier or
not unique, a type id above 4, footer length inconsistent with
`num_row_groups` and `num_columns`, any chunk `rows` differing from its
group's `rows`, sum of group rows differing from `total_rows`, or any
chunk lying outside `[8, footer_offset)`.

## Versioning

Any change to the layout above bumps `kColVersion`; readers refuse files
with a version they do not know. Adding an encoding id does not bump the
version. Adding a column to a table does not either: it is a different
schema in the same format.
