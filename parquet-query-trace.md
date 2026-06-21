# How a Parquet query gets resolved: a top-down trace

Notes from working through *Designing Data-Intensive Applications*, Chapter 4 —
"Column-Oriented Storage" — applied to a real Parquet file structure.

## How Parquet data is structured

A Parquet **dataset** is organized as a nested hierarchy. Each level below is
where a different kind of pruning happens during a query.

```
Dataset
└── Partition directories (optional — e.g. product_category=Fresh fruit/)
    └── Files (one or more per partition, count set at write time)
        └── Row Groups (a horizontal slice of rows, e.g. 1M rows)
            └── Column Chunks (one per column, stored separately)
                └── Pages (smaller units within a chunk, ~1MB — encoding/compression applied here)
```

- **Partitions** are just directories on disk/object storage, derived from
  column values (Hive-style `col=value/` paths). Not part of the Parquet file
  format itself — a filesystem/object-storage convention.
- **Files** are independent Parquet objects, each with their own footer.
- **Row groups** live *inside* a file and contain **every column** for that
  slice of rows — not a subset.
- **Column chunks** hold one column's values for that row group, stored
  contiguously, with their own encoding, compression, and min/max stats.
- **Pages** are the smallest unit inside a column chunk — typically ~1MB —
  and are where compression is physically applied, and where decoding
  happens incrementally rather than all at once for the whole chunk.

Every file ends with a **footer**: metadata describing all its row groups and
column chunks (byte offsets, stats, schema), consulted *before* any data
bytes are fetched.

### Write-time encoding pipeline (the reverse of how reads decode)

```
raw column values (within a row group)
        ↓
   ENCODING (dictionary / RLE / delta) — operates on the column chunk's data
        ↓
   split into PAGES (~1MB units)
        ↓
   COMPRESSION (snappy/zstd) — applied per page
```

Encoding happens first, over the column chunk as a whole — this is where the
big wins come from (e.g. RLE collapsing long runs of a sorted column).
Compression is applied per page, on top of already-encoded data, squeezing
out whatever redundancy is left. A larger row group means encoding has more
homogeneous, sorted data to exploit before it ever gets sliced into pages —
that's why row group size still affects compression quality even though
compression itself happens per page.

---

## The query

```sql
SELECT product_sk, SUM(quantity)
FROM fact_sales
WHERE date_key BETWEEN '2024-06-01' AND '2024-06-07'
GROUP BY product_sk;
```

Assumptions: `fact_sales` is stored as Parquet, physically sorted by
`date_key`, with columns `date_key`, `product_sk`, `quantity`, and ~97
others.

### Level 1 — Dataset / directory level

If the table is partitioned (e.g. by month), the engine looks at directory
names — `date_key_month=2024-06/` — and skips every other month's directory
entirely. No file is even listed for the wrong partitions. Coarsest possible
pruning, done before anything is opened.

### Level 2 — File count within the surviving partition (write-time, not read-time)

However many files exist in the surviving partition was decided at **write**
time — by however many parallel tasks wrote to it (Spark's shuffle
partitions, `repartition()` / `coalesce()`, or auto-sizing by some writers).
This isn't a pruning decision; it's inherited overhead for the next step.
More files here means more footers to open, regardless of how well-pruned
the data turns out to be.

### Level 3 — File level (footer read)

For each file, the engine reads the trailing bytes to learn the footer's
length and location, then reads the footer itself — a small, targeted read,
not a full file scan.

### Level 4 — Row group level

The footer lists every row group's column chunk stats (min/max, null
counts). The engine checks whether each row group's `date_key` range
overlaps `[2024-06-01, 2024-06-07]`. Because the data is sorted by
`date_key`, most row groups have tight, non-overlapping ranges, so the
majority get skipped here. Only overlapping row groups survive.

> **Note on categorical filters:** if the `WHERE` clause instead filtered on
> a low-cardinality column (e.g. `product_category`), min/max stats are a
> weak signal — alphabetical min/max doesn't reveal which values are
> present/absent. The much stronger mechanism for categoricals is
> **partitioning by that column** (Level 1 resolves it via directory
> skipping), not row-group stats. If the categorical column isn't the
> partition key and the data is sorted by something else (like date),
> row-group pruning does almost nothing for it.

### Level 5 — Column chunk level

The query only needs `date_key`, `product_sk`, and `quantity` — not the
other ~97 columns. The engine seeks directly to just those 3 column chunks'
byte offsets (from the footer), ignoring the rest. Pure column pruning, no
stats involved — automatic, just from selecting fewer columns.

### Level 6 — Page level (within each surviving column chunk)

Within a column chunk, data is split into pages (~1MB each), and Parquet
stores **per-page min/max stats** too, not just per-chunk. So if a row group
only partially overlaps the date range, the engine can skip individual pages
within the `date_key` chunk whose values fall entirely outside the range — a
finer-grained version of Level 4. Pages are also the unit that gets
individually decompressed/decoded.

### Level 7 — Byte-range fetch

For each needed page (not the whole column chunk, if page-level pruning
applied), the engine fetches just those bytes — a disk seek locally, or a
**ranged HTTP GET** on object storage (S3/GCS/etc., via the standard HTTP
`Range` header), often coalescing nearby ranges into fewer requests to cut
round-trip overhead.

### Level 8 — Decode: decompression + encoding

Two distinct steps, in this order:

1. **Decompress** each fetched page (snappy/zstd) — gets back the *encoded*
   bytes, not raw values yet.
2. **Decode** those bytes per the encoding used (expand dictionary indices,
   expand RLE runs, reverse delta encoding) — this produces the final raw
   values.

### Level 9 — Row reconstruction + residual filtering

All needed columns share the same row order within a row group, so the
engine reconstructs logical rows (*k*th value from each column chunk = one
row) and applies any leftover filtering — e.g. trimming rows just outside
the date range in row groups that only partially overlapped.

### Level 10 — Query execution: aggregation

Only now does `GROUP BY product_sk` / `SUM(quantity)` actually run — on a
small, fully-pruned, decoded set of values. This is the query execution
layer itself (operators, possibly vectorized).

---

## The funnel, visually

```
All partitions            →  prune by directory                          (1)
  ↓
Files in partition         →  count set at write time, not pruning        (2)
  ↓
All files                   →  read footer only                            (3)
  ↓
All row groups                →  prune by min/max stats (chunk-level)         (4)
  ↓
All columns                     →  prune by SELECT list                        (5)
  ↓
Pages in surviving chunks         →  prune by per-page min/max stats             (6)
  ↓
Disk/object store                  →  fetch only needed page byte ranges          (7)
  ↓
Compressed pages                     →  decompress (per page) → decode             (8)
  ↓
Column chunks                          →  reconstruct rows, residual filter          (9)
  ↓
Filtered rows                            →  aggregate (GROUP BY / SUM)                 (10)
```

Each level only exists because the level above it couldn't fully eliminate
the data — right down to individual ~1MB pages.

---

## Row store vs. column store: how range filtering differs

| | Row store | Column store (Parquet) |
|---|---|---|
| Skip irrelevant rows (e.g. date range) | Needs an explicit **B-tree index** — a separate structure maintained on every write | Min/max stats per row group / page — nearly free, baked into the file itself, but only useful if data is physically sorted/clustered by that column |
| Skip irrelevant columns | Can't — full row is the atomic unit; loads all columns even if 2 are needed | Free — just don't read those column chunks |
| Cost of the skip mechanism | Extra index = extra disk space + write-time maintenance | Stats are nearly free; clustering (e.g. sort order, `ZORDER`-style techniques) costs a rewrite job |

Even with an index narrowing a row store to the right pages, each page still
holds **full rows** — so the index helps skip rows, but row orientation
still forces loading all columns of the rows you do touch. That's the
specific cost columnar storage removes.

## Categorical (low-cardinality) columns

Min/max stats are a weak pruning signal for category labels — alphabetical
min/max doesn't tell you which values are present or absent. The book's
answer is **bitmap encoding**: one bitmap per distinct value, with bitwise
AND/OR combining filters cheaply across columns (since all columns share row
order). Parquet doesn't natively expose this kind of bitmap-index pruning —
that's the territory of systems built explicitly for it (Druid, Pinot,
Vertica). In Parquet/data-lake practice, the practical lever for a
categorical filter column is **partitioning by it**, not relying on
row-group stats.

---

*Based on Designing Data-Intensive Applications, 3rd ed., Chapter 4 —
"Column-Oriented Storage" (compression, sort order, writes), extended with
Parquet's concrete file structure (row groups, column chunks, pages,
footer) and object-storage ranged reads.*
