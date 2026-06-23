# Cloud Data Warehouses

Notes from *Designing Data-Intensive Applications*, Chapter 4 — extended with
the open-source ecosystem context.

---

## The core idea: separate storage from compute

Traditional data warehouses (Teradata, Vertica, on-premises SAP HANA) coupled
storage and compute tightly — data lived on disks attached to the same machines
that ran queries. Scaling one meant scaling the other, which was wasteful and
inflexible.

Cloud data warehouses (BigQuery, Redshift, Snowflake) broke this coupling:

- **Data** lives in object storage (S3, GCS, Azure Blob) — durable, cheap,
  infinitely scalable
- **Compute** is a separate, independently scalable resource spun up only when
  queries run

This is why Parquet's ranged-GET design matters: data permanently lives in
object storage, and compute nodes are ephemeral workers that read from it via
ranged HTTP GETs. Cloud data warehouses are architected around this assumption.

---

## What decoupling enables

**Elastic compute** — spin up 1000 compute nodes for a heavy query, tear them
down immediately after. Pay only for the time they run. This is why BigQuery
bills per TB scanned rather than per machine-hour.

**Decoupled scaling** — storage grows with data volume; compute scales with
query load independently. Idle weekends don't waste compute sitting next to
petabytes of data.

**Multiple compute clusters, one copy of data** — Snowflake's multi-cluster
model lets different teams (data science, BI, engineering) run independent
compute clusters against the same underlying Parquet files in S3, without
contending with each other.

---

## The open-source stack: from monolith to layers

Traditional open-source warehouses like Apache Hive bundled everything together.
As data moved to object storage (data lakes on S3), this monolith broke apart
into four distinct, independently-swappable layers:

```
SQL query
    ↓
Query engine         →  parse SQL, optimize plan, decide which files/row groups to skip
    ↓
Execution framework  →  distribute tasks across cluster, shuffle, fault tolerance
    ↓
Table format         →  which files currently constitute the table (transactions, time travel)
    ↓
Storage format       →  how bytes are physically laid out within each file
```

### Query engine
Parses SQL, optimizes it into an execution plan, decides which files/row groups
to skip (the pruning logic from your top-down trace). Examples: Trino, Presto,
Apache DataFusion, DuckDB.

### Execution framework
Takes the plan and runs it across many machines — parallel tasks, shuffling data
between nodes for GROUP BY/JOIN, fault tolerance if a worker dies. Examples:
Apache Spark, Apache Flink.

### Table format
Manages which Parquet files currently constitute a table — transactions,
insert/update/delete via immutable file swaps, time travel, schema evolution.
Examples: Delta Lake, Apache Iceberg, Apache Hudi.

### Storage format
How data is physically encoded and compressed within a file — columnar layout,
row groups, column chunks, pages, dictionary/RLE/delta encoding. Examples:
Parquet, ORC, Apache Arrow.

These layers are now mix-and-match: Trino can query Delta tables stored as
Parquet on S3; Spark can write Iceberg tables read by DataFusion. Some systems
(Spark, Trino) vertically integrate query engine + execution framework but keep
the lower two layers pluggable.

---

## Trade-offs the book flags

**Row-by-row computation is less efficient** with columnar storage — column
stores are optimized for scanning many rows of few columns, not touching one
row at a time. Alternative APIs or batch frameworks are preferable for this.

**Cost** — cloud warehouses can be expensive for large jobs. Running Spark
directly against S3 is sometimes more cost-efficient.

**Network latency** — separating storage and compute means every query involves
network I/O (ranged GETs), adding latency vs. data on local NVMe disks. Caching
layers (Snowflake's local SSD cache, Alluxio in front of S3) absorb repeated
reads without going back to object storage every time.

---

## One-line summary

Cloud data warehouses are: **columnar Parquet files on object storage + a table
format managing which files are valid + an elastic compute layer that queries
them via ranged GETs** — the same three layers covered in the Parquet query
trace, assembled into a product someone else operates for you.

---

*Based on Designing Data-Intensive Applications, 3rd ed., Chapter 4 —
"Cloud Data Warehouses" and "Data Storage for Analytics."*
