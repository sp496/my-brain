# Avro — Notes for a Databricks Data Engineer

_Based on Chapter 5: Encoding and Evolution — Designing Data-Intensive Applications_

---

## What is Avro and why does it exist?

Avro is a binary encoding format with a schema. It was created in 2009 as part of the Hadoop ecosystem specifically because Protobuf wasn't a good fit for Hadoop's use cases — particularly around dynamically generated schemas.

Its core job: define a **contract between two teams** (e.g. an app team publishing to Kafka and a data engineering team reading from it) so that data can flow between them reliably, even as the schema evolves over time.

---

## The core idea: writer's schema vs reader's schema

When data is written, the **writer's schema** is used — this is the schema the producer had at the time of writing.

When data is read, the **reader's schema** is used — this is the schema your Spark job or consumer has right now.

These two schemas do not need to be identical. Avro resolves the differences between them at read time by **matching fields by name**.

> In Databricks terms: the app team owns the writer's schema. Your Spark job using `from_avro()` is the reader. They can evolve independently as long as the compatibility rules are followed.

---

## Schema resolution rules

| Situation | What happens |
|---|---|
| Field in writer, not in reader | Ignored by the reader |
| Field in reader, not in writer | Filled with the field's **default value** |
| Field in both | Matched by **name** and decoded |
| Field order differs | Doesn't matter — name matching, not position |

---

## Schema evolution rules — what's safe and what isn't

| Change | Safe? | Why |
|---|---|---|
| Add field **with** a default | ✅ Both directions | Old readers fill the default; new readers read the value |
| Remove field **with** a default | ✅ Both directions | Old readers still have it defined; new readers fill default |
| Add field **without** a default | ⚠️ Backward only | New readers handle old data fine, but old readers break on new data |
| Remove field **without** a default | ⚠️ Forward only | Old readers expect it and have no fallback |
| Rename a field | ❌ Breaks forward | Name matching fails — old readers can't find the renamed field |
| Change to a compatible type | ⚠️ Depends | e.g. int → long is safe; others may truncate or fail |

**Practical habit:** Before finalising any schema, think about how it will need to change over the next 6–12 months. Add defaults and declare nullability upfront. A field added with a default today costs nothing. A field without a default, once 50 consumers are reading it, is a coordination problem.

---

## Nullability — the most common mistake

In Avro, fields are **not nullable by default**. To make a field nullable you must explicitly declare a union type:

```
union { null, string }
```

And `null` must be listed **first** in the union if you want null to be the default value.

This forces you to be intentional about nullability — which prevents silent bugs downstream. In Delta Lake terms, this maps to whether a column is nullable or not in your table schema.

---

## How does the reader know which writer's schema was used?

This depends on the context:

**Large files (Avro object container files)**
The writer's schema is embedded once at the top of the file. All records in the file use that schema. This is the format used for data lake files, HDFS dumps, and database snapshots.

**Kafka + Schema Registry (most common in Databricks)**
Each Kafka message carries a **schema ID** (4 bytes prepended by Confluent's serializer). The reader extracts the schema ID, looks up the corresponding schema in the Schema Registry, then decodes the rest of the message. This is why the Schema Registry is central — it's the database of all historical schema versions.

---

## Where Avro appears in the Databricks stack

```
Kafka message (Avro bytes)
        ↓
   from_avro()       ← Avro rules apply here (writer/reader schema resolution)
        ↓
   Spark DataFrame   ← now just Spark types, Avro is done
        ↓
  .write.delta       ← Delta rules apply here (mergeSchema, type checking)
        ↓
   Bronze Delta table (stored as Parquet)
```

**Key point:** Avro and Delta Lake are two separate schema evolution systems. You need to reason about both independently.

- `from_avro()` handles schema differences between Kafka producer and your Spark job
- `mergeSchema` (or `ALTER TABLE`) handles schema differences at the Delta layer

A common mistake: assuming `mergeSchema` protects you from Avro schema changes upstream. It doesn't — Avro resolution happens before Delta sees the data.

---

## Avro vs Parquet — when to use which

| | Avro | Parquet |
|---|---|---|
| Format type | Row-oriented | Column-oriented |
| Best for | Streaming, wire transport, large immutable files | Analytics, data warehouses, lake storage |
| Used in | Kafka messages, Avro object container files | Delta Lake, Spark analytics, Snowflake, BigQuery |
| Schema | Embedded in file or via Schema Registry | Embedded in file footer |
| When to use | Ingestion layer (Kafka → Bronze) | Storage layer (Bronze onwards) |

In a typical Databricks pipeline: **Avro is the wire format, Parquet is the storage format.** The handoff happens when you write from `from_avro()` into a Delta table.

---

## Avro vs Protobuf — why Avro wins for data engineering

Avro uses field **names** for matching. Protobuf uses **tag numbers**.

This makes Avro friendlier to dynamically generated schemas. If you have a Postgres table and want to export it as Avro, you can auto-generate the schema from the table definition — column names map directly to Avro field names. If the table schema changes, just regenerate the Avro schema and the name-based matching still works.

With Protobuf, tag numbers must be assigned manually and can never be reused. Every table schema change requires careful human (or very careful automated) management of those numbers.

---

## Forward vs backward compatibility — the mental model

- **Who changes first?**
  - Producer changes first → you need **forward compatibility** (old readers handle new data)
  - Consumer changes first → you need **backward compatibility** (new readers handle old data)
  - In Kafka pipelines, **producers almost always change first** — so forward compatibility is what you're typically designing for

- **"New code reads old data"** = new schema reading data encoded with the old schema
- **"Old code reads new data"** = old schema reading data encoded with the new schema

---

## The unknown field problem — silent data loss

If a consumer reads a message with an unknown field, partially understands it, then re-serialises and republishes it downstream — **the unknown field is gone silently.**

Avro handles this correctly by preserving unknown fields during resolution. But only if your consumer passes data through rather than re-serialising from a stripped model object. Be careful when building multi-step pipelines where a Spark job both reads and republishes Avro messages.

---

## Modes of dataflow — where schema compatibility matters

The book identifies two modes most relevant to data engineers:

**Through databases**
Data written by an old schema version sits alongside data written by a new schema version. Old rows don't automatically update. Delta Lake handles this by filling nulls for columns that didn't exist when old rows were written — same concept as Avro's default value fill-in.

**Through message brokers (Kafka)**
Producer and consumer are fully decoupled. A message might sit in Kafka for days before being consumed. You cannot assume reader and writer are in sync. This is exactly why Avro + Schema Registry is the standard pattern for Kafka-based pipelines.

---

## Summary

Avro's value isn't the byte layout — it's the **discipline it forces around schema design before data starts flowing.** Understanding the writer/reader schema split, the resolution rules, and the compatibility guarantees gives you the mental model to:

- Design schemas that won't break when upstream teams add fields
- Know which Delta Lake options (`mergeSchema`, `ALTER TABLE`) to reach for when schemas evolve
- Understand why a Schema Registry exists and what it's actually doing
- Avoid silent data loss in multi-step Kafka pipelines
