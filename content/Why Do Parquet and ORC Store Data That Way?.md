---
title: Why Do Parquet and ORC Store Data That Way?
publish: true
draft: false
enableToc: true
tags:
  - internals
  - curious-questions
cssclasses:
  - page-rainbow
---

Ever Wondered Why Open Storage Formats Like Parquet and ORC Store Data the Way They Do?

In open storage formats like Parquet and ORC, data is organized using a hybrid layout:
- Data is horizontally partitioned into Row Groups (in Parquet)/Stripes (in ORC).
- Within each Row Group, attributes are vertically partitioned and stored as column chunks.

*This Hybrid Storage Format (Row Groups with Column Chunks) is called **PAX (Partition Attributes Across)**.*

But why do we need a hybrid storage format? Isn't it better to store everything in a Columnar Format, which provides much better performance, compression, and I/O Performance? Why partition it into a row group first?

To understand the reasons, we need to understand the benefits and drawbacks of:
- Row Storage Model (NSM), and 
- Columnar Storage Model (DSM)

## N-Ary Storage Model (NSM): Row Storage Model
- Ideal for OLTP workloads.
- Stores data in memory contiguously next to each other for all attributes.

### Benefits
- **Spatial Locality:** All attribute/tuple values are stored contiguously in a single memory page, and don't require additional sub-joins to make an entire row. 
  In short, the data for all the attributes are physically close to each other.
### Drawbacks
- Inefficient for analytics, fetching even a few attributes requires getting the entire row.

![[NSM-Storage-Format.png]]

## Decomposition Storage Model (DSM): Columnar Storage Model
- Ideal for OLAP workloads where read-only queries perform large scans over a subset of the table’s attributes.
- Maintains a separate file per attribute with metadata about the entire column.

### Benefits
- The same data type values stored contiguously in memory result in Vectorized Processing, better compression, and I/O Performance.

### Drawbacks
- To fetch multiple attributes, multiple files need to be read.
- **Lack of Spatial Locality**: To reconstruct a record returned by a query, all attributes must be stitched together via ***sub-joins***.

![[Pasted image 20251025120401.png]]


In real OLAP workloads, queries involve multiple attributes, making a columnar storage layout inefficient. As more attributes are fetched, the cost of stitching them into a record, requiring sub-joins, rises.

## Why PAX?

PAX brings the best of both storage formats: 
- Columnar Storage's performance for efficient processing and compression.
- Row Storage's locality to reduce record reconstruction overhead.

The hybrid columnar layout (PAX) enables the query engines to use vectorized query processing and mitigates the record reconstruction overhead in a row group.

![[Pasted image 20251025120440.png]]

Modern data systems like Snowflake, DuckDB, and Apache Arrow leverage PAX for efficient storage and query performance.

## Reference Papers
1. [Data page layouts for relational databases on deep memory hierarchies](https://dl.acm.org/doi/pdf/10.1007/s00778-002-0074-9)
2. [A Deep Dive into Common Open Formats for Analytical DBMSs](https://www.vldb.org/pvldb/vol16/p3044-liu.pdf)