---
title: Iceberg - Data and Delete File Relationship
publish: true
draft: false
tags:
- Iceberg
- internals
cssclasses:
- page-green
---

SQL queries that demonstrate the sequence number relationship between data files and delete files using Apache Iceberg's metadata tables:

## **Query 1: Basic Sequence Number Relationship**

```sql
-- Show sequence numbers for data and delete files in the current snapshot

SELECT
content,
CASE
	WHEN content = 0 THEN 'DATA'
	WHEN content = 1 THEN 'POSITION_DELETE'
	WHEN content = 2 THEN 'EQUALITY_DELETE'
END as file_type,
file_path,
sequence_number,
file_sequence_number,
record_count,
file_size_in_bytes
FROM your_table.entries
WHERE status = 1 -- Only active files
ORDER BY content, sequence_number;
```

## **Query 2: Delete File to Data File Applicability**

```sql
-- Show which delete files can apply to which data files based on sequence numbers

WITH data_files AS (
SELECT
file_path as data_file_path,
sequence_number as data_seq_num,
record_count as data_record_count
FROM your_table.entries
WHERE content = 0 AND status = 1 -- Active data files
),

delete_files AS (
SELECT
file_path as delete_file_path,
sequence_number as delete_seq_num,
content as delete_type,
record_count as delete_record_count
FROM your_table.entries
WHERE content IN (1, 2) AND status = 1 -- Active delete files
)

SELECT
d.data_file_path,
d.data_seq_num,
df.delete_file_path,
df.delete_seq_num,
CASE df.delete_type
	WHEN 1 THEN 'POSITION_DELETE'
	WHEN 2 THEN 'EQUALITY_DELETE'
END as delete_type,
CASE
	WHEN df.delete_seq_num >= d.data_seq_num THEN 'APPLICABLE'
	ELSE 'NOT_APPLICABLE'
END as applicability,
df.delete_record_count
FROM data_files d
CROSS JOIN delete_files df
ORDER BY d.data_seq_num, df.delete_seq_num;
```

## **Query 3: Dangling Delete Files Detection**

```sql

-- Detect potentially dangling delete files based on sequence numbers

WITH max_data_seq AS (
SELECT MAX(sequence_number) as max_data_sequence
FROM your_table.entries
WHERE content = 0 AND status = 1 -- Active data files
),

delete_analysis AS (
SELECT
file_path,
sequence_number,
content,
record_count,
file_size_in_bytes,
CASE
	WHEN content = 1 THEN 'POSITION_DELETE'
	WHEN content = 2 THEN 'EQUALITY_DELETE'
END as delete_type
FROM your_table.entries
WHERE content IN (1, 2) AND status = 1 -- Active delete files
)

SELECT
da.file_path,
da.sequence_number as delete_seq_num,
da.delete_type,
da.record_count,
mds.max_data_sequence,
CASE
	WHEN da.sequence_number <= mds.max_data_sequence THEN 'ACTIVE'
	ELSE 'POTENTIALLY_DANGLING'
END as status
FROM delete_analysis da
CROSS JOIN max_data_seq mds
ORDER BY da.sequence_number DESC;
```

## **Query 4: Files Relationship with Partition Information**

```sql
-- Show data files and their applicable delete files with partition details

SELECT
e.content,
CASE e.content
	WHEN 0 THEN 'DATA'
	WHEN 1 THEN 'POSITION_DELETE'
	WHEN 2 THEN 'EQUALITY_DELETE'
END as file_type,
e.file_path,
e.sequence_number,
e.file_sequence_number,
e.partition,
e.record_count,
e.file_size_in_bytes / 1024 / 1024 as size_mb
FROM your_table.entries e
WHERE e.status = 1 -- Active files only
ORDER BY e.partition, e.content, e.sequence_number;
```

## **Query 5: Advanced Sequence Number Analysis**

```sql
-- Comprehensive analysis of sequence number relationships

WITH file_stats AS (
SELECT
content,
COUNT(*) as file_count,
MIN(sequence_number) as min_seq,
MAX(sequence_number) as max_seq,
AVG(sequence_number) as avg_seq,
SUM(record_count) as total_records,
SUM(file_size_in_bytes) / 1024 / 1024 / 1024 as total_size_gb
FROM your_table.entries
WHERE status = 1
GROUP BY content
),

cross_references AS (
SELECT
d.sequence_number as data_seq,
COUNT(CASE WHEN del.sequence_number >= d.sequence_number THEN 1 END) as applicable_deletes,
COUNT(del.sequence_number) as total_deletes
FROM (SELECT DISTINCT sequence_number FROM your_table.entries WHERE content = 0 AND status = 1) d
CROSS JOIN (SELECT sequence_number FROM your_table.entries WHERE content IN (1,2) AND status = 1) del
GROUP BY d.sequence_number
)

SELECT
CASE fs.content
	WHEN 0 THEN 'DATA_FILES'
	WHEN 1 THEN 'POSITION_DELETES'
	WHEN 2 THEN 'EQUALITY_DELETES'
END as file_type,
fs.file_count,
fs.min_seq,
fs.max_seq,
fs.avg_seq,
fs.total_records,
fs.total_size_gb
FROM file_stats fs
ORDER BY fs.content;
```

## **Query 6: Position Delete File Path Relationships**

```sql
-- For positional delete files, show which specific data files they reference

-- (This requires parsing the delete file content, so this is a conceptual query)

SELECT

del.file_path as delete_file_path,
del.sequence_number as delete_seq_num,
del.record_count as delete_record_count,
data.file_path as data_file_path,
data.sequence_number as data_seq_num,
data.record_count as data_record_count,

CASE
WHEN del.sequence_number >= data.sequence_number THEN 'DELETE_APPLIES'
ELSE 'DELETE_DOES_NOT_APPLY'
END as relationship
FROM your_table.entries del
JOIN your_table.entries data ON (

-- This join condition would need actual file path matching logic
-- In practice, you'd need to read the delete file content to get exact references
del.partition = data.partition -- Same partition assumption
)
WHERE del.content = 1 -- Position delete files
AND data.content = 0 -- Data files
AND del.status = 1
AND data.status = 1
ORDER BY del.sequence_number, data.sequence_number;
```

  

These queries help you understand:

- **Which delete files can apply to which data files** based on sequence number rules[^1]

- **Potential dangling delete files** that may need cleanup[^2]

- **The distribution of sequence numbers** across different file types

- **File relationships and their applicability** for merge-on-read operations


The key insight is that **delete files with sequence number N can only affect data files with sequence number ≤ N**. 
This ensures that deletes don't accidentally affect data that was added after the delete operation was committed.[^3]
  

[^1]: https://iceberg.apache.org/spec/
[^2]: https://iceberg.apache.org/docs/latest/spark-queries/
[^3]: https://amdatalakehouse.substack.com/p/understanding-apache-iceberg-delete
[^4]: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-delete.html
[^5]: https://www.apachecon.com/acna2022/slides/02_Ho_Icebergs_Best_Secret.pdf
[^6]: https://stackoverflow.com/questions/74142050/how-to-actually-delete-files-in-iceberg
[^7]: https://trino.io/docs/current/connector/iceberg.html
[^8]: https://docs.cloudera.com/runtime/7.2.16/iceberg-how-to/topics/iceberg-delete-update.html
[^9]: https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_meta_table/
[^10]: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-table-data.html
[^11]: https://iomete.com/resources/reference/iceberg-tables/maintenance
[^12]: https://iomete.com/resources/reference/iceberg-tables/writes
[^13]: https://docs.cloudera.com/cdw-runtime/cloud/iceberg-how-to/topics/iceberg-query-metadata.html
[^14]: https://dataos.info/resources/lakehouse/iceberg_metadata_tables/
[^15]: https://docs.snowflake.com/en/user-guide/tables-iceberg
[^16]: https://www.e6data.com/blog/apache-iceberg-snapshots-time-travel
[^17]: https://www.phdata.io/blog/how-querying-apache-iceberg-metadata-can-elevate-your-dataops-strategy/