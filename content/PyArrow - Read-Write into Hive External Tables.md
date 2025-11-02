---
title: PyArrow - Read-Write into Hive External Tables
publish: true
draft: false
enableToc: true
tags:
  - pyarrow
  - programming
  - Optimization
cssclasses:
  - page-rainbow
---

There are multiple ways to read from /write into Hive External tables using Python (without Apache Spark). Most of them (depending on the library being used, of course) require listing the files in the table location.

One of the classic and very first options that comes to mind is via using Pandas, but there is a more performant way via using PyArrow.

## Reads using PyArrow
To read a Hive-partitioned external table stored as Parquet files on S3 using PyArrow, the PyArrow Dataset API can be used, which has built-in support for Hive-style partitioning and S3 access.
S3 integration is supported via `pyarrow.fs.S3FileSystem` or `s3fs`.

```python
# Simple Example
import pyarrow.dataset as ds

# Root path of the Hive External Table on S3
s3_path = "s3://bucket/path/to/hive-partitioned-table"

dataset = ds.dataset(
	s3_path,
	format="parquet",
	partitioning="hive"
	)
```

**For more controlled S3 Access:**

```python
import pyarrow.dataset as ds 
import pyarrow.fs 

s3_fs = pyarrow.fs.S3FileSystem(region="eu-west-1") # Or customize as needed

# Notice the path passed here doesn't have s3:// because the filesystem is specifically passed.
dataset = ds.dataset(
	"your-bucket/path/to/hive-partitioned-table",
	filesystem=s3_fs,
	format="parquet",
	partitioning="hive"
	)
```
More detailed docs on `S3FileSystem` [here](https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html).

### Selecting columns and Filtering
PyArrow **pushes down both filters (predicate pushdown)** and **column projections** during Parquet dataset reading.

```python
import pyarrow.dataset as ds

table = dataset.to_table(
	columns=["col1", "col2"],
	filter=(ds.field("col3") > 100)
	)
```

- `columns` accepts a list of column names to read.
- `filter` can use expressions like `ds.field('colname') == value` or more complex logical/comparison expressions.

This approach prevents unnecessary data transfer and speeds up both I/O and downstream processing by loading only the required data.

The coolest and best thing about PyArrow is zero-copy memory sharing, which avoids expensive conversions. This is one of the aspects of [[Apache Arrow]] that makes it more performant.

Fine! **What if the dataset is too big to fit into memory? What can PyArrow do about it?**

When working with large Parquet or Hive-partitioned datasets that don’t fit into memory, PyArrow allows **streaming reads in batches** instead of loading the full table at once.

This is achieved via the **Dataset** or **ParquetFile** APIs, using iterators that return **RecordBatch** objects incrementally.

```python
# Huge Multifile dataset like Hive-partitioned or directory-based Parquet dataset
import pyarrow.dataset as ds

dataset = ds.dataset(
	"s3://bucket/hive-partitioned-data/",
	format="parquet",
	partitioning="hive")

for batch in dataset.to_batches(
	columns=["user_id", "event_type"],
	filter=(ds.field("year") == 2025),
	batch_size=200_000
	):
    # Process batch using PyArrow compute.
    print(batch.num_rows, "rows in this batch")
```

#### Zero Copy and Expensive Conversions
The term **zero-copy memory sharing** in Apache Arrow (and PyArrow) refers to the ability to **access existing in-memory data without copying it** between processes, languages, or libraries.

It’s a crucial performance optimization that minimizes memory usage and avoids the CPU overhead associated with serialization and deserialization.

In typical data handling pipelines (e.g., with Pandas, NumPy, or Spark), moving data between systems involves _copying_ the data from one memory layout to another. Each copy:
- Allocates new memory
- Serializes (writes) or deserializes (reads) data
- Uses CPU cycles to convert types

**“Expensive conversions”** refers to:
- **Serialization/Deserialization**: Converting Arrow data into Python objects or Pandas Series involves materializing new arrays.
- **Memory Allocation**: Each copy duplicates gigabytes for large datasets, worsening memory scaling.
- **Type Casting**: Converting Arrow’s efficient bitmap null handling to NumPy’s masked types incurs additional CPU cost.

Let's take this a step further.

#### Getting list of distinct values for a column
We can get the distinct values list for a column in the memory itself without converting it to Pandas Dataframe, avoiding any expensive conversions.

```python
# Without any expensive conversions
unique_table = table.select(['col1']).group_by(['col1']).aggregate([])

# Convert to Python object finally to get all the results as a list
unique_values = unique_table.to_pylist()
```

This can be extended to perform aggregations and other computations in memory without any conversions. `pyarrow.compute` can be used to perform computational operations on PyArrow datasets.
## Writes using PyArrow
Writing to external Hive Tables requires:
- Data that needs to be written should be of `pa.Table` format
	- This can be achieved via `pa.Table.from_*` [methods](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).
- Define a schema for the data that is being written if needed (in case `from_pydict` is used)
- Root path of the table
- Partition column (if the External Hive table is partitioned)
- `existing_data_behavior`: Controls how the dataset will handle data that already exists in the destination. More details [here](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_to_dataset.html)

```python
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

s3_fs = pyarrow.fs.S3FileSystem(region="eu-west-1")

log_schema = pa.schema([
	pa.field("app_id", pa.int32()),
	pa.field("cluster_id", pa.string()),
	pa.field("time_taken_mins", pa.float64()),
	pa.field("execution_dt", pa.date32())
	])
	
log_data = {
	"app_id": "application_12345_0001",
	"cluster_id": "j-xC12345",
	"time_taken_mins": 3.5,
	"execution_dt": datetime.now().date()
}

# Conversion of values to arrays - can also be converted to pa.array
log_table_data = {key: [value] for key, value in log_data.items()}
table = pa.Table.from_pydict(log_table_data, schema=log_schema)

# root_path can have s3:// even with filesystem provided, unlike the during reads
pq.write_to_dataset(
	table,
	root_path="s3://bucket-name/test/control_db/app_run_logs",
	filesystem=s3_fs,
	partition_cols=["execution_dt"],
	existing_data_behavior="overwrite_or_ignore",
)
```