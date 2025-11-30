---
title: HashPartitioning in Apache Spark
publish: true
draft: false
enableToc: true
tags:
  - Spark
  - internal
cssclasses:
  - page-rainbow
---

**Hash Partitioning** is a data distribution strategy used in distributed systems (such as Spark) to deterministically **assign records to partitions** based on the hash of a specific key.

This is the same as how [[HashPartitioner in Apache Spark|HashPartitioner]] works in Spark, but with some additional changes and at Spark Catalyst layer. 
HashPartitioning is the most commonly used strategy during the aggregations, joins, and repartitioning of data when using Spark DataFrame API.

## How does HashPartitioning work internally?

In Hash Partitioning, the `partitionID` is determined:
- Calculating the hash of the key using [[Murmur3Hash]]
- Positive Modulo the hash value with the number of partitions defined via `spark.sql.shuffle.partitions`

Code snippet from [Partitioning.scala](https://github.com/apache/spark/blob/branch-3.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala#L299)

```scala
// From Apache Spark 3.5 Source Code (simplified)
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression
  with Partitioning {

  // This is the key verification point:
  // It constructs a Catalyst expression tree that calculates the ID.
  override def partitionIdExpression: Expression = {
    // 1. Hash the keys using Murmur3Hash
    // 2. Apply Pmod (Positive Modulo) by numPartitions
    Pmod(new Murmur3Hash(expressions), Literal(numPartitions))
  }
}
```

### Sample Code
Here's a sample code that can validate the same concept

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, col, expr

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HashPartitioningCheck") \
    .getOrCreate()

# 1. Create sample data
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("David", 4), ("Eve", 5)]
df = spark.createDataFrame(data, ["name", "id"])

# 2. Repartition by "id" into 4 partitions
# This forces a Shuffle Exchange using HashPartitioning
num_partitions = 4
df_repartitioned = df.repartition(num_partitions, "id")

# 3. Verify the Partition IDs
# - actual_pid: The real partition ID assigned by Spark (using spark_partition_id())
# - calculated_pid: The ID we calculate manually using the formula `pmod(hash(key), numPartitions)`
#   Note: 'hash' in Spark SQL uses Murmur3Hash, same as HashPartitioning
df_check = df_repartitioned.select(
    col("id"),
    col("name"),
    spark_partition_id().alias("actual_pid"),
    expr(f"pmod(hash(id), {num_partitions})").alias("calculated_pid")
)

df_check.show()

# 4. Check for any mismatches (Should be 0)
mismatches = df_check.filter(col("actual_pid") != col("calculated_pid")).count()
print(f"Mismatched rows: {mismatches}")

spark.stop()
```

```sql   
+---+-------+----------+--------------+
| id|   name|actual_pid|calculated_pid|
+---+-------+----------+--------------+
|  2|    Bob|         0|             0|
|  4|  David|         0|             0|
|  5|    Eve|         0|             0|
|  1|  Alice|         1|             1|
|  3|Charlie|         3|             3|
+---+-------+----------+--------------+

Mismatched rows: 0
```

## Diving Deeper in Code
This section traces the code path from the logical `HashPartitioning` concept down to the runtime partitioning execution in Spark SQL.

> [!summary]- TL;DR
> 1. **Logical:** `HashPartitioning` (Expression)
> 2. **Physical Setup (`ShuffleExchangeExec`):**
>    - Compiles `HashPartitioning.partitionIdExpression` into a `UnsafeProjection`.
>    - Creates `PartitionIdPassthrough` as the Spark Core `Partitioner`. 
>  3. **Runtime:**
>    - **Step 1:** For each row, run `UnsafeProjection` -> `Murmur3Hash` -> `Pmod` -> `partitionId` (Int).
>    - **Step 2:** Pass `(partitionId, row)` to `ShuffleWriter`.
>    - **Step 3:** `ShuffleWriter` calls `part.getPartition(partitionId)`.
>    - **Step 4:** `PartitionIdPassthrough` returns `partitionId` as-is.

So effectively, **Spark SQL *bypasses* the standard `HashPartitioner` logic** and bakes the partitioning logic directly into the row transformation via Catalyst expressions.

### 1. Logical to Physical Planning (`SparkStrategies`)

Starts when the Catalyst optimizer converts a logical plan (like a `Join` or a `Repartition`) into a physical plan.

- **File:** `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala`

- **Mechanism:** The `EnsureRequirements` physical rule inserts a `ShuffleExchangeExec` operator when it detects that the child's output partitioning doesn't satisfy the operator's required child distribution (e.g., `ClusteredDistribution` for Hash Join).

- **Result:** A `ShuffleExchangeExec` node is created with a specific `newPartitioning` field, which is an instance of `HashPartitioning`.
 
```scala
// HashPartitioning is a Catalyst Expression, not a Partitioner
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning { ... }
```

### 2. Preparing the Shuffle (`ShuffleExchangeExec`)

This is where the logical `HashPartitioning` is translated into runtime components.

- **File:** `sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala`

- **Method:** `prepareShuffleDependency()`

This method is responsible for creating the `ShuffleDependency` needed by Spark Core's shuffle mechanism. It defines the `part` (Partitioner) and `getPartitionKey` (function to calculate partition ID).

#### A. Creating the Partitioner

Interestingly, for `HashPartitioning`, Spark **does not** use `HashPartitioner`. It uses a dummy identity partitioner because the partition ID is calculated _before_ the partitioner sees it.

```scala
// Inside prepareShuffleDependency
val part: Partitioner = newPartitioning match {
  case HashPartitioning(_, n) =>
    // For HashPartitioning, we use a "PartitionIdPassthrough"
    // This simple partitioner just returns the key it receives as the partition ID.
    new PartitionIdPassthrough(n)
  ...
}
```

[PartitionIdPassthrough in Partitioner.scala](https://github.com/apache/spark/blob/8f6c6d646b4596877bae111858227653d42ec51d/core/src/main/scala/org/apache/spark/Partitioner.scala#L138)
```scala
/**
 * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
 * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
 */
private[spark] class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
```
#### B. Generating the Partition ID

The actual hash logic comes from `HashPartitioning.partitionIdExpression` ([[Murmur3Hash]] + Modulo), which is compiled into a projection.

```scala
// Inside prepareShuffleDependency logic for HashPartitioning
case h: HashPartitioning =>
  // 1. Create a projection using the Catalyst expression that calculates partition ID
  val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
  
  // 2. Define the function that will be applied to every row
  row => projection(row).getInt(0) // Evaluates Murmur3Hash(keys) % numPartitions

```

### 3. Execution (`ShuffleExchangeExec.doExecute`)

- **File:** `sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala`

When the operator executes:
1. It runs the `rdd.mapPartitions` transformation.
2. Inside the map function, it applies the `getPartitionKey` function (derived above) to every `InternalRow`.
3. This results in an RDD of `(partitionId, InternalRow)`.
4. This RDD is passed to the `ShuffleDependency` with the `PartitionIdPassthrough` partitioner.