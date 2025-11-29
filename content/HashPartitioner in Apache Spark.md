---
title: HashPartitioner in Apache Spark
publish: true
draft: false
enableToc: true
tags:
  - Spark
  - internals
cssclasses:
  - page-rainbow
---

HashPartitioner is an implementation of Spark's Partitioner abstraction that does hash-based partitioning for key-value RDDs.

Spark uses this partitioner internally for many shuffle-based **RDD transformations** like reduceByKey, partitionBy, etc.

The main role of HashPartitioner is to guarantee that identical keys are co-located, which enables parallel aggregation and joins without repeated data movement.

> [!info]- Misconception: HashPartitioning uses HashPartitioner internally
> HashPartitioner and [[HashPartitioning]] are independent of each other and belong to different layers of Spark, i.e., Spark Core and Spark SQL/Catalyst, respectively.
> 
> Although mathematical logic and the role of both things are same:
> `Hash % numPartitions`

## How does HashPartitioner work internally?
HashPartitioner is used for RDD operations (`groupByKey`, `reduceByKey`) and relies on Java's `hashCode()`. [Partitioner.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/Partitioner.scala#L114)

```scala
// From Apache Spark 3.5 Source Code
class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    // Relies on standard Java Object.hashCode
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }
}
```

Based on the code, it's clear that HashPartitioner takes the number of Partitions as input, which is internally used to calculate the partition index, based on the key of the received records.

```
Partition_Index = key.hashCode() % numParititions
```

Here's a simple example to understand this:

Let's assume there are 4 records in a RDD with keys as 1,2,3,4 and numPartitions = 2

```
Key 1: 1 % 2 = 1 --> Partition 1
Key 2: 2 % 2 = 0 --> Partition 0
Key 3: 3 % 2 = 1 --> Partition 1
Key 4: 4 % 2 = 0 --> Partition 0

2 records will be sent in Partition 0 and Partition 1 each
```