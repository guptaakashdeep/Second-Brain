---
title: Why does Spark convert Columnar Data into Row Orientation?
publish: true
draft: false
tags:
- Spark
- internals
cssclasses:
- page-green
---
If you work with **Parquet, ORC, or Open Table Formats**, you’ve likely noticed the **ColumnarToRow** node in the Spark plan. But why does Spark do this, and is it a drawback?

This brings up further quite a few questions:
- Isn't it better to use columnar data format while processing also?
- What's the point of using Columnar format when data needs to be converted back into Row-oriented data representation in memory?
- Is it some optimization that was added in Spark 3.0?

#### Main Reason for Conversion
Spark "mostly" works on RDDs of InternalRow, i.e., memory-optimized row-oriented data representation in memory.

So, when you read from a columnar format/source, this is what happens:
- Spark Scans always produces columnar data when reading from columnar formats.
- So, You get all the benefits while reading, like faster reading, skipping non-relevant data, IO performance, etc.
- Once the data is read in columnar format, Spark needs to convert it into InternalRow format to perform most of its operations.
- To do this, the ColumnarToRow node is added to Spark Plan.

#### Doesn't this add an overhead of conversion?
Yes, it does, but its implementation is highly efficient using vectorization conversion in batches.

#### Is this an optimization in Spark 3.0?
No, this conversion has been happening since earlier versions of Spark. In Spark < 3.0, it occurred within the **ColumnBatchScan** node. 
Spark 3.0 makes it more **transparent** to the users as a part of the Columnar Processing Support extension.

#### Is this an issue?
This is the question I have been exploring myself, and based on what I have dug so far:
- The columnar execution model is more beneficial than the row-oriented model.
- The most used format in the current era is Apache Parquet (used in all the Open Format Tables)

#### Curious Questions
***
If Databricks uses Photon, which uses Columnar In-Memory Representation, why does reading from Parquet still add ColumnarToRow Node? -- tested it in the Community Server.