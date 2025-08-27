---
title: Magnet - Push based shuffle service
draft: true
tags:
- Spark
- DataEngineering
---
Magnet is a shuffle service developed by Linked In.

```python
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("yarn").app("samplecode").getOrCreate()
```

```sql
SELECT * from db.table
```

![[./images/Spark-On-Yarn-mem.png]]
