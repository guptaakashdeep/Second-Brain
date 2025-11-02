---
title: Context Objects in Python
publish: true
draft: false
enableToc: true
tags:
  - design-pattern
  - Python
  - Programming
cssclasses:
  - page-dblue
---

A **context object** in data engineering is a design pattern used to reduce the number of parameters passed through function calls by grouping related dependencies and configuration settings into a single object. 

This simplifies function signatures and improves maintainability, especially in complex pipelines or ETL applications, from a Data Engineering perspective.

A great example of a context object design is Airflow's `context` object, which includes details about `task_instance`, `dag_runs`, `dagconf`, etc.

Here's a simple example of how this can be used in an ETL pipeline
## Before Context Object

```python
def extract_data(spark_session, logger, source_path):
    logger.info("Extracting data")
    df = spark_session.read.parquet(source_path)
    return df

def transform_data(logger, config, df):
    logger.info("Transforming data")
    columns = config.get("selected_columns", [])
    if columns:
        df = df.select(*columns)
    return df

def load_data(logger, df, target_path):
    logger.info("Loading data")
    df.write.mode("overwrite").parquet(target_path)

# Usage:
df = extract_data(my_spark_session, my_logger, "s3://input/")
df_transformed = transform_data(my_logger, {"selected_columns": ["id", "timestamp", "value"]}, df)
load_data(my_logger, df_transformed, "s3://output/")

```

Each function needs many parameters; calling each function is verbose and error-prone if you have to pass multiple configs/resources repeatedly.

## After Using a Context Object
Context object can be defined using [[Dataclasses - When to and not to use|Dataclasses]].

```python
from dataclasses import dataclass
from pyspark.sql import SparkSession
from custom_logging import SparkLogger

@dataclass
class ETLContext:
    spark_session: SparkSession
    logger: SparkLogger
    config: dict

def extract_data(context: ETLContext, source_path: str):
    context.logger.info("Extracting data")
    df = context.spark_session.read.parquet(source_path)
    return df

def transform_data(context: ETLContext, df):
    context.logger.info("Transforming data")
    columns = context.config.get("selected_columns", [])
    if columns:
        df = df.select(*columns)
    return df

def load_data(context: ETLContext, df, target_path: str):
    context.logger.info("Loading data")
    df.write.mode("overwrite").parquet(target_path)


# Usage:
etl_context = ETLContext(
    spark_session=my_spark_session,
    logger=my_logger,
    config={"selected_columns": ["id", "timestamp", "value"]}
)

df = extract_data(etl_context, "s3://input/")
df_transformed = transform_data(etl_context, df)
load_data(etl_context, df_transformed, "s3://output/")
```

This reduces repetition, function signatures are cleaner, and adding new dependencies (e.g., user credentials) only requires updating the Context object, not all function signatures.

> [!warning]
> Context objects should be used judiciously, especially in higher-level orchestration; avoid overuse in low-level utility functions to minimize coupling
