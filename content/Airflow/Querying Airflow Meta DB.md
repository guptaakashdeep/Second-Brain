---
title: Querying Airflow Meta Database from Airflow DAG
publish: true
draft: false
tags:
- Airflow
- Programming
cssclasses:
- page-brown
---

Querying Airflow Metadata DB can be handy when you have a use case that can't be solved using methods provided by the Airflow models library. In addition, it can be convenient if you need something for Auditing purposes, like getting the number of successful/failed DAG Runs for a particular dag or how many times a dag has been triggered for the day and whatnot.
In most cases, you will find the methods in Airflow models to achieve what you are looking for (which basically does the same thing internally, i.e. query Airflow metadatabase). Still, the main point is that whatever the case, it's always helpful to know how to query the Airflow meta DB if necessary.

First thing first, 
- *How do I know what tables Airflow database has, and what information can I get from it?*
  The answer to this is in the Airflow ERD Schema Diagram provided in the Airflow Documentation [ERD Diagram](https://airflow.apache.org/docs/apache-airflow/stable/database-erd-ref.html)
- Most of these tables are exposed as Airflow models in `airflow.models` package, such as `task_instance` table is exposed as `airflow.models.taskinstance`, `dag_run` table is exposed as `airflow.models.dagrun`, `xcom` table is exposed as `airflow.models.xcom`, and so on.

To keep things simple and easier to understand, let's examine a scenario that can be solved by using both the Airflow models library method and directly querying the database. 

## Getting Dag Run for the day
***
Let's assume we have a scenario where we have a DAG that: 
- runs twice a day and wants to perform different tasks based on whether the DAG run is the first or second run. 
- If it's the first run of the day, perform `task_X` and 
- If the first DAG run was successful, perform `task_Y` on the second DAG run of the day.

All we need to do is get the number of successful DAG runs for the day before deciding if `task_X` or `task_Y` needs to be performed.

Let's look into the 2 implementations of this scenario.
#### 1. Using Airflow models class methods
`airflow.models.dagrun.DagRun` class provides a [find()](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dagrun/index.html#airflow.models.dagrun.DagRun.find) method to get the DAG Runs for a particular `dag_id`. 

```python
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.models.dagrun import DagRun
from datetime import datetime

@provide_session
def _fetch_dag_run_nondb(session=None, **context):
	runs = DagRun.find(dag_id='automation_sample',
					state=State.SUCCESS,
					session=session)

	# filtering runs for todays date
	todays_run = list(filter(lambda run: run.execution_date == datetime.now().date, runs))
```
#### 2. By Querying Airflow Metadata DB

```python
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.models.dagrun import DagRun
from datetime import datetime
from sqlalchemy import Date

@provide_session
def _fetch_dag_run_db(session=None, **context):
	ti_runs = session.query(DagRun).filter(
		DagRun.dag_id == 'automation_sample',
		DagRun.state == State.SUCCESS,
		DagRun.execution_date.cast(Date) == datetime.now().date()
		).all()
	
	print("===Fetched ti runs ====")
	print(ti_runs)
	
	for record in ti_runs:
		print("\n", f"{record.external_trigger} - {record.run_type} - {record.state} - {record.execution_date} - {record.execution_date.date()} - {record.conf}")
```

## The Difference
***
Did you notice any difference in both implementations besides the fact that one uses a method and the other queries the database?

If your answer is `execution_date`, good work; you got it right. 
One flexibility you get when querying the database directly is that you can filter down the data while reading from the database directly using the `DagRun.find()` method, which still provides you with the execution_date parameter but requires you to pass *the exact timestamp for the first dag run.* 