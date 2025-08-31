---
title: Data-aware scheduling using callbacks
publish: true
draft: false
tags:
- Airflow
- internals
- Programming
cssclasses:
- page-brown
---
[Airflow Datasets](https://www.astronomer.io/docs/learn/airflow-datasets) can be used to trigger an external dag to define cross-dag dependencies.

DAGs that update an Airflow Dataset are considered `producers`, and those that are triggered by dataset updates are considered `consumers`.

This works fine when a task is defined within the DAG context. Upon completion of a task with an `outlets` parameter, the consumer DAGs are triggered.
### Problem Statement
Trying to trigger consumer dags based on Airflow dataset updates from callback functions like `on_success_callback` and `on_failure_callback` doesn't work.

**Question:** How to trigger consumer dags on dataset update from callback function in Airflow ?
#### Reason for the behaviour
The main reasons for this behaviour are:
1. As per the airflow source code of [dag.py](https://github.com/apache/airflow/blob/main/airflow/models/dag.py#L3311), only those Airflow datasets are created by the scheduler during dag parsing, which are defined within the `outlets` parameters of a task definition. Provided that this task is defined within the DAG and part of the task dependencies.
   In case there is some task that is being executed from `callback` function by calling `.execute()` methods, even after defining the `outlets` parameter, consumer dag doesn't get triggers. The suspected reason for that is: *Airflow monitors datasets only within the context of DAGs and tasks.*

2. Defining dataset only in callback function task like above doesn't get created/registered in Airflow DB.

```python
from airflow.operator.python import PythonOperator
from airflow.operator.empty import EmptyOperator
from airflow.datasets import Dataset

sample_dataset = Dataset("/tmp/dataset.json")

def callback_function(context):
	callback_task = PythonOperator(task_id='callback', 
								   python_callable=some_function,
								   outlets=[sample_dataset])
	# executing a task like this doesn't triggers the consumer DAG. 
	# The main reason for that is, executing like this doesn't 
	# recognize the dataset in task_instance object and doesn't register any outlet events. 
	callback_task.execute(context)

default_args = {
	...
}

with DAG('dataset_test_producer',
		default_args=default_args,
		schedule=None,
		catchup=False
		) as dag:
	
	task1 = EmptyOperator(task_id='task_1', 
						on_success_callback=callback_function)
	
	task1
```

### Solution (Airflow `2.7.0`)
While looking for a solution to this problem, I looked into the airflow source code to identify, by default how dataset update triggers the consumer dags. 
While looking into the codebase, I looked into [datasets/manager.py](https://github.com/apache/airflow/blob/main/airflow/datasets/manager.py#L64). This has a method called `register_dataset_change` method. This method calls method that triggers notification actions on dataset update.
By default, when a task is completed successfully, that has `outlets` parameters defined makes a call to `register_dataset_change` method, this can be seen in [task_instance.py](https://github.com/apache/airflow/blob/main/airflow/models/taskinstance.py#L2929) which results in triggering of consumer DAG.

So the solution for this problem requires 2 steps:
1. Your producer dag must have at least one task with `outlets` defined for that dataset to work it as a producer.
   This is required so while parsing of DAG, the dataset on which update needs to be tracked is created in Airflow database.

2. Call `register_dataset_change` function from within the callback method. This will ensure that a dataset update event is registered and will trigger the consumer dags for the Airflow dataset.

```python
from airflow.datasets import Dataset
from airflow.datasets.manager import dataset_manager
from airflow.utils.session import provide_session

sample_dataset = Dataset("/tmp/dataset.json")

@provide_session
def callback_function(context, session=None):
	dataset_manager.register_dataset_change(task_instance=context['ti'],
								dataset=sample_dataset, session=session)

default_args = {
		...
	}
	
	with DAG('dataset_test_producer',
			default_args=default_args,
			schedule=None,
			catchup=False
			) as dag:
		# a dummy task to make sure dataset is registered.
		task0 = EmptyOperator(task_id='task_1',
							outlets=[sample_dataset]
							)
		task1 = EmptyOperator(task_id='task_1', 
							on_success_callback=callback_function
							)
		
		task0 >> task1
```

**New Findings:**

We don't need to define a task *just* to register the dataset. DAG Trigger via Dataset will still work. 
The only difference is from visualization perspective in Dataset tab, the Producer will not appear and there will only be Consumer in this case.
