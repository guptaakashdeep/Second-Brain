---
title: Right Sizing Spark Executors
publish: true
draft: false
enableToc: true
tags:
  - Spark
  - Optimization
  - DataEngineering
cssclasses:
  - page-rainbow
---
>[!Warning] This is WIP

"[Right-sizing Spark executor memory](https://www.linkedin.com/blog/engineering/infrastructure/right-sizing-spark-executor-memory)" details LinkedIn’s large-scale use of Apache Spark and the challenges faced in efficiently tuning Spark executor memory for analytical workloads.

**Executor Memory** was the single biggest source of configuration changes (52%) and significantly impacted resource efficiency (memory utilization was only 50%) and user productivity.

## LinkedIn's Solution
LinkedIn developed an automated Spark right-sizing system, focusing first on executor memory.

The system:

- Uses a continuous feedback loop: nearline (Kafka → Samza → MySQL DB) for historic memory metrics and real-time signals for immediate OOM failure response.

- Policy-based adjustment: OOM errors trigger a 50% memory increase **(Policy P0: Executor OOM Scale Up)**; otherwise, a heuristic uses a 30-day memory usage history to set memory with conservative buffers for jobs with limited historical data **(Policy P1: Heuristic-Based Executor Memory Scale Up/Down)**.

- Guardrails cap how much memory can scale up or down to maintain stability.
## My Thoughts
This is an excellent solution that, with some effort, can significantly improve resource utilization for jobs in a multi-tenant Spark cluster.

Furthermore, once implemented, it can lead to substantial cost savings for projects running Spark on serverless platforms such as EMR Serverless, AWS Glue, Databricks Serverless, and others.

The cost models for these serverless solutions are primarily based on the amount of resources used and the duration of their usage. By right-sizing executors, it’s possible to reduce resource consumption, resulting in notable cost savings.

Although the LinkedIn blog does not provide detailed implementation steps, it does offer lots of details that can be used by a seasoned Spark Developer.

### Implementation Idea
Here's how I am thinking to implement it

The general idea is to get these details for **every executor** for a Spark Application:
- `PeakJVMMemory` details, i.e., `MAX_HEAP_MEMORY`
- `ProcessTreeMemory` details, i.e., `MAX_TOTAL_MEMORY`
- Calculate `MAX_OVERHEAD_MEMORY`

$$OverheadMemory_{max} = TotalMemory_{max} \ - \ HeapMemory_{max}$$

>[!infor] How to get memory details?
>All these executor-related details can be fetched from [Spark History Server REST APIs](https://spark.apache.org/docs/latest/monitoring.html#executor-metrics) (SHS)
>All the executors details for an application ID can be retrieved from: `https://<history-server-baseUrl>/api/v1/applications/<appId>/<attemptId>/allexecutors`
>
>This returns a list of all the executors. To retrieve the memory details, we need all of these details present in `peakMemoryMetrics.*`

>[!tip] Enabling Process Map Metrics
>To enable Process Map Metrics to be calculated during batch run for each executor for a Spark Application, it's ***required*** to set these parameters at job level:
>- `spark.executor.processTreeMetrics.enabled = true`
>
>Additionally, these metrics are only available for `/proc` file systems, i.e., any Linux/Unix-based systems.

### Calculating Memory Details from Executor Metrics
Once we get all the `peakMemoryMetrics` from SHS for ***EACH*** executor, memory details can be calculated using

$$
\begin{gather*}
HeapMemory_{max} \ = \ JVMHeapMemory \ + \ JVMOffHeapMemory \\\\
TotalMemory_{max} \ = \ ProcessTreeJVMRSSMemory
\end{gather*}
$$

>[!tip]- ProcessTree Memory Metrics
> ProcessTree Memory RSS Metrics has 3 types of metrics being reported:
> - `ProcessTreeJVMRSSMemory`
> - `ProcessTreePythonRSSMemory`
> - `ProcessTreeOtherRSSMemory`
> 
> Initial thinking was: `TOTAL_MAX_MEMORY` will be sum of these, but looking at the metrics for a particular job, it looks like `ProcessTreeJVMRSSMemory` include `ProcessTreePythonRSSMemory` also when there is any Python UDF being used in the Spark Application.
> 
> ***How did I conclude this?***
> If I add both `ProcessTreeJVMRSSMemory` and `ProcessTreePythonRSSMemory` this is exceeding the entire [[Spark Unified Memory Management|Executor Container Memory]]. Also, `ProcessTreeJVMRSSMemory` actually includes the entire process and the child process memory. During Python UDF execution, a worker process is initialized by the executor as a child process.


[[Heap and RSS in Spark|Relation between Heap and RSS Memory in Spark]] explains more details on how these are related.

Once you have all these details, LinkedIn Blog mentions to calculate the suggested `HEAP` and `OVERHEAD` memory using:

$$
\begin{gather*}
HeapMemory = HeapMemory_{max} + BUFFER \times \left( \frac{HeapMemory_{max}}{TotalMemory_{max}} \right) \\\\
OverheadMemory = OverheadMemory_{max} + BUFFER \times \left( \frac{OverheadMemory_{max}}{TotalMemory_{max}} \right) 
\end{gather*}
$$

Looking at this formula directly, it appears to use raw maxima to suggest updated values for `HeapMemory` and `OverheadMemory`. However, applying this approach can lead to over-provisioning of executor memory, especially in cases of data skew, where a single executor may experience significantly higher memory usage.

I believe that using [[What the heck are Percentiles?|percentiles]] (such as `p50`, `p90`, or `p95`) of maximum memory usage is a more effective method for determining appropriate Heap and Overhead memory. This approach accounts for skewness without resulting in unnecessary over-provisioning of resources.

#### Updated Calculation based on percentile
- Calculate `p50` of Heap and Total memory to calculate Overhead Memory.
- Calculate `p90` of Heap and Total memory to calculate Overhead Memory.
- If `p90 - p50` gap is small (e.g., < 10-20% of `p50` => `(p90 - p50)/p50` ), `p90` is a good point to do the further calculation
- if `p90 - p50` gap is **large** (e.g., >20-30% of p50), flag/highlight:
	- **Investigate further:** Is the skew legitimate (e.g., data skew, uneven partitions)? Can the job be optimized to balance usage?
	- Optionally set a “middle ground”:
	    - Take a **weighted mean** between p50 and p90 (e.g., 70% p50 + 30% p90).
	    - Allow the user to select/confirm whether to provision for p90 (safe but costly) or p50 (more efficient, might risk OOM for outliers).
	- Can be configurable.