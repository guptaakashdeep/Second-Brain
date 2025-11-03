---
title: yarn top - details
publish: true
draft: false
enableToc: true
tags:
  - Spark
cssclasses:
  - page-rainbow
---

The `yarn top` command provides a real-time view of running applications and their resource consumption on the cluster. 

The output headers detail various resource metrics for each application.

| Header      | Description                                                                                                                                                                                                                                                                                     |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TYPE`      | The framework or type of the application being run. In your example, all applications are Spark applications.                                                                                                                                                                                   |
| `QUEUE`     | The specific YARN scheduler queue to which the application was submitted.                                                                                                                                                                                                                       |
| `PRIOR`     | The priority of the application within its queue. A lower number typically indicates a higher priority, though this depends on scheduler configuration.                                                                                                                                         |
| `#CONT`     | The total number of containers currently allocated to the application.                                                                                                                                                                                                                          |
| `#RCONT`    | The number ofreserved containers. This indicates containers that have been reserved by the scheduler for the application but are not yet allocated.                                                                                                                                             |
| `VCORES`    | The total number of virtual cores currently being used by all allocated containers for the application.                                                                                                                                                                                         |
| `RVCORES`   | The number of reserved virtual cores corresponding to the reserved containers.                                                                                                                                                                                                                  |
| `MEM`       | The total amount of memory currently allocated to the application across all its containers.                                                                                                                                                                                                    |
| `RMEM`      | The total amount of reserved memory corresponding to the reserved containers.                                                                                                                                                                                                                   |
| `VCORESECS` | A cumulative measure of vCore usage over time. It is calculated by multiplying the number of allocated vCores by the number of seconds they have been running. This metric reflects the total CPU resource consumption throughout the application's lifetime.                                   |
| `MEMSECS`   | A cumulative measure of memory usage over time. It is calculated by multiplying the amount of allocated memory by the number of seconds it has been allocated. This provides a "GB-seconds" or "MB-seconds" value, representing the total memory resources consumed by the application so far . |
