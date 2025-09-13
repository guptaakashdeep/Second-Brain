---
title: What the heck are Percentiles?
publish: true
draft: false
enableToc: true
tags:
  - 🌱
cssclasses:
  - page-rainbow
---


In the world of data, various percentiles are calculated to understand the distribution of data and to make comparisons. A percentile indicates the value below which a certain percentage of observations fall.[statisticsbyjim](https://statisticsbyjim.com/basics/percentiles/)

Here are some of the most commonly used percentiles and their specific applications:

## Key Percentiles and Their Uses

- **25th, 50th, and 75th Percentiles (Quartiles)** These are some of the most frequently used percentiles. They divide a dataset into four equal parts.[wikipedia](https://en.wikipedia.org/wiki/Percentile)
    
    - **25th Percentile (First Quartile or Q1)** This is the value below which 25% of the data falls. It's often used to understand the lower end of a distribution. For example, in salary data, it can show the income level of the bottom quarter of earners.[statisticsbyjim](https://statisticsbyjim.com/basics/percentiles/)
        
    - **50th Percentile (Median or Second Quartile or Q2)** The median is a crucial measure of central tendency. It represents the midpoint of the data, where half the values are below it and half are above. It's preferred over the mean (average) for skewed distributions, such as income or house prices, because it's not affected by extreme outliers.[britannica+2](https://www.britannica.com/topic/percentile)
        
    - **75th Percentile (Third Quartile or Q3)** This is the value below which 75% of the data is found. It helps to understand the higher end of a distribution. In performance metrics, it might represent a high-performing threshold.[statisticsbyjim](https://statisticsbyjim.com/basics/percentiles/)
        
    - **Interquartile Range (IQR)** The difference between the 75th and 25th percentiles (Q3 - Q1), the IQR measures statistical dispersion and represents the middle 50% of the data. It's a key component in identifying outliers and is used in box plots.[statisticsbyjim](https://statisticsbyjim.com/basics/percentiles/)
        
- **90th, 95th, and 99th Percentiles (Tail-End Percentiles)** These are critical for understanding the behavior at the extreme high end of a distribution.
    
    - **Application in Performance Monitoring and Engineering** In fields like web performance and systems engineering, these percentiles are used to measure "worst-case" scenarios. For example, the 95th percentile (P95) of response times for a web service indicates that 95% of users have an experience that is that fast or faster. This is more informative than the average, as it captures the experience of the majority of users, including those on the slower end, without being skewed by a few extreme outliers.[fastercapital](https://fastercapital.com/topics/applications-of-percentiles.html/1)
        
    - **Application in Risk Management** In finance, these percentiles are used in models like Value at Risk (VaR) to estimate potential losses. For instance, a 99% VaR indicates the maximum loss that is not expected to be exceeded 99% of the time.[fastercapital](https://fastercapital.com/topics/applications-of-percentiles.html/1)
        
- **1st and 5th Percentiles** These are used to understand the extreme low end of a distribution. They are often used in manufacturing for quality control or in finance to identify the worst-performing assets.
    

Here are practical examples of how different percentiles are used to measure and analyze the performance of query engines and databases.

## **Analyzing Query Latency**

Imagine you have collected the execution times (in milliseconds) for thousands of a specific type of query over the last hour.

- **p50 (Median): The Typical User Experience**
    
    - **Value:** `50ms`
    - **Meaning:** Half of the queries finished in 50ms or less. This represents the most typical performance a user or application will experience. It's a much better indicator of central tendency than the average, which can be skewed by a few very slow queries.[geeksforgeeks+1](https://www.geeksforgeeks.org/how-to-calculate-percentiles-for-monitoring-data-intensive-systems/)
        
- **p90 and p95: The "Reasonably Worst-Case" Experience**
    
    - **Value (p95):** `250ms`
    - **Meaning:** 95% of queries completed within 250ms. This is a common metric for setting Service Level Objectives (SLOs). You might set a goal that the p95 latency must remain below 300ms. It tells you about the experience of the majority of your users, including those who experience some slowness, but it ignores the most extreme outliers.[library.humio](https://library.humio.com/data-analysis/functions-percentile.html)
        
- **p99 and p99.9: The "Tail Latency" for Identifying Critical Issues**
    
    - **Value (p99):** `800ms`
    - **Meaning:** 99% of queries finished in under 800ms, but 1% took longer. This small percentage might represent queries affected by specific issues like network packet loss, a "noisy neighbor" on a shared resource, or a specific type of data that causes poor performance. While it affects only a few users, this metric is crucial for identifying deep-seated problems that could become more widespread.[abstracta](https://abstracta.us/blog/performance-testing/performance-testing-metrics/)
## **Database and System Resource Utilization**

Percentiles are also used to monitor the resources consumed by a database.

- **p25: Identifying Underutilized Resources**
    
    - **Metric:** CPU utilization of a database node.
    - **Value (p25):** 15%
    - **Meaning:** For 25% of the time, the CPU utilization was 15% or lower. While the average might be higher, this low-end percentile could indicate that the node is frequently idle. This insight could lead to a decision to scale down the cluster during off-peak hours to save costs.[tech-couch](https://tech-couch.com/post/measuring-software-performance-with-percentiles)
        
- **p75 and IQR: Understanding Performance Variability**
    
    - **Metric:** I/O operations per second (IOPS).
    - **Value (p25):** 1,000 IOPS
    - **Value (p75):** 5,000 IOPS
    - **Meaning:** The middle 50% of the time, the database is operating between 1,000 and 5,000 IOPS. This Interquartile Range (IQR) gives you a sense of the "normal" operating range. A very wide IQR might indicate inconsistent performance, while a narrow one suggests stable and predictable behavior.

## Summary Table: Database Performance Scenarios

|Percentile|Metric Example|Interpretation|Actionable Insight|
|---|---|---|---|
|**p50**|Query Latency|"The typical query takes 50ms."|Provides a baseline for the most common user experience.|
|**p95**|API Response Time|"95% of API calls return in under 250ms."|Used to define and monitor SLOs to ensure a good experience for the vast majority of users.|
|**p99**|Job Execution Time|"1% of data processing jobs take longer than 10 minutes."|Helps to identify and debug rare but critical performance bottlenecks.|
|**p25**|CPU Utilization|"The server's CPU is at 15% utilization or less for a quarter of the time."|Reveals opportunities for cost savings by scaling down underutilized resources.|
|**IQR**|Disk I/O|"The database's disk I/O is usually between 1,000 and 5,000 operations."|Helps to understand the consistency of performance. A wide range may point to instability.|
