---
title: Sum Types in SQL
publish: true
draft: false
enableToc: true
tags:
  - SQL
cssclasses:
  - page-brown
---

I have always been confused between cumulative, running, and rolling sums. This is just a short note to avoid that confusion.
## Simple Aggregate Sum
The standard, basic use of the `SUM()` function without a `WINDOW` clause.

```sql
-- Calculating the total sales for an entire year or for a specific region.
SELECT SUM(sale_amount) AS total_sales
FROM sales
WHERE sale_date BETWEEN '2023-01-01' AND '2023-12-31';
```
## Cumulative Sum/Running Total
It calculates the sum of the current row and all preceding rows within the result set, based on a defined order.

| sale_date  | sale_amount | cumulative_sum |
|:----------:|:-----------:|:--------------:|
| 2025-01-01 |    1000     |      1000      |
| 2025-01-02 |     500     |      1500      |
| 2025-01-03 |    1500     |      3000      |
| 2025-01-04 |    2000     |      5000      |
| 2025-01-05 |    3000     |      8000      |
| 2025-01-06 |     300     |      8300      |

```sql
-- Tracking how much total revenue has accumulated day-by-day 
-- until today
SELECT
    sale_date,
    sale_amount,
    SUM(sale_amount) OVER (ORDER BY sale_date) AS cumulative_sum
FROM
    daily_sales
```

### Partitioned Cumulative Sum
The cumulative sum that resets when a specified grouping column changes. It uses the `PARTITION BY` clause within the window definition.

```sql
-- Calculating a running total for sales _within each product category separately.
SELECT
    category,
    sale_date,
    sale_amount,
    SUM(sale_amount) OVER (
        PARTITION BY category
        ORDER BY sale_date
    ) AS category_running_total
FROM
    sales_by_category;

```

## Rolling Sum (Moving Window Sum)
This type of sum calculates the aggregate over a "moving" window of a fixed size, either by a number of `ROWS` or a time `RANGE`

```sql
-- Calculating a 7-day average of sales, where the window slides forward with each new day.
SELECT
    sale_date,
    sale_amount,
    SUM(sale_amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW -- Current row + 6 preceding rows = 7 total rows
    ) AS seven_day_rolling_sum
FROM
    daily_sales;
```

```sql
-- 7 calendar days back
-- We use `INTERVAL '6 DAY' PRECEDING` to sum the current day and the 6 days prior, totaling a 7-day period.
SELECT
    sale_date,
    sale_amount,
    SUM(sale_amount) OVER (
        ORDER BY sale_date
        RANGE BETWEEN INTERVAL '6 DAY' PRECEDING AND CURRENT ROW
    ) AS seven_day_rolling_sum
FROM
    daily_sales;

```

| sale_date  | sale_amount |                                weekly_rolling_sum                                 |
|:----------:|:-----------:|:---------------------------------------------------------------------------------:|
| 2024-01-01 |   100.00    |                                      100.00                                       |
| 2024-01-02 |   150.00    |                                      250.00                                       |
| 2024-01-04 |    75.00    | 325.00 (Sums Jan 1, 2, 4. Jan 3 is missing in data but covered by the date range) |
| 2024-01-05 |   200.00    |           425.00 (Sums Jan 1, 2, 4, 5. Jan 1 is within 6 days of Jan 5)           |
| 2024-01-06 |    50.00    |                          475.00 (Sums Jan 1, 2, 4, 5, 6)                          |
| 2024-01-08 |   125.00    |        450.00 (Sums Jan 2, 4, 5, 6, 8. Jan 1 is outside the 6-day window)         |