## The summarize Function: Automated Business Intelligence

The `summarize` function is the crown jewel of `pysqdb`'s analytical tools. It automates the most tedious part of SQL: writing long `GROUP BY` clauses and complex join logic. By simply defining which columns you want to see and which calculations you want to perform, `pysqdb` generates and executes an optimized analytical query.

This function is designed to replace hours of manual SQL writing with a single, declarative Python dictionary structure.

**`ps.summarize(tables, on, join_types=None, columns=None, aggregations=None, order_by=None, create_table=None, create_view=None, return_df=True)`**

### Key Features

* **Auto-Group Logic:** Any non-aggregated column you select is automatically added to the `GROUP BY` clause.
* **Alias-Friendly Aggregations:** Define your metrics with clean names (e.g., `"total_revenue": "SUM(price)"`).
* **Multi-Table Intelligence:** Join as many tables as you need; `pysqdb` manages the table aliases and join conditions behind the scenes.
* **Integrated Sorting:** Sort your final dashboard by any metric or grouping column.

### Parameters

* **tables (list):** List of tables to join and analyze.
* **on (list):** Join keys or conditions between the tables.
* **columns (dict):** A mapping of `{table: [columns]}` to define your grouping dimensions.
* **aggregations (dict):** A mapping of `{alias: "SQL_FUNC"}` for your metrics.
* **order_by (dict):** Sorting directions (e.g., `{"total_sales": "DESC"}`).

---

### Usage Examples

**1. Building a Global Sales Dashboard**

Instead of writing 20 lines of SQL, define your dimensions and metrics in a clean dictionary.

```python
import pysqdb as ps

ps.connect("retail_warehouse")

# Create a summary of sales by category and region
summary_df = ps.summarize(
    tables=["sales", "products", "stores"],
    on=["product_id", "store_id"],
    columns={
        "products": ["category"],
        "stores": ["region"]
    },
    aggregations={
        "total_revenue": "SUM(amount)",
        "avg_transaction": "AVG(amount)",
        "order_count": "COUNT(order_id)"
    },
    order_by={"total_revenue": "DESC"}
)

```

**2. Advanced Performance Reporting**

Join tables and save the result as a "View" for your visualization tools, while keeping a sample in a DataFrame for immediate check.

```python
ps.summarize(
    tables=["employees", "performance"],
    on="emp_id",
    join_types="LEFT",
    columns={"employees": ["department", "job_title"]},
    aggregations={
        "avg_score": "AVG(rating)",
        "max_score": "MAX(rating)"
    },
    create_view="dept_performance_v"
)

```

---

### Why is this the "Ultimate" EDA Tool?

1. **Declarative Syntax:** You focus on **what** you want to calculate, not **how** to write the SQL syntax.
2. **Reduced Human Error:** Forget about "Column not in GROUP BY" errors. `pysqdb` ensures that the SQL is always syntactically perfect.
3. **Optimized Execution:** DuckDB's vectorized engine processes these aggregations at speeds that outperform Pandas by 10x-50x on large datasets.

---


## The window Function: Advanced Time-Series & Ranking

Window functions are essential for complex data analysis where you need to compare a row's value to other rows in the same dataset (e.g., comparing today's sales to yesterday's). The `window` function in `pysqdb` simplifies this by allowing you to define partitions, ordering, and frames using a clean dictionary-based interface.

This is your go-to tool for rolling averages, cumulative totals, percentiles, and row-over-row growth metrics.

**`ps.window(table, operations, partition_by=None, order_by=None, frame_clause=None, create_table=None, create_view=None, return_df=True)`**

### Key Features

* **Preserve Granularity:** Unlike `summarize`, this function keeps all your original rows and simply adds new calculated columns.
* **Rolling Analytics:** Easily implement moving windows with the `frame_clause`.
* **Smart Over Clause:** Automatically assembles complex SQL `OVER()` clauses from your Python parameters.
* **Seamless Ranking & Lags:** Calculate `RANK()`, `LEAD()`, or `LAG()` without writing nested subqueries.

### Parameters

* **table (str):** The source table or view.
* **operations (dict):** A mapping of `{alias: "SQL_FUNC"}` for the window calculations.
* **partition_by (str or list):** The columns to group by inside the window (restarts calculation for each group).
* **order_by (dict):** Defines the sequence of rows for metrics like running totals or lags.
* **frame_clause (str):** Specific boundaries for the calculation (e.g., `'ROWS BETWEEN 6 PRECEDING AND CURRENT ROW'`).

---

### Usage Examples

**1. Calculating Daily Growth & Rolling Averages**

In finance and e-commerce, seeing the trend is more important than seeing a single point.

```python
import pysqdb as ps

ps.connect("financial_data")

# Calculate daily change and 7-day moving average
time_series_df = ps.window(
    table="daily_revenue",
    operations={
        "yesterday_revenue": "LAG(revenue)",
        "rolling_7d_avg": "AVG(revenue)"
    },
    order_by={"date": "ASC"},
    frame_clause="ROWS BETWEEN 6 PRECEDING AND CURRENT ROW"
)

```

**2. Advanced Ranking & Percentiles**

Perfect for leaderboards or identifying high-value customer segments within each region.

```python
ps.window(
    table="user_activity",
    operations={
        "regional_rank": "RANK()",
        "activity_percentile": "PERCENT_RANK()"
    },
    partition_by="region",
    order_by={"total_score": "DESC"},
    create_view="user_rankings_v"
)

```


**3. Cumulative Sums (Running Totals)**

Track how your metrics accumulate over time without losing individual transaction details.

```python
ps.window(
    table="transactions",
    operations={"running_total": "SUM(amount)"},
    order_by={"timestamp": "ASC"},
    return_df=True
)

```

---

### Why use pysqdb's window function?

1. **Safety First:** Building `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...)` strings manually is a major source of SQL bugs. `pysqdb` builds this safely for you.
2. **Speed:** DuckDB's window function implementation is world-class, often outperforming equivalent Pandas `.rolling()` or `.expanding()` operations on larger-than-memory datasets.
3. **Data Integrity:** Because the function starts with `SELECT *`, you never lose context—your original data remains untouched, just enriched with new insights.

---



## Conclusion: The Analytical Powerhouse

By mastering the Summarization and Window functions, you have unlocked the full potential of `pysqdb`. You are now capable of transforming raw, messy data into high-level business intelligence and complex time-series insights without leaving the comfort of your Python environment.

These functions represent the bridge between raw data engineering and high-end data science, powered by the unmatched performance of the DuckDB engine.

## Future Roadmap: What’s Coming Next

`pysqdb` is a living project, and we are constantly working to add more specialized tools to your arsenal. Here is a sneak peek at what is currently in development:

### Advanced Time-Series Analysis

We are building dedicated functions to handle complex temporal data, including:

* **Smart Reformatting:** One-click conversion between disparate datetime formats.
* **Resampling & Interpolation:** High-speed grouping by custom time intervals (e.g., 5-minute bins or fiscal quarters).
* **Automated Period Comparisons:** Simplified functions for Year-over-Year (YoY) and Month-over-Month (MoM) reporting.

### Advanced Feature Engineering

To further support machine learning workflows, future updates will include:

* **Native One-Hot Encoding:** Fast categorical encoding directly within the SQL engine.
* **Scaling & Normalization:** SQL-based Min-Max and Z-score scaling for multi-billion row datasets.
* **String Manipulation Suite:** Advanced fuzzy matching and regex tools for cleaning text data at scale.

We are committed to making `pysqdb` the fastest and most intuitive data wrangling library in the Python ecosystem. Stay tuned for these updates!


