
# Welcome to pysqdb 🦆🚀

`pysqdb` is a powerful, minimalist package for your data workflows. It combines the speed of DuckDB's SQL engine with the flexibility of Python, allowing you to perform complex data manipulations and analyses with ease. Whether you're working with large datasets or need to quickly prototype data transformations, `pysqdb` provides a seamless, zero-copy experience.

## Installation

You can simply install `pysqdb` using pip:

```bash
pip install pysqdb

```

---

## Why `pysqdb`? (The "Show, Don't Tell" Test)

To understand why `pysqdb` exists, let's look at a common real-world Data Engineering & Analytics problem.

**🔥 The Problem:**
Imagine you have three massive tables: `sales`, `customers`, and `products`. You need to:

1. Join these three tables.
2. Group the data by the customer's `occupation`.
3. Calculate advanced window functions: A **7-day rolling average** of sales, the **cumulative sum** of sales, and a **sales rank** based on the date.

Let's see how this is traditionally solved, and how `pysqdb` changes the game.
### 🛑 Approach 1: Raw SQL (Too Much Manual Work & Repetition)
SQL is undeniably powerful and is the gold standard for databases. However, writing raw queries for complex analytics pipelines means typing endless `JOIN` conditions, repetitive `OVER (PARTITION BY...)` clauses, and managing massive Common Table Expressions (CTEs). It works perfectly, but it demands too much manual typing and boilerplate code.

```sql
WITH joined_data AS (
    SELECT c.occupation, s.sale_date, s.sales_amount
    FROM sales s
    INNER JOIN customers c ON s.customer_id = c.customer_id
    INNER JOIN products p ON s.product_id = p.product_id
)
SELECT 
    occupation,
    sale_date,
    sales_amount,
    AVG(sales_amount) OVER (
        PARTITION BY occupation 
        ORDER BY sale_date ASC 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg,
    SUM(sales_amount) OVER (
        PARTITION BY occupation 
        ORDER BY sale_date ASC
    ) AS cumulative_sales,
    DENSE_RANK() OVER (
        PARTITION BY occupation 
        ORDER BY sale_date ASC
    ) AS sales_rank
FROM joined_data;

```

### 🐢 Approach 2: Pandas (Memory Killer & Clunky Syntax)

Pandas requires loading all three massive datasets directly into your RAM. The syntax for chained `.groupby()`, `.rolling()`, and `.transform()` is notoriously hard to read and prone to memory crashes (Out of Memory - OOM).

```python
import pandas as pd

# 1. RAM Killer: Loading and merging massive datasets in memory
merged_df = sales.merge(customers, on='customer_id', how='inner')
merged_df = merged_df.merge(products, on='product_id', how='inner')

# 2. Clunky Syntax: Sorting is strictly required before rolling
merged_df = merged_df.sort_values(by=['occupation', 'sale_date'])

# 3. Ugly Transformations
merged_df['rolling_7d_avg'] = merged_df.groupby('occupation')['sales_amount'].transform(
    lambda x: x.rolling(window=7, min_periods=1).mean()
)
merged_df['cumulative_sales'] = merged_df.groupby('occupation')['sales_amount'].cumsum()
merged_df['sales_rank'] = merged_df.groupby('occupation')['sale_date'].rank(method='dense')

```

---

### ✨ Approach 3: The `pysqdb` Way (Zero-Copy, Elegant, Fast)

With `pysqdb`, you don't load massive datasets into RAM. You use our **Dictionary-Based Architecture** to define what you want. DuckDB handles the heavy lifting under the hood at C++ speed, and you only get the final Pandas DataFrame when the job is completely done.

```python
import pysqdb as ps

# Connect to database (or run in-memory)
ps.connect("my_database.duckdb")

# Step 1: Join tables into a ZERO-COPY virtual view. No RAM wasted!
ps.summarize(
    tables=["sales", "customers", "products"],
    join_types=["inner", "inner"],
    on=["customer_id", "product_id"],
    create_view="master_sales", 
    return_df=False
)

# Step 2: Advanced Analytics with elegant dictionary mapping
final_df = ps.window(
    table="master_sales",
    partition_by="occupation",
    order_by={"sale_date": "ASC"},
    frame_clause="ROWS BETWEEN 6 PRECEDING AND CURRENT ROW", # The 7-day frame
    
    operations={
        "rolling_7d_avg": "AVG(sales_amount)",
        "cumulative_sales": "SUM(sales_amount)",
        "sales_rank": "DENSE_RANK()"
    }
)

print(final_df)

```

### 📈 The Scaling Factor: When Things Get Complex

While the example above uses three tables, the true power of `pysqdb` shines in real-world production environments. When your pipeline scales to joining **5, 10, or more massive tables**, traditional approaches hit a wall:

* **Pandas** scripts crash with fatal Out-Of-Memory (OOM) errors.
* **Raw SQL** queries mutate into unreadable 500-line monoliths.

With `pysqdb`, scaling your pipeline simply means adding a few more strings to your `tables=["...", "..."]` array. Your codebase remains incredibly concise, and the complexity stays perfectly flat, no matter how massive your data architecture becomes.

> **"The declarative elegance of Python dictionaries, backed by the raw analytical muscle of DuckDB."**

--- 

### 🎯 Key Benefits of `pysqdb  :

* **Zero-Copy Architecture:** Create logical views instead of duplicating data in memory.
* **Readable Pipeline:** Say goodbye to endless pandas `.groupby().transform()` chains.
* **DuckDB Speed:** Powered by the fastest analytical SQL engine on the planet.
* **Modular:** Built with Software Engineering principles (Single Responsibility) for Data Scientists.

---

## 📚 Explore the Documentation

Ready to master your data pipelines? Dive into our comprehensive documentation to see detailed examples, parameters, and pro-tips for every single function in the `pysqdb` arsenal.

To keep things modular and easy to learn, we've organized the documentation into four core pillars:

1. **Core Operations & Data I/O**
* Managing DuckDB connections, importing dirty CSV/Excel/JSON files with Pandas fallback, and lightning-fast Parquet exports.


2. **Data Cleaning & Preprocessing**
* Smart missing value imputation, outlier clipping/removal, and duplicate handling.


3. **Relational Operations (Joins & Filters)**
* Memory-efficient table joins, filter-then-join pipelines, and dynamic table manipulations.


4. **Advanced Analytics**
* Building automated grouping dashboards (`summarize`) and our signature dictionary-based `window` functions.



**[Read the Full Documentation Here ➡️](https://vasif-asadov1.github.io/pysqdb/02_io_operations/)**


