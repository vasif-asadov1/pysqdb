
# pysqdb: The Elegance of Python, The Muscle of DuckDB

![PyPI - Version](https://img.shields.io/pypi/v/pysqdb?color=blue&style=flat-square)
![Python - Version](https://img.shields.io/pypi/pyversions/pysqdb?style=flat-square)
![Powered by DuckDB](https://img.shields.io/badge/Powered%20by-DuckDB-FFAA00?style=flat-square&logo=duckdb&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)

**`pysqdb`** is a powerful, minimalist Data Engineering and EDA toolkit. It combines the blazing-fast execution of DuckDB's SQL engine with the declarative flexibility of Python dictionaries, allowing you to perform complex data manipulations with zero-copy efficiency. 

Whether you're working with massive out-of-core datasets or rapidly prototyping data transformations, `pysqdb` is designed to be your pipeline's high-performance bridge.

---

## Installation

Install `pysqdb` via pip. It's lightweight and ready to go:

```bash
pip install pysqdb

```

---

## Why `pysqdb`? (The "Show, Don't Tell" Test)

To understand why `pysqdb` exists, let's look at a common real-world Data Engineering problem.

**The Scenario:**
You have three massive tables: `sales`, `customers`, and `products`. Your objective is to:

1. Join these three tables.
2. Group the data by the customer's `occupation`.
3. Calculate advanced window functions: A **7-day rolling average** of sales, the **cumulative sum** of sales, and a **sales rank** based on the date.

Let's look at the traditional approaches, and how `pysqdb` rewrites the rules.

### Approach 1: Raw SQL (Boilerplate Hell)

SQL is the gold standard, but writing complex pipelines requires endless `JOIN` conditions and repetitive `OVER (PARTITION BY...)` clauses. It's powerful, but it demands too much manual typing and produces monolithic code.

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
    AVG(sales_amount) OVER (PARTITION BY occupation ORDER BY sale_date ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg,
    SUM(sales_amount) OVER (PARTITION BY occupation ORDER BY sale_date ASC) AS cumulative_sales,
    DENSE_RANK() OVER (PARTITION BY occupation ORDER BY sale_date ASC) AS sales_rank
FROM joined_data;

```

### Approach 2: Pandas (The Memory Killer)

Pandas forces you to load all massive datasets directly into RAM. The syntax for chained `.groupby()`, `.rolling()`, and `.transform()` is notoriously clunky and highly susceptible to Out-Of-Memory (OOM) crashes.

```python
import pandas as pd

# 1. RAM Killer: Loading and merging massive datasets in-memory
merged_df = sales.merge(customers, on='customer_id').merge(products, on='product_id')

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

### Approach 3: The `pysqdb` Way (Elegant, Zero-Copy, Fast)

With `pysqdb`, you don't load massive datasets into RAM. You use our **Dictionary-Based Architecture** to simply declare what you want. DuckDB handles the heavy lifting at C++ speed under the hood, and you only retrieve the final Pandas DataFrame when the job is done.

```python
import pysqdb as pdb

# Connect to the database (or run completely in-memory)
pdb.connect("my_database.duckdb")

# Step 1: Join tables into a ZERO-COPY virtual view. No RAM wasted!
pdb.summarize(
    tables=["sales", "customers", "products"],
    join_types=["inner", "inner"],
    on=["customer_id", "product_id"],
    create_view="master_sales", 
    return_df=False
)

# Step 2: Advanced Analytics with elegant dictionary mapping
final_df = pdb.window(
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

```

---

## The Scaling Factor

The true power of `pysqdb` shines in real-world production environments. When your pipeline scales to joining **10+ massive tables**, traditional approaches hit a brick wall:

* **Pandas** scripts crash with fatal Out-Of-Memory (OOM) errors.
* **Raw SQL** queries mutate into unmaintainable 500-line monsters.

With `pysqdb`, scaling simply means adding strings to your `tables=["...", "..."]` array. Your codebase remains incredibly concise, and the complexity stays perfectly flat.

> *"The declarative elegance of Python dictionaries, backed by the raw analytical muscle of DuckDB."*

---

## Key Benefits

* **Zero-Copy Architecture:** Create logical views instead of duplicating massive data in memory.
* **Readable Pipelines:** Say goodbye to endless pandas `.groupby().transform()` chains.
* **DuckDB Speed:** Powered by the fastest analytical SQL engine on the planet.
* **Modular Engineering:** Built with Software Engineering principles (Single Responsibility) tailored for Data Scientists.

---

## Documentation

Ready to master your data pipelines? Dive into our comprehensive documentation to explore detailed examples, parameters, and pro-tips for every function in the `pysqdb` arsenal.

1. **[Core Operations & Data I/O](https://www.google.com/search?q=%23)** - *Managing connections, fallback imports, and Parquet exports.*
2. **[Data Cleaning & Preprocessing](https://www.google.com/search?q=%23)** - *Smart imputation, outlier clipping, and duplicate handling.*
3. **[Relational Operations](https://www.google.com/search?q=%23)** - *Memory-efficient joins, filter-then-join pipelines, and schema management.*
4. **[Advanced Analytics](https://www.google.com/search?q=%23)** - *Automated grouping and signature dictionary-based window functions.*

👉 **[Read the Full Documentation Here](https://www.google.com/search?q=%23)** *(Link to MkDocs will be updated shortly)*


