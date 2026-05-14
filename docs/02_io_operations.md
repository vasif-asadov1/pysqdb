## Connection & Database Management

The first step in any `pysqdb` workflow is establishing a connection. Our `connect` function is designed to be smart and flexible, handling both persistent disk storage and ultra-fast in-memory processing.

### `ps.connect(db_name)`

This function initializes the global database connection. Everything you do afterwards (reading files, cleaning data, analytics) will happen within this active connection.

#### **How it works:**

* **Auto-Extension:** If you name your database `my_data`, `pysqdb` automatically appends `.duckdb` for you.
* **In-Memory Mode:** Want to work purely in RAM? Just use `:memory:`.
* **Active Session Management:** If you call `connect` again, it safely closes the previous connection before opening the new one, preventing memory leaks and locked files.

#### **Examples:**

**1. Persistent Database (Saves to disk)**
Ideal for long-term projects where you want your tables to stay there tomorrow.

```python
import pysqdb as ps

# Creates 'analytics_project.duckdb' in your current folder
sp.connect("analytics_project")

```

**2. In-Memory Database (Lightning Fast)**
Perfect for quick scripts, prototyping, or when you don't need to save the data permanently.

```python
import pysqdb as ps

# Runs entirely in RAM. Data is gone when the script ends.
sp.connect(":memory:")

```

---

## Importing Data (The `read` function)

Once connected, you need to bring your data into the DuckDB ecosystem. `pysqdb` provides a robust `read` function that handles different formats and, most importantly, **recovers from errors** where other libraries fail.

### `ps.read(path, file_type, table_name, strict=False)`

#### **Key Features:**

* **Auto-Formatting:** Supports `csv`, `json`, `parquet`, and `excel`.
* **Smart Fallback:** If DuckDB's C++ engine encounters a "dirty" CSV (like a malformed Kaggle dataset), `pysqdb` automatically switches to a **Pandas-powered Python engine** to rescue your data.
* **Strict Mode:** Set `strict=True` if you want the operation to fail on bad data lines.

#### **Usage:**

```python
# Reading a dirty CSV file
ps.read(path="data/movies.csv", file_type="csv", table_name="movies")

# Reading a Parquet file for high-performance analytics
ps.read(path="data/logs.parquet", file_type="parquet", table_name="system_logs")

```

---

## Exporting Data

After your analysis is complete, you can easily move your data out of the database and back into a file for sharing or reporting.

### `ps.export_table(table_name, export_format="csv", file_name=None)`

```python
# Export your cleaned table back to a CSV
ps.export_table(table_name="cleaned_movies", export_format="csv")

# Export to Parquet for use in other big data tools
ps.export_table(table_name="final_results", export_format="parquet", file_name="output_data")

```


---

# 🚀 The `DF` Function: Bridging SQL & Pandas

The `DF` function is your primary tool for retrieving data from the DuckDB engine into a Python-friendly **Pandas DataFrame**.

While `sqpy` handles heavy computations at C++ speeds within the database, `DF` allows you to pull specific slices of your data into RAM for final visualization, machine learning, or custom Python processing.

## `sp.DF(table, head=None, tail=None, sample=None, columns=None)`

Unlike a standard `SELECT *`, this function is optimized to fetch only what you need, saving memory and time.

### **Parameters:**

* **`table` (str):** The name of the table you want to query.
* **`head` (int, optional):** Returns the first $N$ rows (uses SQL `LIMIT`).
* **`tail` (int, optional):** Returns the last $N$ rows. **Note:** Since SQL tables are unordered by nature, this uses an optimized `OFFSET` calculation to find the end of the table.
* **`sample` (int, optional):** Returns $N$ random rows using DuckDB's native sampling engine—perfect for quick Exploratory Data Analysis (EDA).
* **`columns` (list or str, optional):** Specify exactly which columns you want to pull. Great for memory management.

---

### **Pro Usage Examples**

#### **1. Quick Data Inspection**

Need to see what's inside your `movies` table without crashing your session? Use `head` or `sample`.

```python
import pysqdb as ps

# Get the first 10 rows
df_head = ps.DF("movies", head=10)

# Get 50 random rows for a quick sanity check
df_sample = ps.DF("movies", sample=50)

```

#### **2. Memory-Efficient Column Selection**

If your table has 100 columns but you only need two, don't waste RAM.

```python
# Pull only 'title' and 'revenue' into a Pandas DataFrame
df_slim = ps.DF("movies", columns=["title", "revenue"], head=100)

```

#### **3. Fetching the "Tail"**

Useful for checking the most recent entries in a time-series or sorted table.

```python
# Get the last 5 rows of the table
df_last = ps.DF("reviews", tail=5)

```

---

### **💡 Pro Tip: Performance vs. Memory**

Remember that `pysqdb` is designed to handle **Big Data**. While `DF` is convenient, pulling millions of rows into a Pandas DataFrame can still lead to memory issues.

> **Best Practice:** Use `pysqdb`'s analytical functions (`summarize`, `filter`, `window`) to aggregate or reduce your data first, and use `DF` only when you are ready to work with the final, smaller result set.


---

# The `give_array` Function: Ready for Machine Learning

While Dataframes are great for manipulation, most Mathematical and Machine Learning libraries (like Scikit-learn, NumPy, and PyTorch) require data in the form of raw arrays.

The `give_array` function extracts data directly from your DuckDB tables and converts it into a high-performance **NumPy ndarray**, providing a direct pipeline from your database to your models.

## `sp.give_array(table, columns)`

This function bypasses the overhead of manual conversion, giving you a clean array of the features or targets you need.

### **Parameters:**

* **`table` (str):** The name of the source table.
* **`columns` (list or str):** The column or list of columns to be extracted. If a single string is provided, it is automatically converted to a list.

### **Returns:**

* **`numpy.ndarray`:** A NumPy array containing the values from the specified columns.

---

### **Pro Usage Examples**

#### **1. Extracting Features for Machine Learning ($X$ and $y$)**

Instead of manually slicing DataFrames, you can pull your feature matrix and target vector directly.

```python
import pysqdb as ps

# Connect and prepare your data
ps.connect("model_data")

# Extract features (X) as a NumPy array
X = ps.give_array("movies", columns=["budget", "runtime", "popularity"])

# Extract target (y)
y = ps.give_array("movies", columns="revenue")

# Now it's ready for Scikit-learn!
# model.fit(X, y)

```

#### **2. Fast Statistical Calculations**

NumPy is incredibly fast for vector operations. Use `give_array` when you need to perform heavy math outside of SQL.

```python
import numpy as np

# Get revenue as a 1D array
revenue_array = ps.give_array("movies", "revenue")

# Perform fast NumPy math
log_revenue = np.log1p(revenue_array)

```

---

### **💡 Why use `give_array` instead of `DF().to_numpy()`?**

1. **Syntactic Sugar:** It’s a cleaner, more readable one-liner in your scripts.
2. **Intent-Driven:** It signals that you are moving from the "Data Engineering" phase to the "Modeling/Math" phase.
3. **Consistency:** Handles the conversion of single column strings to lists internally, preventing common shape errors in NumPy.

---


# The `give_info` Function: Instant Data Profiling

Every great data project starts with understanding your data. The `give_info` function is your "all-in-one" diagnostic tool. It provides a high-level overview of your table's structure alongside deep statistical insights, helping you spot missing values, outliers, and distribution patterns instantly.

## `sp.give_info(table)`

Instead of running multiple Pandas commands like `.info()`, `.describe()`, and `.isnull().sum()`, `give_info` leverages DuckDB's native summarization engine to do it all in one go—even on datasets that are too large for RAM.

### **What it tells you:**

* **General Metadata:** Total row count and the number of columns.
* **Schema Details:** Every column name and its specific data type.
* **Statistical Summary:**
* **Range:** Min and Max values.
* **Central Tendency:** Average (Mean) and Median (q50).
* **Dispersion:** 25th (q25) and 75th (q75) percentiles.
* **Uniqueness:** `approx_unique` count to identify IDs or categorical data.
* **Data Quality:** `null_percentage` to identify where your data is "leaking."



---

### **Usage Example**

Imagine you just loaded a massive `sales` table and want to check its health before starting your analysis.

```python
import pysqdb as ps

# Connect and load data
ps.connect("ecommerce_db")

# Get the full statistical report
sales_info = ps.give_info("sales")

# View the report
print(sales_info)

```

**What the output looks like (Cleaned & Focused):**
The function returns a refined Pandas DataFrame containing only the most actionable insights:

| column_name | column_type | min | max | avg | q50 | null_percentage |
| --- | --- | --- | --- | --- | --- | --- |
| **order_id** | BIGINT | 1001 | 9999 | ... | ... | 0.0 |
| **revenue** | DOUBLE | 0.0 | 5400.5 | 120.4 | 85.0 | 2.1 |
| **category** | VARCHAR | ... | ... | ... | ... | 0.0 |

---

### **Why use `give_info` instead of Pandas `.describe()`?**

1. **Performance:** DuckDB computes these statistics using optimized C++ kernels. It is significantly faster than Pandas, especially on multi-million row tables.
2. **Built-in Quality Check:** It automatically calculates the `null_percentage`, which is the #1 thing a data scientist checks before cleaning.
3. **Smart Summarization:** It handles both numerical and categorical data types gracefully within the same report.


---

## The `import_df` Function: Bridging Pandas and DuckDB

While `pysqdb` excels at reading raw files, you often already have data in your Python session as a **Pandas DataFrame**. The `import_df` function provides a high-speed bridge to move that data into DuckDB.

Unlike a simple import, `pysqdb` allows you to slice your DataFrame (rows or columns) during the transfer, ensuring you only store the data you actually need.

## `ps.import_df(df, table_name, columns=None, rows=None)`

### Key Features

* **Smart Slicing:** Supports both integer-based (`iloc`) and label-based (`loc`) slicing for both rows and columns.
* **Memory Efficient:** Uses DuckDB's `register` mechanism to move data without unnecessary overhead.
* **Non-Destructive:** Performs operations on a copy, leaving your original DataFrame untouched in memory.
* **Dynamic Selection:** Pass a list for specific columns or a tuple for a range of columns.

### Parameters

* **df (pandas.DataFrame):** The source DataFrame.
* **table_name (str):** The name of the new table to create in DuckDB.
* **columns (list or tuple, optional):** * `["col1", "col3"]`: Selects specific columns.
* `(1, 5)`: Selects a range of columns by index.
* `("Name", "Salary")`: Selects a range of columns by label.


* **rows (list or tuple, optional):**
* `[0, 100]`: Imports the first 100 rows.
* `["id_100", "id_200"]`: Imports a specific range of index labels.



---

### Pro Usage Examples

#### 1. Direct Transfer of a Full DataFrame

The simplest way to move your work from Pandas to the SQL power of `sqpy`.

```python
import pandas as pd
import pysqdb as ps

# Your existing Pandas work
my_df = pd.read_csv("huge_dataset.csv")

ps.connect("my_analytics_db")

# Move the entire DF to DuckDB
ps.import_df(my_df, table_name="raw_sales")

```

#### 2. Importing a Specific Subset (Slicing)

If your DataFrame has 200 columns but you only need a specific range, you can slice it during the import.

```python
# Import only the first 500 rows and columns from 'Date' to 'Revenue'
ps.import_df(
    df=large_df,
    table_name="filtered_sales",
    rows=[0, 500],
    columns=("Date", "Revenue")
)

```

#### 3. Index-Based Column Selection

Perfect for datasets where you know the structure but not necessarily all the column names.

```python
# Import specific column indices: 0, 2, and 5
ps.import_df(large_df, "feature_table", columns=[0, 2, 5])

```

---

### Why use import_df instead of DuckDB's native register?

1. **Integrated Slicing:** You don't need to slice your DataFrame manually before importing. `pysqdb ` handles the `iloc`/`loc` logic internally.
2. **Persistence:** While DuckDB's `register` creates a temporary virtual link, `import_df` creates a **physical table** in your database, ensuring your data is saved even after you close the DataFrame.
3. **Safety:** It includes built-in error handling and unregisters temporary views automatically, keeping your database environment clean.

---



## The `copy_table` Function: Data Checkpointing

Before you perform destructive data cleaning operations—like deleting rows or dropping columns—it is a best practice to create a backup of your data. The `copy_table` function allows you to create an exact physical clone of any table in your database instantly.

Since this operation stays entirely within the DuckDB engine, copying a table with millions of rows is significantly faster than exporting and re-importing data.

## `ps.copy_table(table, copied_table)`

### Key Features

* **Full Data Persistence:** Unlike a view, this creates a physical duplicate of the data on your disk.
* **Instant Backups:** Perfect for creating "Checkpoints" before experimenting with complex transformations.
* **Non-Destructive Workflow:** Safely test your cleaning scripts on the copy while keeping the original source of truth intact.

### Parameters

* **table (str):** The name of the original source table.
* **copied_table (str):** The name for your new duplicated table.

---

### Pro Usage Examples

#### 1. Creating a Safety Backup

Always create a copy before running operations that permanently alter your table.

```python
import pysqdb as ps

ps.connect("production_db")

# Create a backup before cleaning
ps.copy_table(table="raw_users", copied_table="raw_users_backup")

# Now you can safely clean the original
ps.remove_rows("raw_users", "is_bot == True")

```

#### 2. Creating a Staging Table

Use copying to move data from a raw state to a staging state where you can perform feature engineering.

```python
# Create a staging table for feature engineering
ps.copy_table("sales_data", "stg_sales_features")

# Apply transformations to the staging table
ps.clip_outliers("stg_sales_features", "amount")

```

---

### Why use copy_table?

1. **Safety First:** It provides a "Undo" button for your database. If a complex cleaning script goes wrong, you can simply restore from your copy.
2. **Performance:** DuckDB uses highly optimized block-level copying, making this one of the fastest operations in the library.
3. **Workflow Organization:** It helps you maintain a clear lineage of your data (e.g., `raw` -> `backup` -> `clean`).

---



## Conclusion: You've Laid the Foundation!

You have just mastered the core DNA of `pysqdb`. By reaching this point, you now have the power to:

* **Bridge the Gap:** Seamlessly connect Python to the lightning-fast world of DuckDB.
* **Handle Dirty Data:** Load files that usually crash other libraries, thanks to our robust fallback mechanisms.
* **Profile Instantly:** Get a deep statistical "X-ray" of your tables without writing long, repetitive SQL or Pandas code.
* **Prepare for ML:** Convert database tables directly into NumPy arrays, ready for your next big model.

### **What's Next?**

Data in the real world is rarely perfect. It’s messy, contains nulls, and has outliers that can ruin your analysis.

In the next section, **"Data Cleaning & Preprocessing"**, we will move from just *handling* data to *perfecting* it. We'll explore how to handle missing values, eliminate duplicates, and clip outliers with single-function elegance.



