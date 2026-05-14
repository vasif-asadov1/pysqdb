# Data Cleaning Operations

The `Data Cleaning` pillar is where your raw, messy data gets transformed into a polished, analysis-ready asset. This section includes functions for handling missing values, outliers, duplicates, and even physical data ordering. Each function is designed to operate directly within the DuckDB engine, ensuring that you can clean datasets of any size without worrying about memory constraints.

## The `impute` Function

Missing data (NULLs) can bias your analysis and crash your machine learning models. The `impute` function provides an elegant, SQL-native way to fill these gaps using statistical measures or fixed constants.

Unlike Pandas, which often creates a copy of the entire dataset in RAM, `pysqdb` performs an **In-Place Update** directly within the database. This means you can clean multi-gigabyte tables without breaking a sweat.

## `ps.impute(table, column, method="mean", fixed_const=None, return_df=False)`

### **How it works:**

The function analyzes the column, calculates the requested statistic (mean, median, or mode), and runs a highly optimized `UPDATE` query to fill only the missing values.

### **Parameters:**

* **`table` (str):** The name of the table to update.
* **`column` (str):** The specific column containing NULL values.
* **`method` (str):**
* `"mean"`: Fills with the mathematical average (Numeric only).
* `"median"`: Fills with the middle value (Numeric only).
* `"mode"`: Fills with the most frequent value (Great for Categorical data).
* `"fixed_const"`: Fills with a specific value provided in `fixed_const`.


* **`fixed_const` (any, optional):** Required only if `method="fixed_const"`. Includes built-in type checking (e.g., won't let you put a string in a numeric column).
* **`return_df` (bool):** If `True`, returns the updated table as a Pandas DataFrame for immediate inspection.

---

### **Pro Usage Examples**

#### **1. Filling Numerical Gaps with the Average**

Perfect for columns like `age` or `price` where the mean is a safe statistical bet.

```python
import pysqdb as ps

# Replace NULLs in 'budget' with the average budget of all movies
ps.impute(table="movies", column="budget", method="mean")

```

#### **2. Handling Categorical Data with a Fixed Constant**

When a value is missing in a category, you might want to label it as "Unknown".

```python
# Fills NULL genres with the string "Unknown"
# pysqdb automatically validates that 'genre' is a string-type column!
ps.impute(table="movies", 
          column="genre", 
          method="fixed_const", 
          fixed_const="Unknown")

```

#### **3. Using the Mode (Most Frequent)**

For features like `country` or `language`, using the most common value is often the best strategy.

```python
ps.impute(table="movies", column="original_language", method="mode")

```

---

### **Built-in Type Safety (The "SqPy" Shield)**

Your code includes a rigorous validation layer. If you try to impute a numeric column with a string, `pysqdb` catches it before the database throws an error:

```python
# This will raise a TypeError: Column 'revenue' is numeric. 'fixed_const' cannot be a string.
ps.impute(table="movies", column="revenue", method="fixed_const", fixed_const="None")
```


### **Pro Tip: When to use `return_df=True`?**

In large production pipelines, keep `return_df=False` (default) to keep the memory footprint at zero. Use `return_df=True` only during EDA (Exploratory Data Analysis) in a Jupyter Notebook to see the results immediately.

---



# The `remove_outliers` Function: Precision Cleaning

Extreme values (outliers) can heavily distort your statistical averages and mislead your machine learning models. The `remove_outliers` function uses the industry-standard **IQR (Interquartile Range)** method to identify and surgically remove these anomalies directly from your SQL tables.

By leveraging DuckDB's native quantile functions, `pysqdb` performs these complex calculations at blazing speeds without ever needing to load the full dataset into your computer's memory.

## `ps.remove_outliers(table, columns, return_df=False)`

### **The Math Behind the Magic:**

The function calculates the 25th percentile ($Q1$) and the 75th percentile ($Q3$) for each column. It then defines the "Normal Range" as:

* **Lower Bound:** $Q1 - 1.5 \times IQR$
* **Upper Bound:** $Q3 + 1.5 \times IQR$
Any data point outside these bounds is automatically deleted from the table.

### **Parameters:**

* **`table` (str):** The name of the table to clean.
* **`columns` (str or list):** A single column name or a list of numerical columns to check.
* **`return_df` (bool):** If `True`, returns the updated table as a Pandas DataFrame.

---

### **Pro Usage Examples**

#### **1. Cleaning a Single Column**

Perfect for removing impossible values in columns like `price` or `delivery_time`.

```python
import pysqdb as ps

# Remove extreme outliers from the revenue column
ps.remove_outliers(table="sales", columns="revenue")

```

#### **2. Batch Cleaning Multiple Features**

You can clean an entire set of numerical features in one line. `pysqdb` will automatically skip any non-numerical columns you might have accidentally included and warn you about them.

```python
# Clean multiple columns at once
ps.remove_outliers(table="movies", columns=["budget", "runtime", "popularity"])

```

#### **3. Real-Time Inspection**

In a Jupyter Notebook, you might want to see how many rows are left after the cleanup.

```python
# Clean and immediately see the resulting DataFrame
clean_df = ps.remove_outliers("inventory", "stock_count", return_df=True)
print(f"Remaining rows: {len(clean_df)}")

```

---

### **Smart Safeguards**

Your code isn't just fast; it's smart. It includes internal checks to prevent common errors:

* **Categorical Detection:** If you pass a column containing text (like `movie_title`), `pysqdb` will issue a warning and skip it rather than crashing.
* **Null Safety:** It automatically handles columns with missing values by ignoring `NULL` entries during the quantile calculation.

### **Pro Tip: Use with Caution!**

Outlier removal is a permanent `DELETE` operation in your database.

> **Best Practice:** Always run `ps.give_info(table)` before and after removing outliers to see how the distribution (Min/Max/Mean) has shifted!

---


# The `clip_outliers` Function: Soft-Cleaning (Winsorization)

Sometimes, deleting data is too aggressive. When you want to keep all your rows but need to "tame" extreme values that skew your model, `clip_outliers` is your best friend.

Instead of removing rows, this function **winsorizes** them: it sets values above the upper bound to the maximum "normal" value and values below the lower bound to the minimum "normal" value.

## `ps.clip_outliers(table, columns, return_df=False)`

### **How it works:**

Using the **IQR method** ($Q1 - 1.5 \times IQR$ and $Q3 + 1.5 \times IQR$), `pysqdb` identifies the normal boundaries. It then uses a highly efficient SQL `UPDATE` combined with `LEAST` and `GREATEST` functions to compress extreme values into this range.

### **Parameters:**

* **`table` (str):** The table to be updated.
* **`columns` (str or list):** The numerical column(s) to be clipped.
* **`return_df` (bool):** If `True`, returns the updated table as a DataFrame.

---

### **Pro Usage Examples**

#### **1. Preserving Data Size**

If you have a small dataset and cannot afford to lose any rows, use `clip` instead of `remove`.

```python
import pysqdb as ps

# Keep all rows, but cap the 'salary' column at statistical bounds
ps.clip_outliers(table="employees", columns="salary")

```

#### **2. Preparing Features for Linear Models**

Linear regression and many neural networks are sensitive to the magnitude of outliers. Clipping helps them converge faster and produce more stable coefficients.

```python
# Clip outliers in multiple features simultaneously
ps.clip_outliers("housing_data", ["square_feet", "lot_size", "price"])

```

#### **3. Handling Extreme Percentiles**

Since this function updates the table **in-place**, it is incredibly memory-efficient for large-scale data preprocessing.

```python
# Clip and check the new Max/Min values
ps.clip_outliers("sensor_readings", "temperature")
ps.give_info("sensor_readings")

```

---

### **Technical Brilliance: The SQL Logic**

Under the hood, `pysqdb` executes a sophisticated SQL update:

```sql
UPDATE table SET column = LEAST(GREATEST(column, lower_bound), upper_bound)

```

This ensures that:

* Values **lower** than the bound are raised to the `lower_bound`.
* Values **higher** than the bound are lowered to the `upper_bound`.
* Values already in the **normal range** remain untouched.

### **Pro Tip: When to "Remove" vs "Clip"?**

* **Remove:** When the outliers are likely data entry errors or "noise" that shouldn't exist.
* **Clip:** When the outliers are real data points but their extreme magnitude would negatively dominate your statistical model.

---


# The `show_duplicates` Function: The Data Detective

Before you delete data, you need to see it. While standard tools often provide a simple count of duplicates, `show_duplicates` performs a deep forensic analysis of your table. It identifies **every single occurrence** of identical rows, groups them together, and labels them so you can understand exactly where your data pipeline is leaking.

By using DuckDB’s advanced window functions and the `QUALIFY` clause, `pysqdb` identifies duplicates across all columns simultaneously without the need for complex self-joins.

## `ps.show_duplicates(table)`

### **How it works:**

The function partitions the data by **every single column** in your table. It then calculates a dynamic `_duplicate_count` for each row. If a row appears more than once, it is captured and displayed alongside its "twins."

### **What it returns:**

* **`pandas.DataFrame`:** A dataframe containing only the rows that have at least one exact duplicate.
* **`_duplicate_count`:** A temporary column added to the result showing how many times that specific row exists in the original table.

---

### **Pro Usage Examples**

#### **1. Identifying Data Integrity Issues**

Perfect for checking if a data ingestion script accidentally ran twice or if a join operation created a "Cartesian product" mess.

```python
import pysqdb as ps

# Connect to your database
ps.connect("production_db")

# Find and inspect all duplicates in the 'orders' table
duplicates_df = ps.show_duplicates("orders")

# If duplicates exist, they will be sorted so you can compare them row-by-row
print(duplicates_df.head(10))

```

#### **2. Quick Sanity Check Before Analysis**

Use this as a gatekeeper function. If `show_duplicates` returns an empty dataframe, you know your dataset is unique and ready for modeling.

```python
# A clean table returns an empty DF and a helpful message
ps.show_duplicates("customers")
# Output: No duplicates found in table 'customers'.

```

---

### **Technical Brilliance: The `QUALIFY` Clause**

Your code utilizes one of DuckDB's most powerful features:

```sql
SELECT *, COUNT(*) OVER (PARTITION BY ...) as _duplicate_count 
FROM table 
QUALIFY _duplicate_count > 1

```

This is significantly more efficient than the traditional SQL method (which requires a `GROUP BY` and a `JOIN` back to the original table). It allows `sqpy` to filter the results of a window function in a single pass over the data.

### **Pro Tip: Inspection vs. Action**

* Use `show_duplicates` to **understand** the scope of the problem.
* Once you’ve confirmed the duplicates are indeed errors, you can proceed to use our removal functions (like `drop_duplicates`) to sanitize the table permanently.

---



# The `remove_duplicates` Function: Final Table Sanitization

After inspecting your data with `show_duplicates`, it’s time to take action. The `remove_duplicates` function is designed to permanently clean your tables by eliminating redundant data.

Whether you want to keep exactly one copy of every record or completely wipe out any row that has a "twin," `pysqdb` handles the operation directly within the database engine for maximum performance.

## `ps.remove_duplicates(table, keep_first=False, keep_last=False, return_df=False)`

### **The Two Cleaning Strategies:**

1. **Keep None (`keep_first=False`, `keep_last=False`):** This is the "Nuclear Option." It identifies rows that are duplicated and removes **all** occurrences of them, leaving only the rows that were truly unique from the start.
2. **Keep One (`keep_first=True` OR `keep_last=True`):** This is the "Standard Clean." It uses SQL `DISTINCT` logic to ensure that every unique row appears exactly once in your final table.

### **Parameters:**

* **`table` (str):** The name of the table to clean.
* **`keep_first` / `keep_last` (bool):** Set one of these to `True` if you want to keep a single copy of each row.
* **`return_df` (bool):** If `True`, returns the cleaned table as a DataFrame.

---

### **Pro Usage Examples**

#### **1. The Standard Cleanup (DISTINCT)**

The most common use case: you have some redundant rows due to multiple data imports and you want a clean, unique table.

```python
import pysqdb as ps

# Keeps exactly one copy of every unique row
ps.remove_duplicates(table="customers", keep_first=True)

```

#### **2. The Strict Cleanup (Keep Only Truly Unique Rows)**

Use this when duplicates are considered "errors" and you don't trust any version of a duplicated row.

```python
# Removes all rows that appeared more than once.
# If 'User_A' appeared twice, 'User_A' is completely gone.
ps.remove_duplicates(table="transactions", keep_first=False)

```

#### **3. Clean and Inspect**

Verify your row count immediately after the cleanup.

```python
# Clean and return as DataFrame
df_clean = ps.remove_duplicates("web_logs", keep_first=True, return_df=True)
print(f"Dataset size after cleaning: {len(df_clean)}")

```

---

### **Technical Brilliance: Why `CREATE OR REPLACE`?**

Your code uses a very efficient SQL pattern:

```sql
CREATE OR REPLACE TABLE table AS SELECT DISTINCT * FROM table

```

By recreating the table within DuckDB, we avoid the overhead of individual `DELETE` statements. This "Bulk-Update" approach is significantly faster and results in a more compressed, optimized database file on your disk.

### **Conclusion: Data Cleaning Checklist**

Now that you've mastered the Cleaning Pillar, your typical workflow should look like this:

1. **`give_info()`**: Spot the NULLs and outliers.
2. **`impute()`**: Fill the missing gaps.
3. **`clip_outliers()` / `remove_outliers()`**: Tame the extreme values.
4. **`show_duplicates()`**: Inspect for redundancies.
5. **`remove_duplicates()`**: Finalize and seal the table.

---


## The sort_table Function: Physical Data Ordering

In data analysis, order matters. Whether you are preparing a dataset for time-series processing or simply want to organize your records by a specific metric, the `sort_table` function provides a permanent solution.

Unlike temporary sorting in a DataFrame, `sqpy` re-organizes the data physically within the DuckDB engine. This ensures that any subsequent query, view, or export will respect the new sorted order without needing to re-apply the logic.

## `sp.sort_table(table, by, ascending=True, return_df=False)`

### Key Features

* **Physical Persistance:** Uses `CREATE OR REPLACE` logic to rewrite the table in the specified order.
* **Multi-Column Sorting:** Sort by a single column or a prioritized list of multiple columns.
* **Mixed Sorting Directions:** Pass a list of booleans to sort some columns ascending and others descending in a single operation.
* **In-Memory Optimization:** Leverages DuckDB’s high-performance sorting algorithms to handle massive tables efficiently.

### Parameters

* **table (str):** The name of the table to be sorted.
* **by (str or list):** The column(s) that define the new order.
* **ascending (bool or list):** Set `True` for ASC (default) or `False` for DESC. Can be a list matching the `by` parameter.
* **return_df (bool):** If `True`, returns the freshly sorted table as a Pandas DataFrame.

---

### Pro Usage Examples

#### 1. Sorting by a Single Column (Chronological Order)

Re-order your logs so that the most recent events appear at the end of the table.

```python
import pysqdb as ps

ps.connect("system_logs")

# Sort the table by date and physically update it
ps.sort_table(table="logs", by="timestamp", ascending=True)

```

#### 2. Advanced Multi-Level Sorting

Sort a product catalog first by category (ascending) and then by price (descending) to find the most expensive items in each group.

```python
# Sort by Category (ASC) and Price (DESC)
ps.sort_table(
    table="products", 
    by=["category", "price"], 
    ascending=[True, False],
    return_df=True
)

```

#### 3. Preparing Data for Reporting

Physically sort your sales data before exporting it to ensure the final CSV or Excel file is perfectly organized for your stakeholders.

```python
ps.sort_table("quarterly_sales", by="total_revenue", ascending=False)
ps.export_table("quarterly_sales", "csv", file_name="sales_report_2024")

```

---

### Why use sort_table instead of just SQL ORDER BY?

1. **Efficiency:** By physically sorting the table, you pay the performance cost once. Future operations like filtering or window functions can often run faster on ordered data.
2. **Simplified Workflow:** You don't need to append an `ORDER BY` clause to every single analysis script; the database "remembers" the correct order.
3. **Consistency:** When pulling samples with `sp.DF(head=10)`, you are guaranteed to get the top results based on your defined sorting logic.
4. **Optimized Exports:** When exporting to CSV or Excel, the data will already be in the desired order without needing to sort it again in memory.



---

## The remove_columns Function: Streamlining Your Schema

As your analysis progresses, you often find yourself with redundant or unnecessary columns that clutter your workspace and consume memory. The `remove_columns` function is designed to surgically remove these elements from your tables or views.

It leverages DuckDB’s advanced `SELECT * EXCLUDE` logic, allowing you to drop specific columns without the need to manually list every other column you wish to keep.

## `ps.remove_columns(target, columns, new_name=None, return_df=False)`

### Key Features

* **Smart Target Detection:** Automatically identifies whether you are working with a physical `TABLE` or a logical `VIEW` and applies the correct SQL strategy.
* **In-Place Table Modification:** For tables, it uses `ALTER TABLE` to drop columns instantly without rewriting the entire dataset.
* **Safe View Evolution:** Since standard SQL views cannot be "partially dropped," `pysqdb` creates a new, refined view for you.
* **Advanced Exclusion:** Uses the high-performance `EXCLUDE` syntax to maintain code readability and execution speed.

### Parameters

* **target (str):** The name of the table or view to modify.
* **columns (str or list):** A single column name or a list of names to be removed.
* **new_name (str, optional):** If provided, `pysqdb` creates a new object instead of modifying the existing one (recommended for versioning).
* **return_df (bool):** If `True`, returns the updated schema as a Pandas DataFrame.

---

### Pro Usage Examples

#### 1. Dropping Sensitive Data In-Place

Quickly remove columns like `email` or `phone_number` from a physical table to ensure data privacy before sharing the database.

```python
import pysqdb as ps

ps.connect("user_database")

# Permanently removes columns from the table
ps.remove_columns(target="users", columns=["email", "phone_hash"])

```

#### 2. Refining a View for Visualization

Create a cleaner version of an existing view by excluding technical metadata columns that aren't needed for your dashboard.

```python
# Creates 'sales_report_edited' excluding the raw ID columns
ps.remove_columns(
    target="sales_report", 
    columns=["raw_log_id", "internal_status"],
    new_name="clean_sales_view"
)

```

#### 3. Batch Removal with Schema Inspection

Remove multiple columns at once and immediately verify the remaining schema.

```python
# Remove a list of columns and see the result
df_remaining = ps.remove_columns(
    target="survey_results", 
    columns=["temp_id", "session_token", "ip_address"],
    return_df=True
)
print(df_remaining.columns)

```

---

### Why use remove_columns?

1. **Efficiency:** Dropping columns from a table using `ALTER TABLE` is a metadata-only operation in DuckDB, meaning it happens almost instantly regardless of the table size.
2. **Readability:** Instead of writing a `SELECT` statement with 50 columns just to remove 2, you use the `EXCLUDE` power to keep your code clean and maintainable.
3. **Safety:** By supporting the `new_name` parameter, `pysqdb` encourages a non-destructive workflow where you can create refined versions of your data while keeping the original source intact.

---





## The remove_rows Function: Targetted Data Elimination

While filtering is used to select data you want to keep, `remove_rows` is designed for the surgical removal of data you want to discard. Whether you are purging test records, deleting canceled orders, or removing data that falls outside of a specific timeframe, this function handles the operation with precision and speed.

It automatically handles the logic reversal for you: when you provide the removal criteria, `pysqdb` ensures that only the remaining data is preserved in your tables or views.

## `ps.remove_rows(target, criteria, new_name=None, return_df=False)`

### Key Features

* **In-Place Purging:** For physical tables, it uses the high-speed SQL `DELETE` command to remove rows instantly without creating copies.
* **Non-Destructive Options:** Use the `new_name` parameter to create a "clean" copy of your data while keeping the original table intact.
* **Logical View Filtering:** Since you cannot delete from a view, `pysqdb` creates a refined view that filters out the unwanted rows using a `WHERE NOT` clause.
* **Smart Syntax Correction:** Just like our other filtering tools, it automatically converts Python's `==` to SQL's `=` for a seamless coding experience.

### Parameters

* **target (str):** The name of the table or view.
* **criteria (str or list):** The condition(s) defining the rows you want to **remove**. Multiple conditions in a list are combined with `AND`.
* **new_name (str, optional):** The name for the new table or view. If omitted for a table, the deletion is permanent (in-place).
* **return_df (bool):** If `True`, returns the resulting dataset as a Pandas DataFrame.

---

### Pro Usage Examples

#### 1. Cleaning Up Transactional Errors

Quickly remove records that were created by mistake or during system testing.

```python
import pysqdb as ps

ps.connect("business_data")

# Permanently delete test entries from the table
ps.remove_rows(target="transactions", criteria="status == 'test_entry'")

```

#### 2. Creating a Clean View without Outdated Data

Instead of deleting history, create a new view that only shows active data by removing records from previous years.

```python
# Create a new view that excludes rows from 2022
ps.remove_rows(
    target="annual_report", 
    criteria="year < 2023",
    new_name="modern_report_v"
)

```

#### 3. Batch Removal with Multiple Conditions

Combine several rules to wipe out irrelevant data points in one command.

```python
# Remove rows matching all criteria in the list
ps.remove_rows(
    target="leads", 
    criteria=[
        "is_unsubscribed == True",
        "last_contact_date < '2023-01-01'",
        "lead_source == 'cold_call'"
    ]
)

```

---

### Why use remove_rows?

1. **Intuitive Logic:** Sometimes it is easier to define what you *don't* want rather than what you *do*. This function lets you think in terms of "trash removal."
2. **Performance:** For physical tables, `DELETE` operations in DuckDB are highly optimized, making it much faster than reading a whole table into Pandas and filtering it manually.
3. **Database Integrity:** By providing a unified interface for both tables and views, `pysqdb` ensures that your data manipulation remains consistent regardless of the underlying storage type.



---

## The rename_columns Function: Multi-Table Schema Alignment

In large projects, maintaining consistent column names across different tables is vital for clean joins and readable code. The `rename_columns` function allows you to perform bulk renaming operations across multiple physical tables in a single command.

It is designed with maximum flexibility in mind, accepting various input formats and automatically validating your database schema to prevent accidental errors on logical views.

## ps.rename_columns(operations)

### Key Features

* **Bulk Processing:** Rename columns in multiple tables simultaneously by passing a structured dictionary.
* **Format Flexibility:** Accepts simple lists `["old", "new"]`, dictionaries `{"old": "new"}`, or lists of lists `[["old", "new"], ...]`.
* **View Protection:** Automatically detects and skips views, ensuring that structural changes are only applied to physical data layers.
* **Safe Execution:** Includes internal error handling to report missing columns or tables without crashing your entire pipeline.

### Parameters

* **operations (dict):** A dictionary where keys are table names and values are the renaming rules.

---

### Pro Usage Examples

#### 1. Standardizing IDs Across Tables

Ensure that your primary keys follow a consistent naming convention across your entire database.

```python
import pysqdb as ps

ps.connect("production_db")

# Standardize ID columns in multiple tables at once
ps.rename_columns({
    "users": ["id", "user_id"],
    "orders": ["id", "order_id"],
    "products": ["id", "product_id"]
})

```

#### 2. Multi-Column Renaming in a Single Table

Use a dictionary mapping to update several column names for a specific table in one go.

```python
# Pass a dictionary for multiple changes in the 'sales' table
ps.rename_columns({
    "sales": {
        "revenue_usd": "revenue",
        "trans_date": "date",
        "cust_name": "customer_name"
    }
})

```

#### 3. Handling List-of-Lists Format

If your renaming rules are generated dynamically, `pysqdb` can handle lists of pairs effortlessly.

```python
# List of lists format for dynamic operations
dynamic_renames = [["old_f1", "feature_1"], ["old_f2", "feature_2"]]

ps.rename_columns({"ml_features": dynamic_renames})

```

---

### Why use rename_columns?

1. **Automation:** Instead of writing dozens of `ALTER TABLE` statements manually, you define your mapping once and let `pysqdb` handle the iteration.
2. **Standardization:** It encourages a "Schema-First" mindset, helping you keep your database organized as it grows.
3. **Safety:** By skipping views and providing clear warnings, the function ensures that you don't break logical layers that depend on specific underlying column names.

---



## The `rename_tables` Function: Global Asset Refactoring

As projects grow, your naming conventions might evolve. What started as `raw_data` might need to become `stg_sales_2024`. The `rename_tables` function allows you to perform bulk renaming of both physical tables and logical views using a single dictionary mapping.

It automatically detects the asset type and applies the correct SQL logic, ensuring your database remains organized and your table names stay meaningful.

## `ps.rename_tables(operations)`

### Key Features

* **Dual-Type Support:** Seamlessly renames both physical `TABLES` and virtual `VIEWS` without requiring the user to specify the type.
* **Bulk Refactoring:** Update your entire database schema in one command by passing multiple old-to-new name pairs.
* **Metadata Integrity:** Renaming via `ALTER` is a metadata operation, meaning it happens instantly regardless of how many millions of rows are in the table.
* **Safe Skipping:** Includes internal checks to warn you if a source table doesn't exist, preventing script crashes during batch operations.

### Parameters

* **operations (dict):** A dictionary mapping old names (keys) to new names (values).

---

### Pro Usage Examples

#### 1. Batch Versioning

Quickly transition your production tables to a new versioning scheme.

```python
import pysqdb as ps

ps.connect("analytics_db")

# Rename multiple tables to include a version tag
ps.rename_tables({
    "customers": "customers_v1",
    "orders": "orders_v1",
    "inventory_view": "inventory_v1_view"
})

```

#### 2. Cleaning Up Temporary Assets

Rename your experimental or temporary tables to a standardized "archive" format.

```python
# Moving temporary workspace tables to a backup naming format
ps.rename_tables({
    "test_results": "archive_test_results_may",
    "temp_join_result": "backup_join_result"
})

```

---

### Why use rename_tables?

1. **Syntactic Simplicity:** You don't need to remember if the command is `ALTER TABLE` or `ALTER VIEW`. `pysqdb` inspects the `information_schema` and handles the syntax for you.
2. **Speed:** Because this is a metadata-only change, there is zero data movement. It is the fastest way to reorganize your data warehouse.
3. **Pipeline Flexibility:** If you are migrating a legacy pipeline to a new structure, you can use `rename_tables` as a "bridge" to keep your scripts compatible with new naming standards.




