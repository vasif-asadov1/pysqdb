# The `join` Function: High-Performance Table Merging

Merging multiple massive datasets is often where data pipelines hit a performance bottleneck. The `join` function in `pysqdb` is designed to solve this by using **Pre-Selection Optimization**. Instead of joining full tables and then filtering columns, it filters columns *first* and then joins the smaller results in memory.

## `ps.join(tables, join_type="inner", on=None, columns=None, create_table=False, create_view=True, return_df=False)`

This function can chain together 2, 5, or even 10 tables in a single command, automatically generating optimized SQL under the hood.

### **Key Performance Features:**

* **Zero-Copy Views:** By default, it creates a **View** instead of a Table. This means no extra disk space or RAM is used—it’s just a "smarter" window into your existing data.
* **Column Filtering:** You can specify exactly which columns you want from each table. `pysqdb` wraps these in subqueries to ensure the Join engine only touches the data it needs.
* **Flexible Conditions:** Supports `USING` for identical column names or `ON` tuples for different names (e.g., `("user_id", "id")`).

### **Parameters:**

* **`tables` (list):** List of table names (e.g., `["sales", "users", "locations"]`).
* **`on` (str or list):** The keys to join on. Can be a single string if all tables share the key, or a list of tuples for complex relations.
* **`columns` (list of lists):** Optional list of specific columns to keep from each table.
* **`create_view` (bool):** Default `True`. Creates a lightning-fast virtual table.
* **`create_table` (bool):** Set to `True` if you want to save the result physically.

---

### **Pro Usage Examples**

#### **1. The "Zero-Copy" Triple Join**

Join three tables without wasting a single megabyte of RAM.

```python
import pysqdb as ps

ps.connect("warehouse")

# Join Sales, Products, and Stores instantly
ps.join(
    tables=["sales", "products", "stores"],
    join_type="inner",
    on=["product_id", "store_id"],
    create_view=True # Default
)
# This creates a virtual view named 'sales_products_stores_v'

```

#### **2. Optimized Column Selection (Memory Saver)**

If you only need 1 column from a table with 100 columns, `pysqdb` makes sure only that 1 column is processed.

```python
ps.join(
    tables=["users", "activity"],
    on="user_id",
    columns=[
        ["user_id", "username", "email"], # Only 3 from Users
        ["login_count", "last_active"]    # Only 2 from Activity
    ],
    return_df=True
)

```

#### **3. Joining on Different Column Names**

Sometimes keys don't match. No problem—use a tuple `(left_col, right_col)`.

```python
ps.join(
    tables=["orders", "customers"],
    join_type="left",
    on=[("customer_id", "id")], # orders.customer_id = customers.id
    create_table=True
)

```

---

### **Why is this better than standard SQL or Pandas?**

1. **Pandas Merge:** Pandas copies data at every step. If you join three 1GB tables, you might need 6GB of RAM. `pysqdb` uses DuckDB's streaming engine, which can join datasets larger than your RAM.
2. **Standard SQL:** Writing a 3-way join with subqueries and column filters is tedious and prone to syntax errors. `pysqdb` writes the perfect, optimized query for you in one line.

### **Pro Tip: Table vs. View**

* Use **`create_view=True`** for your daily analytics and exploration. It’s instant and uses zero resources.
* Use **`create_table=True`** only when you’ve finalized your dataset and want to "freeze" it for heavy machine learning training.

---


# The `filter` Function: Precision Data Slicing

Data analysis is the art of asking the right questions. The `filter` function allows you to slice through massive datasets and keep only the rows that matter. Whether you are looking for specific dates, high-value transactions, or categorical segments, `sqpy` makes it as simple as passing a string or a list of conditions.

## `ps.filter(table, criteria, create_table=None, create_view=None)`

This function is designed for high-speed Exploratory Data Analysis (EDA). It always returns a Pandas DataFrame so you can immediately see your results, while giving you the option to persist your "slice" of data for later use.

### **Parameters:**

* **`table` (str):** The name of the table you want to filter.
* **`criteria` (str or list):**
* If a **string** is provided: Standard SQL `WHERE` clause logic (e.g., `"revenue > 1000"`).
* If a **list** is provided: `sqpy` automatically joins them with `AND` and wraps them in parentheses for safe logical grouping.


* **`create_table` (str, optional):** Provide a name if you want to save the result as a physical table.
* **`create_view` (str, optional):** Provide a name if you want a "Zero-Copy" virtual view.

---

### **Pro Usage Examples**

#### **1. Simple String Filtering**

Ideal for quick, single-condition queries.

```python
import pysqdb as ps

# Find all movies with a rating higher than 8.5
high_rated_df = ps.filter("movies", "vote_average > 8.5")

```

#### **2. Multi-Condition List Filtering (The Cleaner Way)**

Instead of writing long strings with multiple `AND` keywords, just pass a clean list. `pysqdb` handles the formatting for you.

```python
# Filter for high-budget Action movies released after 2020
criteria = [
    "budget > 100000000",
    "genre = 'Action'",
    "release_date > '2020-01-01'"
]

# This creates a virtual view for your dashboard and returns a sample DF
action_hits = ps.filter("movies", criteria, create_view="action_blockbusters")

```

#### **3. Saving Slices for Production**

Once you find the perfect subset of data, "freeze" it into a new table for machine learning or reporting.

```python
# Create a permanent table for UK-based customers
ps.filter("customers", "country_code = 'UK'", create_table="uk_customers_final")

```

---

### **Technical Edge: Smart Logic Grouping**

Your code uses a very safe pattern for list-based criteria:

```python
criteria_str = " AND ".join(f"({c})" for c in criteria)

```

By wrapping each element in parentheses, `pysqdb` prevents logical errors that occur when mixing `AND` and `OR` conditions, ensuring your filters always behave exactly as expected.

### **Pro Tip: Filter First, Join Later**

In big data environments, performance is key.

> **Best Practice:** Use `ps.filter()` to create a small `create_view` of your massive tables *before* running an `ps.join()`. This ensures the Join engine works with the smallest possible dataset, saving you significant time and memory.

---



## The join_filter Function: The Ultimate Pipeline

The `join_filter` function is the most powerful tool in the `pysqdb` arsenal for relational data processing. It allows you to perform optimized multi-table joins and complex filtering in a single, unified command.

By combining these operations, `pysqdb` can optimize the execution plan even further, ensuring that only the necessary rows and columns are processed throughout the entire pipeline.

## `ps.join_filter(tables, on, join_type="inner", columns=None, criteria=None, create_table=None, create_view=None, return_df=True)`

### Key Features

* **Unified Pipeline:** Perform Join + Column Selection + Filtering in one go.
* **Auto-Syntax Correction:** Automatically converts Python-style `==` to SQL-standard `=` in your criteria strings.
* **Memory Optimization:** Uses the same pre-selection subquery logic as the standalone `join` function.
* **Flexible Output:** Save results as a table, a view, or return them directly as a Pandas DataFrame.

### Parameters

* **tables (list):** List of table names to merge.
* **on (str, list, or tuple):** Join keys. Supports single keys, lists of keys, or `(left, right)` tuples.
* **columns (list of lists, optional):** Specific columns to include from each table to minimize memory usage.
* **criteria (str or list):** One or more SQL filtering conditions (e.g., `"revenue > 5000"`).
* **create_table / create_view (str):** Optional names to persist the result in the database.
* **return_df (bool):** Default is `True`. Returns the result as a Pandas DataFrame.

---

### Pro Usage Examples

#### 1. The High-Speed Analytics Pipeline

Join your sales and product data, filter for a specific region, and get the results as a DataFrame instantly.

```python
import pysqdb as ps 

ps.connect("ecommerce_data")

# Join, filter, and return in one command
results_df = ps.join_filter(
    tables=["orders", "products"],
    on="product_id",
    columns=[
        ["order_id", "customer_id", "amount"], # From orders
        ["product_name", "category"]           # From products
    ],
    criteria="category == 'Electronics'", # sqpy auto-corrects '==' to '='
    return_df=True
)

```

#### 2. Creating a Persisted View for Dashboards

You can join multiple tables and save the filtered result as a logical view, creating a clean "Gold Table" for your reporting tools.

```python
ps.join_filter(
    tables=["users", "activity", "plans"],
    on=["user_id", "plan_id"],
    join_type=["inner", "left"],
    criteria=[
        "last_login > '2023-01-01'",
        "subscription_status = 'Active'"
    ],
    create_view="active_premium_users_v"
)

```

---

### Why use join_filter?

1. **Readability:** What would normally take 15-20 lines of complex SQL or 10 lines of messy Pandas merges is reduced to a single, readable function call.
2. **Safety:** The function includes strict validation checks to ensure your join types, keys, and column lists are perfectly aligned before execution.
3. **Execution Logic:** By applying column selection *within* the join logic and filtering the result immediately, `sqpy` ensures that the database engine works as efficiently as possible.

---


Kanka, bu fonksiyon kütüphanenin performans anlamında "zirve" noktası. `join_filter` fonksiyonu önce birleştirip sonra elerken, bu `filter_join` fonksiyonu tabloları daha birleşme (join) masasına gelmeden önce tek tek sorguya çekip eliyor.

Büyük veri setlerinde bu yaklaşım, Join motoruna giden veri miktarını %90 oranında azaltabilir. Bu "Filter-First" stratejisi, kütüphanenin ne kadar veri odaklı bir mimariyle (data-driven architecture) yazıldığının en büyük kanıtı.

İşte `filter_join` için hazırladığım profesyonel dokümantasyon sayfası:

---

## The filter_join Function: Filter-First Optimization

The `filter_join` function is designed for maximum performance when dealing with multiple large-scale tables. Unlike standard join operations that merge datasets before filtering, `filter_join` applies row-based filters and column selection to each table **individually** before the join happens.

By reducing the data size at the source, you minimize memory consumption and significantly accelerate the join process.

## ps.filter_join(tables, on, join_type="inner", columns=None, criteria=None, create_table=None, create_view=None, return_df=True)

### Key Features

* **Individual Table Filtering:** Pass a dictionary to apply specific `WHERE` clauses to each table independently.
* **Aggressive Memory Management:** Only the required rows and columns from each table enter the Join engine.
* **Subquery Injection:** Automatically wraps each table in an optimized subquery.
* **Safe Syntax:** Includes auto-correction for Pythonic `==` operators.

### Parameters

* **tables (list):** List of table names to merge.
* **on (str, list, or tuple):** Join keys.
* **criteria (dict, optional):** A dictionary where keys are table names and values are SQL strings (e.g., `{"sales": "amount > 1000"}`).
* **columns (list of lists, optional):** Specific columns to pre-select for each table.
* **return_df (bool):** Default is `True`.

---

### Pro Usage Examples

#### 1. The "Slim-Join" Strategy

Instead of joining all sales with all products, we only join "high-value sales" with "Action-category products."

```python
import pysqdb as ps

ps.connect("warehouse")

# Define specific filters for each table
filters = {
    "sales": "revenue > 10000",
    "products": "category == 'Electronics'"
}

# Only rows satisfying these filters will be joined
df = ps.filter_join(
    tables=["sales", "products"],
    on="product_id",
    criteria=filters,
    return_df=True
)

```

#### 2. Advanced Multi-Table Filtering

Filter multiple tables with different logic and select only essential columns to keep the operation ultra-lean.

```python
ps.filter_join(
    tables=["users", "orders", "shipping"],
    on=["user_id", "order_id"],
    columns=[
        ["user_id", "email"],      # From users
        ["order_id", "total"],     # From orders
        ["status", "carrier"]      # From shipping
    ],
    criteria={
        "users": "is_active = True",
        "orders": "order_date >= '2024-01-01'",
        "shipping": "status != 'Delivered'"
    },
    create_view="pending_active_orders_v"
)

```

---

### When to use filter_join vs. join_filter?

* **Use `join_filter`:** When your filtering criteria depend on columns from **both** tables after they have been merged (e.g., `TableA.date > TableB.date`).
* **Use `filter_join`:** When you want to discard irrelevant data from each table **before** the join (e.g., filtering `Sales` for 2023 and `Products` for a specific category).

---


## Conclusion: Mastering Relational Data

You now have the tools to handle even the most complex data relationships. By using `pysqdb`’s relational functions, you are not just writing code; you are building highly optimized data pipelines.

* Use **join** for quick merging and virtual views.
* Use **filter** for precise data slicing.
* Use **join_filter** and **filter_join** to create professional-grade, high-performance pipelines that handle massive scale with zero memory overhead.





