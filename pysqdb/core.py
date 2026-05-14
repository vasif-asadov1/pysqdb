import duckdb
import pandas as pd
import os
import warnings

# Global variable to store the active database connection
_active_conn = None

def connect(db_name):
    """
    Connects to an existing DuckDB database or creates a new one.
    Automatically appends '.duckdb' if no extension is provided.
    
    Args:
        db_name (str): The name or path of the database.
        
    Returns:
        duckdb.DuckDBPyConnection: The active database connection object.
    """
    global _active_conn
    
    if db_name != ":memory:" and not db_name.endswith(".duckdb"):
        db_path = f"{db_name}.duckdb"
    else:
        db_path = db_name
        
    if _active_conn is not None:
        _active_conn.close()
        
    _active_conn = duckdb.connect(db_path)
    print(f"Successfully connected to: {db_path}")
    
    return _active_conn

def read(path, file_type, table_name, strict=False):
    """
    Reads a file and loads it into the active DuckDB database as a table.
    Uses 'CREATE OR REPLACE TABLE' to avoid errors if the table already exists.
    
    Args:
        path (str): The path to the file.
        file_type (str): Format of the file ('csv', 'json', 'parquet', 'excel').
        table_name (str): The name of the table to be created in the database.
        strict (bool): If True, strictly checks CSV formatting. Default is False to allow dirty Kaggle data.
        
    Raises:
        ConnectionError: If no active database connection exists.
        ValueError: If the file_type is not supported.
        FileNotFoundError: If the file does not exist.
    """
    import os
    global _active_conn
    
    # Check if user connected to a database first
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    # Check if the file actually exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file was not found at: {path}")
        
    file_type = file_type.lower()
    
    try:
        if file_type == "csv":
            strict_val = "true" if strict else "false"
            try:
                # DUCKDB GÜNCELLEMESİ: ignore_errors=true ve max_line_size=10000000 eklendi.
                # Bu sayede devasa JSON stringleri DuckDB'yi patlatamaz.
                query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{path}', strict_mode={strict_val}, ignore_errors=true, max_line_size=10000000)"
                _active_conn.execute(query)
            except Exception as duck_e:
                print(f"DuckDB okumasında zorlandı, Pandas Fallback devreye giriyor... ({duck_e})")
                import pandas as pd
                
                # PANDAS GÜNCELLEMESİ: engine='python' eklendi. 
                # C motorunun Buffer Overflow hatasını aşmak için saf Python motoru kullanılır.
                temp_df = pd.read_csv(path, on_bad_lines='skip', low_memory=False, engine='python') 
                _active_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
                
        elif file_type == "json":
            _active_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{path}')")
            
        elif file_type == "parquet":
            _active_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')")
            
        elif file_type in ["excel", "xlsx"]:
            import pandas as pd
            temp_df = pd.read_excel(path)
            _active_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
            
        else:
            raise ValueError(f"Unsupported file_type: '{file_type}'. Supported types: csv, json, parquet, excel.")
            
        print(f"Successfully loaded '{path}' into table '{table_name}'.")
        
    except Exception as e:
        print(f"Error reading file '{path}': {e}")
        raise e
    


def export_table(table_name, export_format="csv", file_name=None):
    """
    Exports a table from the active database to a file.
    
    Args:
        table_name (str): The name of the table to export.
        export_format (str): Format ('csv', 'json', 'parquet', 'excel'). Default is 'csv'.
        file_name (str, optional): Custom name for the output file. 
                                  If None, table_name will be used.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    export_format = export_format.lower()
    
    # Eğer dosya ismi verilmediyse tablo ismini kullan, uzantıyı otomatik ekle
    actual_file_name = file_name if file_name else table_name
    
    # Uzantı kontrolü: Kullanıcı yanlışlıkla dosya ismine uzantı eklemişse mükerrer olmasın
    if not actual_file_name.endswith(f".{export_format}"):
        export_path = f"{actual_file_name}.{export_format}"
    else:
        export_path = actual_file_name
    
    try:
        if export_format in ["csv", "json", "parquet"]:
            # DuckDB COPY komutu performans için en iyisidir
            # (FORMAT...) ifadesiyle formatı açıkça belirtmek daha garantidir
            _active_conn.execute(f"COPY {table_name} TO '{export_path}' (FORMAT {export_format.upper()})")
            
        elif export_format in ["excel", "xlsx"]:
            # Excel için mecburen Pandas fallback kullanıyoruz
            # .df() yerine .fetchdf() bazen daha stabil olabilir ama .df() de iş görür
            export_path = actual_file_name if actual_file_name.endswith(".xlsx") else f"{actual_file_name}.xlsx"
            df = _active_conn.execute(f"SELECT * FROM {table_name}").df()
            df.to_excel(export_path, index=False)
            
        else:
            raise ValueError(f"Unsupported export_format: '{export_format}'. Supported: csv, json, parquet, excel.")
            
        print(f"Successfully exported '{table_name}' to '{export_path}'")
        
    except Exception as e:
        print(f"Error exporting table '{table_name}': {e}")
        raise e
    

    

def DF(table, head=None, tail=None, sample=None, columns=None):
    """
    Returns the specified table as a Pandas DataFrame with optional filtering.
    
    Args:
        table (str): The name of the table.
        head (int, optional): Number of top rows to return.
        tail (int, optional): Number of bottom rows to return.
        sample (int, optional): Number of random rows to return.
        columns (list or str, optional): List of columns to include.
        
    Returns:
        pandas.DataFrame: The resulting dataframe.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    # 1. Column Selection
    cols_str = "*"
    if columns is not None:
        if isinstance(columns, list):
            cols_str = ", ".join(columns)
        else:
            cols_str = columns
            
    query = f"SELECT {cols_str} FROM {table}"
    
    # 2. Row Selection (Head, Tail, Sample)
    if sample is not None:
        query += f" USING SAMPLE {sample} ROWS"
    elif head is not None:
        query += f" LIMIT {head}"
    elif tail is not None:
        # SQL tables have no default order, so we fetch total count and use OFFSET for performance
        count = _active_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        offset = max(0, count - tail)
        query += f" LIMIT {tail} OFFSET {offset}"
        
    return _active_conn.execute(query).df()


def give_array(table, columns):
    """
    Returns selected columns from a table as a NumPy array.
    
    Args:
        table (str): The name of the table.
        columns (list or str): Column(s) to include in the array.
        
    Returns:
        numpy.ndarray: The resulting array.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if isinstance(columns, str):
        columns = [columns]
        
    cols_str = ", ".join(columns)
    query = f"SELECT {cols_str} FROM {table}"
    
    # Fetch as DataFrame and convert to NumPy array
    return _active_conn.execute(query).df().to_numpy()


def give_info(table):
    """
    Provides comprehensive information about the table including row/column counts,
    and statistical summaries (min, max, mean, percentiles, null counts) using DuckDB's SUMMARIZE.
    
    Args:
        table (str): The name of the table.
        
    Returns:
        pandas.DataFrame: A summary dataframe containing the stats.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    # Get total row and column counts
    count = _active_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    num_cols = len(_active_conn.execute(f"DESCRIBE {table}").df())
    
    print("-" * 60)
    print(f"TABLE INFO: '{table}'")
    print(f"Total Rows: {count}")
    print(f"Total Columns: {num_cols}")
    print("-" * 60)
    
    # DuckDB SUMMARIZE computes min, max, approx_unique, avg, q25, q50, q75, null_percentage etc. natively!
    summary_df = _active_conn.execute(f"SUMMARIZE {table}").df()
    
    # We will drop some highly technical columns for a cleaner user experience
    columns_to_keep = ['column_name', 'column_type', 'min', 'max', 'approx_unique', 
                       'avg', 'q25', 'q50', 'q75', 'count', 'null_percentage']
    
    return summary_df[columns_to_keep]




def impute(table, column, method="mean", fixed_const=None, return_df=False):
    """
    Handles missing values (NULLs) in a specified column using SQL update operations.
    Includes data type validation for 'fixed_const' method.
    
    Args:
        table (str): The name of the table.
        column (str): The name of the column to impute.
        method (str): 'fixed_const', 'mean', 'median', or 'mode'.
        fixed_const (any, optional): The constant value to use if method is 'fixed_const'.
        return_df (bool): If True, returns the updated table as a DataFrame.
        
    Returns:
        pandas.DataFrame or None
    """

    # Define _active_conn as global to modify it within the function
    global _active_conn


    if _active_conn is None: # Check if user connected to a database first
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    method = method.lower() # Normalize method string for case-insensitive comparison
    
    if method == "fixed_const":
        if fixed_const is None: # Ensure that fixed_const is provided when method is 'fixed_const'
            raise ValueError("fixed_const parameter is required when method is 'fixed_const'")
            
        # 1. Get the column data type from DuckDB
        col_info = _active_conn.execute(f"DESCRIBE {table}").df()
        
        # Check if column exists
        if column not in col_info['column_name'].values:  # column_name is the default name for the column in DESCRIBE output
            raise ValueError(f"Column '{column}' does not exist in table '{table}'.")
            
        col_type = col_info.loc[col_info['column_name'] == column, 'column_type'].values[0].upper() # Get the data type of the specified column and convert to uppercase for easier comparison
        
        # 2. Check if the column is numeric or categoric (string)
        is_numeric = any(t in col_type for t in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']) # DuckDB uses these types for numeric columns
        is_string = any(t in col_type for t in ['VARCHAR', 'CHAR', 'STRING']) # DuckDB uses these types for string columns
        
        # 3. Apply the user's validation rules
        if is_numeric and isinstance(fixed_const, str): # If the column is numeric, fixed_const should not be a string
            raise TypeError(f"Column '{column}' is numeric. 'fixed_const' cannot be a string.")
            
        if is_string and not isinstance(fixed_const, str): # If the column is string, fixed_const should be a string
            raise TypeError(f"Column '{column}' is categoric. 'fixed_const' must be a string.")
            
        # 4. Format the value for SQL
        val = f"'{fixed_const}'" if isinstance(fixed_const, str) else str(fixed_const)
        update_query = f"UPDATE {table} SET {column} = {val} WHERE {column} IS NULL"
        
    elif method in ["mean", "median", "mode"]:
        agg_func = "avg" if method == "mean" else method
        subquery = f"(SELECT {agg_func}({column}) FROM {table})"
        update_query = f"UPDATE {table} SET {column} = {subquery} WHERE {column} IS NULL"
    else:
        raise ValueError("Unsupported method. Use 'fixed_const', 'mean', 'median', or 'mode'.")
        
    _active_conn.execute(update_query)
    print(f"Successfully imputed missing values in '{column}' using method '{method}'.")
    
    if return_df:
        return _active_conn.execute(f"SELECT * FROM {table}").df()
    

def remove_outliers(table, columns, return_df=False):
    """
    Removes outliers from the specified numerical columns using the IQR method.
    Issues a warning if a categorical column is provided and skips it.
    
    Args:
        table (str): The name of the table.
        columns (str or list): The column(s) to check for outliers.
        return_df (bool): If True, returns the updated table as a DataFrame.
        
    Returns:
        pandas.DataFrame or None
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    # Ensure columns is a list
    if isinstance(columns, str):
        columns = [columns]
        
    # 1. Get column information to check data types
    col_info = _active_conn.execute(f"DESCRIBE {table}").df()
    valid_cols = col_info['column_name'].values
    
    cols_to_process = []
    
    # 2. Filter valid numeric columns and warn about categoricals
    for col in columns:
        if col not in valid_cols:
            warnings.warn(f"Column '{col}' does not exist in table '{table}'. Skipping.")
            continue
            
        col_type = col_info.loc[col_info['column_name'] == col, 'column_type'].values[0].upper()
        is_numeric = any(t in col_type for t in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'])
        
        if not is_numeric:
            # Issue a warning but DO NOT stop execution
            warnings.warn(f"Column '{col}' is not numerical (type: {col_type}). Outlier removal is for numerical columns. Skipping '{col}'.")
        else:
            cols_to_process.append(col)
            
    # 3. Process valid numerical columns using the IQR method
    for col in cols_to_process:
        # DuckDB natively supports exact quantiles using quantile_cont
        query = f"SELECT quantile_cont({col}, 0.25), quantile_cont({col}, 0.75) FROM {table} WHERE {col} IS NOT NULL"
        quantiles = _active_conn.execute(query).fetchone()
        
        q1, q3 = quantiles
        
        # If the column lacks valid data, skip
        if q1 is None or q3 is None:
            continue
            
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # SQL DELETE operation for blazing fast removal
        delete_query = f"DELETE FROM {table} WHERE {col} < {lower_bound} OR {col} > {upper_bound}"
        _active_conn.execute(delete_query)
        
        print(f"Successfully removed outliers from '{col}' (Bounds: [{lower_bound:.2f}, {upper_bound:.2f}]).")
        
    if return_df:
        return _active_conn.execute(f"SELECT * FROM {table}").df()
    

def clip_outliers(table, columns, return_df=False):
    """
    Clips (winsorizes) outliers in the specified numerical columns using the IQR method.
    Values below the lower bound are set to the lower bound, and values above 
    the upper bound are set to the upper bound.
    Issues a warning if a categorical column is provided and skips it.
    
    Args:
        table (str): The name of the table.
        columns (str or list): The column(s) to clip outliers for.
        return_df (bool): If True, returns the updated table as a DataFrame.
        
    Returns:
        pandas.DataFrame or None
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if isinstance(columns, str):
        columns = [columns]
        
    col_info = _active_conn.execute(f"DESCRIBE {table}").df()
    valid_cols = col_info['column_name'].values
    
    cols_to_process = []
    
    for col in columns:
        if col not in valid_cols:
            warnings.warn(f"Column '{col}' does not exist in table '{table}'. Skipping.")
            continue
            
        col_type = col_info.loc[col_info['column_name'] == col, 'column_type'].values[0].upper()
        is_numeric = any(t in col_type for t in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'])
        
        if not is_numeric:
            warnings.warn(f"Column '{col}' is not numerical (type: {col_type}). Outlier clipping is for numerical columns. Skipping '{col}'.")
        else:
            cols_to_process.append(col)
            
    for col in cols_to_process:
        query = f"SELECT quantile_cont({col}, 0.25), quantile_cont({col}, 0.75) FROM {table} WHERE {col} IS NOT NULL"
        quantiles = _active_conn.execute(query).fetchone()
        
        q1, q3 = quantiles
        
        if q1 is None or q3 is None:
            continue
            
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # SQL UPDATE operation to clip values using GREATEST and LEAST
        # LEAST ensures values don't go above the upper_bound
        # GREATEST ensures values don't go below the lower_bound
        update_query = f"""
            UPDATE {table} 
            SET {col} = LEAST(GREATEST({col}, {lower_bound}), {upper_bound})
            WHERE {col} IS NOT NULL
        """
        _active_conn.execute(update_query)
        
        print(f"Successfully clipped outliers in '{col}' (Bounds: [{lower_bound:.2f}, {upper_bound:.2f}]).")
        
    if return_df:
        return _active_conn.execute(f"SELECT * FROM {table}").df()
    

def show_duplicates(table):
    """
    Finds and returns ALL occurrences of duplicated rows in the specified table.
    Adds a '_duplicate_count' column and sorts them so duplicates appear together.
    
    Args:
        table (str): The name of the table.
        
    Returns:
        pandas.DataFrame: A dataframe containing all duplicated rows.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    # Get all column names to partition by
    col_info = _active_conn.execute(f"DESCRIBE {table}").df()
    # Safely wrap column names in double quotes
    cols_str = ", ".join([f'"{c}"' for c in col_info['column_name'].values])
    
    # Use Window Function to count duplicates for EVERY row, then QUALIFY to filter > 1
    # ORDER BY ensures identical rows are grouped together in the final dataframe
    query = f"""
        SELECT *, COUNT(*) OVER (PARTITION BY {cols_str}) as _duplicate_count 
        FROM {table} 
        QUALIFY _duplicate_count > 1
        ORDER BY {cols_str}
    """
    
    df = _active_conn.execute(query).df()
    
    if df.empty:
        print(f"No duplicates found in table '{table}'.")
    else:
        # Number of unique duplicates vs total rows returned
        unique_dups = len(df.drop_duplicates(subset=col_info['column_name'].values))
        print(f"Found {unique_dups} unique duplicated group(s). Displaying all {len(df)} rows.")
        
    return df



def remove_duplicates(table, keep_first=False, keep_last=False, return_df=False):
    """
    Removes duplicated rows from the specified table.
    If both keep_first and keep_last are False, ALL occurrences of the duplicated rows are removed.
    If either keep_first or keep_last is True, exactly one copy of the duplicated row is kept.
    
    Args:
        table (str): The name of the table.
        keep_first (bool): Keep one copy of the duplicate.
        keep_last (bool): Keep one copy of the duplicate (same as keep_first for exact row duplicates).
        return_df (bool): If True, returns the updated table as a DataFrame.
        
    Returns:
        pandas.DataFrame or None
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if keep_first and keep_last:
        raise ValueError("You cannot set both 'keep_first' and 'keep_last' to True at the same time.")
        
    if not keep_first and not keep_last:
        # User wants to remove ALL occurrences of duplicated rows (keep none)
        query = f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {table} GROUP BY ALL HAVING COUNT(*) = 1"
        _active_conn.execute(query)
        print(f"Successfully removed ALL duplicated rows from '{table}'. Kept none of them.")
    else:
        # User wants to keep exactly one copy of the duplicate (DISTINCT)
        query = f"CREATE OR REPLACE TABLE {table} AS SELECT DISTINCT * FROM {table}"
        _active_conn.execute(query)
        print(f"Successfully removed duplicates from '{table}'. Kept exactly one copy of each.")
        
    if return_df:
        return _active_conn.execute(f"SELECT * FROM {table}").df()
    



def join(tables, join_type="inner", on=None, columns=None, create_table=False, create_view=True, return_df=False):
    """
    Dynamically joins multiple tables with specified join types, conditions, and selected columns.
    Optimized for large tables by pre-selecting columns before joining.
    
    Args:
        tables (list): List of table names to join.
        join_type (str or list): Join type(s). Default is 'inner'.
        on (str, list, or tuple): Join condition(s).
        columns (list of lists, optional): Specific columns to select for each table to optimize memory. 
                                           Must match the order of 'tables'. Use [] or None for 'SELECT *'.
        create_table (bool): If True, creates a physical table.
        create_view (bool): If True, creates a logical view (Default: True).
        return_df (bool): If True, returns the result as a DataFrame.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if not create_table and not create_view and not return_df:
        raise ValueError("Error: You must set at least one of 'create_table', 'create_view', or 'return_df' to True.")
        
    if not isinstance(tables, list) or len(tables) < 2:
        raise ValueError("The 'tables' parameter must be a list containing at least two table names.")
        
    if on is None:
        raise ValueError("The 'on' parameter is required to join tables.")
        
    if columns is not None:
        if not isinstance(columns, list) or len(columns) != len(tables):
            raise ValueError(f"The 'columns' parameter must be a list with exactly {len(tables)} elements matching the 'tables' list.")

    n_joins = len(tables) - 1
    
    if isinstance(join_type, str):
        join_type = [join_type] * n_joins
    elif len(join_type) != n_joins:
        raise ValueError(f"Expected {n_joins} join_types, got {len(join_type)}.")
        
    if isinstance(on, (str, tuple)):
        on = [on] * n_joins
    elif len(on) != n_joins:
        raise ValueError(f"Expected {n_joins} 'on' conditions, got {len(on)}.")

    # Helper function to create subqueries for optimized column selection
    def get_table_str(idx):
        t_name = tables[idx]
        if columns is None or not columns[idx]:
            return t_name # Behavior like SELECT *
            
        cols = columns[idx]
        if isinstance(cols, str):
            cols = [cols]
            
        cols_str = ", ".join(cols)
        # Wrap in subquery to filter columns BEFORE joining
        return f"(SELECT {cols_str} FROM {t_name}) AS {t_name}"

    query_parts = [f"FROM {get_table_str(0)}"]
    
    for i in range(n_joins):
        t_right_str = get_table_str(i+1)
        t_right_name = tables[i+1]
        
        j_type = join_type[i].upper()
        if "JOIN" not in j_type:
            j_type += " JOIN"
            
        condition = on[i]
        
        if isinstance(condition, tuple):
            left_col, right_col = condition
            if left_col == right_col:
                join_clause = f"USING ({left_col})"
            else:
                join_clause = f"ON {left_col} = {t_right_name}.{right_col}"
        else:
            join_clause = f"USING ({condition})"
            
        query_parts.append(f"{j_type} {t_right_str} {join_clause}")
        
    full_query = "SELECT * " + " ".join(query_parts)
    base_name = "_".join(tables)
    
    if create_table:
        t_name = f"{base_name}_t"
        _active_conn.execute(f"CREATE OR REPLACE TABLE {t_name} AS {full_query}")
        print(f"Successfully created table '{t_name}'.")
        
    if create_view:
        v_name = f"{base_name}_v"
        _active_conn.execute(f"CREATE OR REPLACE VIEW {v_name} AS {full_query}")
        print(f"Successfully created view '{v_name}'.")
        
    if return_df:
        return _active_conn.execute(full_query).df()
    

def sort_table(table, by, ascending=True, return_df=False):
    """
    Sorts the table by the specified column(s) and physically updates the table in the database.
    
    Args:
        table (str): The name of the table to sort.
        by (str or list): The column name(s) to sort by.
        ascending (bool or list): Sorting order. True for ascending, False for descending.
                                  Can be a single boolean or a list matching the 'by' parameter.
        return_df (bool): If True, returns the sorted table as a DataFrame.
        
    Returns:
        pandas.DataFrame or None
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if isinstance(by, str):
        by = [by]
        
    if isinstance(ascending, bool):
        ascending = [ascending] * len(by)
    elif len(ascending) != len(by):
        raise ValueError("The length of the 'ascending' list must match the length of the 'by' list.")
        
    order_clauses = []
    for col, asc in zip(by, ascending):
        direction = "ASC" if asc else "DESC"
        order_clauses.append(f"{col} {direction}")
        
    order_by_str = ", ".join(order_clauses)
    
    # Update the table physically in the database with the new order
    query = f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {table} ORDER BY {order_by_str}"
    _active_conn.execute(query)
    
    print(f"Successfully sorted table '{table}' by: {order_by_str}")
    
    if return_df:
        return _active_conn.execute(f"SELECT * FROM {table}").df()



# COPY THE TABLE

def copy_table(table, copied_table):
    """
    Creates an exact physical copy of an existing table in the database.
    Highly recommended before performing destructive data cleaning operations.
    
    Args:
        table (str): The name of the source table.
        copied_table (str): The name of the new duplicated table.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    query = f"CREATE OR REPLACE TABLE {copied_table} AS SELECT * FROM {table}"
    _active_conn.execute(query)
    
    print(f"Successfully copied table '{table}' to '{copied_table}'.")



def filter(table, criteria, create_table=None, create_view=None):
    """
    Filters a table based on specified SQL criteria.
    Always returns a pandas DataFrame for EDA purposes.
    Optionally stores the filtered result as a new physical table or logical view.
    
    Args:
        table (str): The name of the table to filter.
        criteria (str or list): The filtering condition(s). If a list is provided,
                                conditions are combined using ' AND '.
        create_table (str, optional): Name of the new table to save results. Default is None.
        create_view (str, optional): Name of the new view to save results. Default is None.
        
    Returns:
        pandas.DataFrame: The filtered data.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")

    # Parse criteria: if it's a list, combine with AND
    if isinstance(criteria, list):
        # Wrap each condition in parentheses to ensure safe logical grouping
        criteria_str = " AND ".join(f"({c})" for c in criteria)
    else:
        criteria_str = criteria

    # Base query for filtering
    query = f"SELECT * FROM {table} WHERE {criteria_str}"

    # 1. Create a physical table if requested
    if create_table is not None:
        _active_conn.execute(f"CREATE OR REPLACE TABLE {create_table} AS {query}")
        print(f"Successfully created filtered table '{create_table}'.")

    # 2. Create a logical view if requested
    if create_view is not None:
        _active_conn.execute(f"CREATE OR REPLACE VIEW {create_view} AS {query}")
        print(f"Successfully created filtered view '{create_view}'.")

    # 3. Always return as DataFrame
    return _active_conn.execute(query).df()


def join_filter(tables, on, join_type="inner", columns=None, criteria=None, create_table=None, create_view=None, return_df=True):
    """
    The ultimate operation: Joins multiple tables (optimized with column selection) 
    and applies a filter to the joined result in one go.
    
    Args:
        tables (list): List of table names to join.
        on (str, list, or tuple): Join condition(s).
        join_type (str or list): Join type(s). Default is 'inner'.
        columns (list of lists, optional): Specific columns to include from each table.
        criteria (str or list): The WHERE clause applied to the joined result.
        create_table (str, optional): Name of the physical table to save results.
        create_view (str, optional): Name of the logical view to save results.
        return_df (bool): If True, returns the result as a DataFrame (Default: True).
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")

    # User MUST choose at least one output method. Bam! Right in the face.
    if not return_df and create_table is None and create_view is None:
        raise ValueError("ERROR: You set 'return_df=False' but didn't provide a name for 'create_table' or 'create_view'. Where should I put the results?!")

    if not isinstance(tables, list) or len(tables) < 2:
        raise ValueError("The 'tables' parameter must be a list containing at least two table names.")
    if on is None:
        raise ValueError("The 'on' parameter is required to join tables.")

    n_joins = len(tables) - 1
    
    if isinstance(join_type, str):
        join_type = [join_type] * n_joins
    elif len(join_type) != n_joins:
        raise ValueError(f"Expected {n_joins} join_types, got {len(join_type)}.")
        
    if isinstance(on, (str, tuple)):
        on = [on] * n_joins
    elif len(on) != n_joins:
        raise ValueError(f"Expected {n_joins} 'on' conditions, got {len(on)}.")

    if columns is not None:
        if not isinstance(columns, list) or len(columns) != len(tables):
            raise ValueError(f"The 'columns' parameter must be a list with exactly {len(tables)} elements matching the 'tables' list.")

    # Helper function for optimized column selection
    def get_table_str(idx):
        t_name = tables[idx]
        if columns is None or not columns[idx]:
            return t_name
        cols = columns[idx]
        if isinstance(cols, str): cols = [cols]
        cols_str = ", ".join(cols)
        return f"(SELECT {cols_str} FROM {t_name}) AS {t_name}"

    query_parts = [f"FROM {get_table_str(0)}"]
    
    # 1. Build the JOIN part
    for i in range(n_joins):
        t_right_str = get_table_str(i+1)
        t_right_name = tables[i+1]
        
        j_type = join_type[i].upper()
        if "JOIN" not in j_type:
            j_type += " JOIN"
            
        condition = on[i]
        
        if isinstance(condition, tuple):
            left_col, right_col = condition
            if left_col == right_col:
                join_clause = f"USING ({left_col})"
            else:
                join_clause = f"ON {left_col} = {t_right_name}.{right_col}"
        else:
            join_clause = f"USING ({condition})"
            
        query_parts.append(f"{j_type} {t_right_str} {join_clause}")
        
    base_join_query = "SELECT * " + " ".join(query_parts)

    # 2. Build the FILTER part
    if criteria:
        if isinstance(criteria, list):
            criteria_str = " AND ".join(f"({c})" for c in criteria)
        else:
            criteria_str = criteria
            
        # Pythonic syntax correction: replace '==' with '=' for SQL compatibility
        criteria_str = criteria_str.replace("==", "=")
        
        final_query = f"{base_join_query} WHERE {criteria_str}"
    else:
        final_query = base_join_query

    # 3. Handle Outputs
    if create_table is not None:
        _active_conn.execute(f"CREATE OR REPLACE TABLE {create_table} AS {final_query}")
        print(f"Successfully created filtered table '{create_table}'.")

    if create_view is not None:
        _active_conn.execute(f"CREATE OR REPLACE VIEW {create_view} AS {final_query}")
        print(f"Successfully created filtered view '{create_view}'.")

    if return_df:
        return _active_conn.execute(final_query).df()
    


def filter_join(tables, on, join_type="inner", columns=None, criteria=None, create_table=None, create_view=None, return_df=True):
    """
    Optimized Join operation: Filters individual tables FIRST (row-based), 
    selects required columns, and THEN joins them together.
    
    Args:
        tables (list): List of table names.
        on (str, list, or tuple): Join condition(s).
        join_type (str or list): Join type(s). Default is 'inner'.
        columns (list of lists, optional): Columns to select from each table.
        criteria (dict, optional): Dictionary mapping table names to their WHERE clauses 
                                   (e.g., {"sales": "sales_amount > 10000"}).
        create_table (str, optional): Name of the physical table to save results.
        create_view (str, optional): Name of the logical view to save results.
        return_df (bool): If True, returns the result as a DataFrame.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")

    if not return_df and create_table is None and create_view is None:
        raise ValueError("ERROR: You set 'return_df=False' but didn't provide a name for 'create_table' or 'create_view'.")

    if not isinstance(tables, list) or len(tables) < 2:
        raise ValueError("The 'tables' parameter must be a list containing at least two table names.")
    if on is None:
        raise ValueError("The 'on' parameter is required to join tables.")
        
    if criteria is not None and not isinstance(criteria, dict):
        raise ValueError("The 'criteria' parameter must be a dictionary (e.g., {'table_name': 'condition'}).")

    n_joins = len(tables) - 1
    
    if isinstance(join_type, str): join_type = [join_type] * n_joins
    elif len(join_type) != n_joins: raise ValueError(f"Expected {n_joins} join_types, got {len(join_type)}.")
        
    if isinstance(on, (str, tuple)): on = [on] * n_joins
    elif len(on) != n_joins: raise ValueError(f"Expected {n_joins} 'on' conditions, got {len(on)}.")

    if columns is not None:
        if not isinstance(columns, list) or len(columns) != len(tables):
            raise ValueError(f"The 'columns' parameter must be a list with exactly {len(tables)} elements.")

    # Helper function: Builds the optimized subquery (Filter rows -> Select columns)
    def get_table_str(idx):
        t_name = tables[idx]
        
        # 1. Column Selection Setup
        if columns is None or not columns[idx]:
            cols_str = "*"
        else:
            cols = columns[idx]
            if isinstance(cols, str): cols = [cols]
            cols_str = ", ".join(cols)
            
        # 2. Row Filtering Setup (Checking the criteria dictionary)
        filter_str = ""
        if criteria and t_name in criteria:
            # Pythonic fix: replace '==' with '=' for SQL
            cond = criteria[t_name].replace("==", "=")
            filter_str = f" WHERE {cond}"
            
        return f"(SELECT {cols_str} FROM {t_name}{filter_str}) AS {t_name}"

    # Build the JOIN query using our optimized subqueries
    query_parts = [f"FROM {get_table_str(0)}"]
    
    for i in range(n_joins):
        t_right_str = get_table_str(i+1)
        t_right_name = tables[i+1]
        
        j_type = join_type[i].upper()
        if "JOIN" not in j_type: j_type += " JOIN"
            
        condition = on[i]
        if isinstance(condition, tuple):
            left_col, right_col = condition
            if left_col == right_col:
                join_clause = f"USING ({left_col})"
            else:
                join_clause = f"ON {left_col} = {t_right_name}.{right_col}"
        else:
            join_clause = f"USING ({condition})"
            
        query_parts.append(f"{j_type} {t_right_str} {join_clause}")
        
    final_query = "SELECT * " + " ".join(query_parts)

    # 3. Handle Outputs
    if create_table is not None:
        _active_conn.execute(f"CREATE OR REPLACE TABLE {create_table} AS {final_query}")
        print(f"Successfully created filtered_join table '{create_table}'.")

    if create_view is not None:
        _active_conn.execute(f"CREATE OR REPLACE VIEW {create_view} AS {final_query}")
        print(f"Successfully created filtered_join view '{create_view}'.")

    if return_df:
        return _active_conn.execute(final_query).df()




def remove_columns(target, columns, new_name=None, return_df=False):
    """
    Removes specified columns from a Table or a View.
    - If target is a Table: Drops columns in-place (or creates a new table if new_name is provided).
    - If target is a View: Creates a new view excluding the specified columns.
    
    Args:
        target (str): The name of the table or view.
        columns (str or list): The column(s) to remove.
        new_name (str, optional): The name of the new table/view. Required for in-place view modifications.
        return_df (bool): If True, returns the updated result as a DataFrame.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if isinstance(columns, str):
        columns = [columns]
        
    # Check if target is a TABLE or a VIEW
    type_query = f"SELECT table_type FROM information_schema.tables WHERE table_name = '{target}'"
    target_type_result = _active_conn.execute(type_query).fetchone()
    
    if not target_type_result:
        raise ValueError(f"Target '{target}' does not exist in the database.")
        
    target_type = target_type_result[0] # Returns 'BASE TABLE' or 'VIEW'
    cols_str = ", ".join(columns)
    
    if target_type == 'BASE TABLE':
        if new_name:
            # Create a new table without the columns (Safe Copy)
            _active_conn.execute(f"CREATE OR REPLACE TABLE {new_name} AS SELECT * EXCLUDE ({cols_str}) FROM {target}")
            print(f"Successfully created new table '{new_name}' excluding: {cols_str}")
            final_target = new_name
        else:
            # Drop columns in-place for maximum speed
            for col in columns:
                _active_conn.execute(f"ALTER TABLE {target} DROP COLUMN {col}")
            print(f"Successfully dropped columns from table '{target}': {cols_str}")
            final_target = target
            
    elif target_type == 'VIEW':
        if not new_name:
            new_name = f"{target}_edited"
            print(f"Note: Views cannot be modified in-place directly. Creating a new view named '{new_name}'.")
            
        # Create a new view excluding the columns using DuckDB's powerful EXCLUDE syntax
        _active_conn.execute(f"CREATE OR REPLACE VIEW {new_name} AS SELECT * EXCLUDE ({cols_str}) FROM {target}")
        print(f"Successfully created new view '{new_name}' excluding: {cols_str}")
        final_target = new_name
        
    if return_df:
        return _active_conn.execute(f"SELECT * FROM {final_target}").df()





def remove_rows(target, criteria, new_name=None, return_df=False):
    """
    Removes rows from a Table or a View based on specified criteria.
    - If target is a Table: Deletes rows in-place (or creates a new table if new_name is provided).
    - If target is a View: Creates a new view excluding the rows that match the criteria.
    
    Args:
        target (str): The name of the table or view.
        criteria (str or list): The condition(s) defining which rows to REMOVE.
        new_name (str, optional): The name of the new table/view. Required for in-place view modifications.
        return_df (bool): If True, returns the updated result as a DataFrame.
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if isinstance(criteria, list):
        criteria_str = " AND ".join(f"({c})" for c in criteria)
    else:
        criteria_str = criteria
        
    # Pythonic fix: replace '==' with '=' for SQL compatibility
    criteria_str = criteria_str.replace("==", "=")
        
    # Check if target is a TABLE or a VIEW
    type_query = f"SELECT table_type FROM information_schema.tables WHERE table_name = '{target}'"
    target_type_result = _active_conn.execute(type_query).fetchone()
    
    if not target_type_result:
        raise ValueError(f"Target '{target}' does not exist in the database.")
        
    target_type = target_type_result[0] # 'BASE TABLE' or 'VIEW'
    
    if target_type == 'BASE TABLE':
        if new_name:
            # Create a new table keeping only rows that DO NOT match the criteria
            _active_conn.execute(f"CREATE OR REPLACE TABLE {new_name} AS SELECT * FROM {target} WHERE NOT ({criteria_str})")
            print(f"Successfully created new table '{new_name}' with specific rows removed.")
            final_target = new_name
        else:
            # Delete in-place directly from the table
            _active_conn.execute(f"DELETE FROM {target} WHERE {criteria_str}")
            print(f"Successfully deleted rows from table '{target}' where: {criteria_str}")
            final_target = target
            
    elif target_type == 'VIEW':
        if not new_name:
            new_name = f"{target}_edited"
            print(f"Note: Views cannot be modified in-place directly. Creating a new view named '{new_name}'.")
            
        # Create a new view keeping only rows that DO NOT match the criteria
        _active_conn.execute(f"CREATE OR REPLACE VIEW {new_name} AS SELECT * FROM {target} WHERE NOT ({criteria_str})")
        print(f"Successfully created new view '{new_name}' with specific rows removed.")
        final_target = new_name
        
    if return_df:
        return _active_conn.execute(f"SELECT * FROM {final_target}").df()
    



def rename_columns(operations):
    """
    Renames columns across multiple physical tables using a dictionary mapping.
    Skips views automatically as modifying view columns in-place is not a best practice.
    
    Args:
        operations (dict): A dictionary where keys are table names and values are the renaming rules.
                           Can accept: 
                           - A list for a single column: ["old_col", "new_col"]
                           - A dict for multiple columns: {"old_col1": "new_col1", "old_col2": "new_col2"}
                           - A list of lists: [["old1", "new1"], ["old2", "new2"]]
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if not isinstance(operations, dict):
        raise ValueError("The 'operations' parameter must be a dictionary (e.g., {'table_name': ['old', 'new']}).")
        
    for table, renames in operations.items():
        # 1. Check if table exists and if it's a VIEW
        type_query = f"SELECT table_type FROM information_schema.tables WHERE table_name = '{table}'"
        target_type_result = _active_conn.execute(type_query).fetchone()
        
        if not target_type_result:
            print(f"Warning: Target '{table}' does not exist in the database. Skipping.")
            continue
            
        if target_type_result[0] == 'VIEW':
            print(f"Skipping '{table}': Renaming columns directly on a VIEW is not supported. Please rename the columns in the underlying table instead.")
            continue

        # 2. Normalize the user's input format into a standard dictionary
        rename_dict = {}
        if isinstance(renames, dict):
            rename_dict = renames
        elif isinstance(renames, list):
            # Check if it's a simple list ["old", "new"]
            if len(renames) == 2 and isinstance(renames[0], str) and isinstance(renames[1], str):
                rename_dict = {renames[0]: renames[1]}
            # Check if it's a list of lists [["old1", "new1"], ["old2", "new2"]]
            else:
                for item in renames:
                    if isinstance(item, list) and len(item) == 2:
                        rename_dict[item[0]] = item[1]
        
        if not rename_dict:
            print(f"Warning: Invalid rename format provided for table '{table}'. Skipping.")
            continue
            
        # 3. Execute the renaming
        for old_col, new_col in rename_dict.items():
            try:
                _active_conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")
                print(f"Successfully renamed column in '{table}': '{old_col}' -> '{new_col}'")
            except Exception as e:
                print(f"Error renaming column '{old_col}' in '{table}': {e}")


def rename_tables(operations):
    """
    Renames multiple tables or views using a dictionary mapping.
    Automatically detects if the target is a TABLE or a VIEW and applies the correct SQL command.
    
    Args:
        operations (dict): A dictionary mapping old names to new names.
                           Example: {"old_table_name": "new_table_name"}
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    if not isinstance(operations, dict):
        raise ValueError("The 'operations' parameter must be a dictionary (e.g., {'old_name': 'new_name'}).")
        
    for old_name, new_name in operations.items():
        # Check if target exists and identify if it's a TABLE or VIEW
        type_query = f"SELECT table_type FROM information_schema.tables WHERE table_name = '{old_name}'"
        target_type_result = _active_conn.execute(type_query).fetchone()
        
        if not target_type_result:
            print(f"Warning: Target '{old_name}' does not exist in the database. Skipping.")
            continue
            
        target_type = target_type_result[0]
        
        try:
            if target_type == 'VIEW':
                _active_conn.execute(f"ALTER VIEW {old_name} RENAME TO {new_name}")
                print(f"Successfully renamed VIEW: '{old_name}' -> '{new_name}'")
            else:
                _active_conn.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
                print(f"Successfully renamed TABLE: '{old_name}' -> '{new_name}'")
        except Exception as e:
            print(f"Error renaming '{old_name}': {e}")



def import_df(df, table_name, columns=None, rows=None):
    """
    Imports a pandas DataFrame directly into DuckDB with optional row and column slicing.
    
    Args:
        df (pandas.DataFrame): The DataFrame to be imported.
        table_name (str): The name of the new table.
        columns (list or tuple, optional): 
            - Tuple (start, end) for a range slice (e.g., ("name", "occupation") -> loc, or (1, 5) -> iloc).
            - List ["col1", "col2"] for exact specific columns.
        rows (list or tuple, optional): 
            - Tuple or List [start, end] for row slicing (e.g., [1, 100] -> iloc, or ["id_1", "id_5"] -> loc).
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")
        
    # We work on a copy to avoid altering the user's original DataFrame in memory
    temp_df = df.copy()
    
    # 1. Handle Row Slicing
    if rows is not None:
        if isinstance(rows, (list, tuple)) and len(rows) == 2:
            start, end = rows
            # If integers, use iloc
            if isinstance(start, int) and isinstance(end, int):
                temp_df = temp_df.iloc[start:end]
            # If strings/labels, use loc
            else:
                temp_df = temp_df.loc[start:end]
        else:
            raise ValueError("The 'rows' parameter must be a list or tuple of exactly two elements: [start, end].")

    # 2. Handle Column Slicing/Selection
    if columns is not None:
        # If it's a tuple of 2 elements, we treat it as a RANGE (slice)
        if isinstance(columns, tuple) and len(columns) == 2:
            start, end = columns
            if isinstance(start, int) and isinstance(end, int):
                temp_df = temp_df.iloc[:, start:end]
            else:
                temp_df = temp_df.loc[:, start:end]
        # If it's a list, we treat it as EXACT column names
        elif isinstance(columns, list):
            # If list of integers, use iloc to select specific column indices
            if all(isinstance(x, int) for x in columns):
                temp_df = temp_df.iloc[:, columns]
            else:
                temp_df = temp_df[columns]
        else:
            raise ValueError("The 'columns' parameter must be a list (specific columns) or a tuple (range).")

    try:
        # Register the sliced DataFrame temporarily as a virtual view
        _active_conn.register("temp_df_view", temp_df)
        
        # Create a physical table from that virtual view
        _active_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df_view")
        
        # Unregister the temporary view to keep the memory clean
        _active_conn.unregister("temp_df_view")
        
        print(f"Successfully imported pandas DataFrame into table '{table_name}'. Final shape: {temp_df.shape}")
    except Exception as e:
        print(f"Error importing DataFrame: {e}")



def summarize(tables, on, join_types=None, columns=None, aggregations=None, order_by=None, create_table=None, create_view=None, return_df=True):
    """
    The ultimate EDA function: Joins multiple tables, automatically groups by selected columns, 
    applies alias-friendly aggregations, and sorts the final result.
    
    Args:
        tables (list): List of table names to join.
        on (list): Join conditions.
        join_types (list, optional): Join types. Defaults to 'inner' for all.
        columns (dict, optional): Dict of {table: [columns]} to select and GROUP BY automatically.
        aggregations (dict, optional): Dict of {alias: "SQL_AGG_FUNC"} (e.g. {"total_sales": "SUM(amount)"}).
        order_by (dict, optional): Dict for sorting (e.g. {"total_sales": "DESC"}).
        create_table (str, optional): Save result as a physical table.
        create_view (str, optional): Save result as a logical view.
        return_df (bool): Return pandas DataFrame (Default: True).
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")

    if not return_df and create_table is None and create_view is None:
        raise ValueError("ERROR: You set 'return_df=False' but didn't provide a name for 'create_table' or 'create_view'.")

    n_joins = len(tables) - 1
    
    if join_types is None:
        join_types = ["INNER"] * n_joins
    elif isinstance(join_types, str):
        join_types = [join_types] * n_joins

    # 1. Build SELECT and GROUP BY parts automatically
    select_cols = []
    group_cols = []
    
    if columns:
        for t_name, cols in columns.items():
            if isinstance(cols, str): cols = [cols]
            for c in cols:
                field = f"{t_name}.{c}"
                select_cols.append(field)
                group_cols.append(field)
                
    if aggregations:
        for alias, func in aggregations.items():
            select_cols.append(f"{func} AS {alias}")
            
    if not select_cols:
        select_cols.append("*")
        
    select_str = ", ".join(select_cols)
    group_str = ", ".join(group_cols)

    # 2. Build JOIN part
    query_parts = [f"FROM {tables[0]}"]
    
    for i in range(n_joins):
        t_right = tables[i+1]
        j_type = join_types[i].upper()
        if "JOIN" not in j_type: j_type += " JOIN"
            
        condition = on[i]
        if isinstance(condition, tuple):
            left_col, right_col = condition
            if left_col == right_col:
                join_clause = f"USING ({left_col})"
            else:
                join_clause = f"ON {left_col} = {t_right}.{right_col}"
        else:
            join_clause = f"USING ({condition})"
            
        query_parts.append(f"{j_type} {t_right} {join_clause}")
        
    from_str = " ".join(query_parts)

    # 3. Build ORDER BY part
    order_str = ""
    if order_by:
        order_clauses = []
        for col, direction in order_by.items():
            d_upper = direction.upper()
            if d_upper.startswith("ASC"): d_str = "ASC"
            elif d_upper.startswith("DESC"): d_str = "DESC"
            else: d_str = d_upper
            order_clauses.append(f"{col} {d_str}")
        order_str = "ORDER BY " + ", ".join(order_clauses)

    # 4. Assemble Final Query
    final_query = f"SELECT {select_str} {from_str}"
    if group_str:
        final_query += f" GROUP BY {group_str}"
    if order_str:
        final_query += f" {order_str}"

    # 5. Handle Outputs
    if create_table is not None:
        _active_conn.execute(f"CREATE OR REPLACE TABLE {create_table} AS {final_query}")
        print(f"Successfully created summary table '{create_table}'.")

    if create_view is not None:
        _active_conn.execute(f"CREATE OR REPLACE VIEW {create_view} AS {final_query}")
        print(f"Successfully created summary view '{create_view}'.")

    if return_df:
        return _active_conn.execute(final_query).df()



def window(table, operations, partition_by=None, order_by=None, frame_clause=None, create_table=None, create_view=None, return_df=True):
    """
    Applies advanced SQL Window Functions (Rolling averages, Cumulative sums, Ranking, Lags/Leads).
    
    Args:
        table (str): The name of the table or view.
        operations (dict): Dict of {alias: "SQL_FUNC"} (e.g., {"prev_day": "LAG(amount)"}).
        partition_by (str or list, optional): Columns to partition the window.
        order_by (dict, optional): Dict for ordering inside the window (e.g., {"date": "ASC"}).
        frame_clause (str, optional): Advanced frame limits (e.g., 'ROWS BETWEEN 6 PRECEDING AND CURRENT ROW').
    """
    global _active_conn
    if _active_conn is None:
        raise ConnectionError("No active database connection. Please use sp.connect() first.")

    # 1. Build PARTITION BY Clause
    partition_clause = ""
    if partition_by:
        if isinstance(partition_by, str): partition_by = [partition_by]
        partition_clause = "PARTITION BY " + ", ".join(partition_by)

    # 2. Build ORDER BY Clause
    order_clause = ""
    if order_by:
        order_clauses = []
        for col, direction in order_by.items():
            d_upper = direction.upper()
            d_str = "ASC" if d_upper.startswith("ASC") else "DESC"
            order_clauses.append(f"{col} {d_str}")
        order_clause = "ORDER BY " + ", ".join(order_clauses)

    # 3. Build FRAME Clause (The Senior Touch for Rolling metrics)
    frame_str = f" {frame_clause}" if frame_clause else ""

    # 4. Assemble the OVER() component
    over_core = f"{partition_clause} {order_clause}{frame_str}".strip()
    over_clause = f"OVER ({over_core})" if over_core else "OVER ()"

    # 5. Build SELECT columns (Keep all original columns with '*')
    select_cols = ["*"]
    for alias, func in operations.items():
        select_cols.append(f"{func} {over_clause} AS {alias}")

    select_str = ", ".join(select_cols)
    final_query = f"SELECT {select_str} FROM {table}"

    # 6. Handle Outputs
    if create_table is not None:
        _active_conn.execute(f"CREATE OR REPLACE TABLE {create_table} AS {final_query}")
        print(f"Successfully created windowed table '{create_table}'.")

    if create_view is not None:
        _active_conn.execute(f"CREATE OR REPLACE VIEW {create_view} AS {final_query}")
        print(f"Successfully created windowed view '{create_view}'.")

    if return_df:
        return _active_conn.execute(final_query).df()















