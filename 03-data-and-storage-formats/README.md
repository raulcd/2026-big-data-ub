# Storage Formats Practice: E-commerce Orders

## Setup

From your local git repository:

```bash
cd 03-data-and-storage-formats
```

### Build and start the environment

Review the `Dockerfile` and `compose.yaml` files.

```bash
docker compose up -d --build
```

The above starts a Jupyter notebook server with Python, PyArrow, DuckDB, and Pandas
pre-installed. Open your browser at http://localhost:8888.

### Download the dataset

We are going to be using a synthetic e-commerce orders dataset with **10 million rows**.

We can generate it via the following command but it takes several minutes.
```python
docker compose exec jupyter python generate_data.py
```

You can dowload it from huggingface. We will see that on the next section.


---

## Exercises

From the browser notebook. Create a new notebook in the `notebooks/` directory for your work.

Once inside Jupyter, run this as the first cell of your notebook to download the dataset:

```python
import urllib.request
url = "https://huggingface.co/datasets/BigDataUB/synthetic-columnar/resolve/main/orders.csv"
print("Downloading dataset (~830MB, this will take a few minutes)...")
urllib.request.urlretrieve(url, "/app/data/orders.csv")
print("Done!")
```

### Exercise 1: Explore the CSV

Start by importing the libraries we'll use throughout the practice:

```python
import os
import time
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import duckdb
import pandas as pd
```

Check the version of pyarrow, pandas and duckdb:

Example:

```python
pa.__version__
```

#### Reading CSV files with PyArrow

PyArrow provides `pyarrow.csv.read_csv()` to read CSV files. It returns an Arrow
**Table** - a columnar, in-memory data structure (similar to a Pandas DataFrame,
but stored in columnar format).

An Arrow Table has useful attributes:
- `table.schema` --> the column names and data types
- `table.num_rows` --> number of rows
- `table.num_columns` --> number of columns

Documentation: https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html

**Q1.** Read the CSV file at `/app/data/orders.csv` with PyArrow. Print its schema
and how many rows and columns it has.

Do it in separate cells so you can inspect the data and what each of those return.

<details>
<summary><b>Hint 1</b></summary>

```python
table = pcsv.read_csv("/app/data/orders.csv")
```
</details>

<details>
<summary><b>Hint 2</b></summary>

```python
print(table.schema)
print(f"Rows: {table.num_rows:,}")
print(f"Columns: {table.num_columns}")
```
</details>

#### Measuring file sizes and read times

Throughout this practice, we'll compare formats by measuring file sizes and read
times. Python's `os.path.getsize()` returns the size of a file in bytes, and
`time.time()` returns the current time in seconds, by taking the difference
before and after an operation, we can measure how long it takes.

**Q2.** How large is the CSV file on disk (in MB)?

<details>
<summary><b>Hint</b></summary>

```python
csv_size = os.path.getsize("/app/data/orders.csv")
print(f"CSV size: {csv_size / (1_000_000):.1f} MB")
```
</details>

**Q3.** How long does it take to read the full CSV? Measure the time using
`time.time()` before and after the read.

<details>
<summary><b>Hint</b></summary>

```python
start = time.time()
table = pcsv.read_csv("/app/data/orders.csv")
csv_read_time = time.time() - start
print(f"CSV read time: {csv_read_time:.2f}s")
```
</details>


<details>
<summary><b>Hint</b></summary>

Another possibility could be:

```python
%timeit table = pcsv.read_csv("/app/data/orders.csv")
```
</details>

### Exercise 2: Format conversion - CSV → JSON → Parquet

In this exercise we'll convert the same data between three formats (CSV, JSON,
Parquet) and compare their sizes. This helps illustrate why format choice matters.

#### Writing JSON with Pandas

PyArrow can read JSON (`pyarrow.json.read_json`) but does not have a JSON writer.
To write JSON, we need to convert the Arrow Table to a Pandas DataFrame first.

Arrow Tables have a `.to_pandas()` method that converts to a DataFrame. You can
also take a subset of rows with `.slice(offset, length)`, for example,
`table.slice(0, 100_000)` returns the first 100,000 rows as a new Arrow Table.

**Q4.** Convert the CSV to JSON Lines format (one JSON object per line). Write a
sample of 100,000 rows first, full JSON conversion is very large. Can be done but
it will take a while on your laptop ~3-4 minutes.

Check the Pandas documentation for `DataFrame.to_json`:
https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html

Use `orient=records` and `lines=True` for writing a JSON record per line otherwise
it will write it as if column oriented and we won't be able to see anything.

<details>
<summary><b>Hint</b></summary>

```python
df_sample = table.slice(0, 100_000).to_pandas()
df_sample.to_json("/app/data/orders_sample.json", orient="records", lines=True)
```
</details>

#### Writing Parquet with PyArrow

PyArrow provides `pyarrow.parquet.write_table()` to write an Arrow Table to a
Parquet file. The function takes at minimum two arguments: the table and the
output file path. By default, it applies a compression algorithm, but which one?

Documentation: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html

**Q5.** Write the full dataset as Parquet using the default compression. How long
does it take?

<details>
<summary><b>Hint</b></summary>

```python
start = time.time()
pq.write_table(table, "/app/data/orders_default.parquet")
parquet_write_time = time.time() - start
print(f"Parquet write time: {parquet_write_time:.2f}s")
```
</details>

#### Inspecting Parquet metadata

Every Parquet file contains metadata that describes its structure: how many row
groups, which columns, what compression was used, min/max statistics, etc. PyArrow
can read this metadata without reading the actual data using `pq.read_metadata()`.

The metadata object has a `.row_group(i)` method to inspect individual row groups,
and each row group has a `.column(j)` method to see column-level details.

**Q6.** What compression algorithm did PyArrow use by default? Use
`pq.read_metadata` to inspect the file and find out:

```python
metadata = pq.read_metadata("/app/data/orders_default.parquet")
print(metadata)
```

Look at the row group metadata. Can you find the compression codec?

<details>
<summary><b>Hint</b></summary>

```python
print(metadata.row_group(0).column(0))
```

Look for the `compression` field in the output.
</details>

#### Comparing file sizes

**Q7.** Compare file sizes. Fill in the table:

Note: the JSON file only has 100k rows, so the comparison is not direct. To
get an estimate for the full dataset, multiply the JSON size by the ratio of
total rows to 100k (i.e. ×10 for 1M rows, ×100 for 10M rows).

| Format | File Size (MB) | Estimated Full Size (MB) | Ratio vs CSV |
|--------|---------------|--------------------------|--------------|
| CSV    |               |                          | 1.0x         |
| JSON (100k rows) |    |                          |              |
| Parquet (default) |   |                          |              |

<details>
<summary><b>Hint</b></summary>

```python
for name, path in [("CSV", "orders.csv"), ("JSON (100k)", "orders_sample.json"),
                    ("Parquet (default)", "orders_default.parquet")]:
    size = os.path.getsize(f"/app/data/{path}")
    print(f"{name}: {size / 1e6:.1f} MB")
```
</details>

### Exercise 3: Compression comparison

Parquet supports several compression algorithms. Each one makes a different
trade-off between **file size**, **write speed**, and **read speed**.

You can choose the compression when writing with `pq.write_table()` by passing the
`compression` parameter. Common options are:
- `"snappy"` --> fast compression/decompression, moderate ratio (the default)
- `"gzip"` --> slow compression, good ratio
- `"zstd"` --> fast compression, very good ratio (often the best balance)
- `"none"` --> no compression at all

**Q8.** Write the dataset as Parquet with each compression algorithm and compare
file sizes and write times:

The last compressing algorithm (gzip) will take quite a while (~3 minutes). That's expected.
You can remove it from the list or wait. I'd recommend waiting for full comparison.

<details>
<summary><b>Hint</b></summary>

```python
for codec in ["snappy", "zstd", "none", "gzip"]:
    start = time.time()
    pq.write_table(table, f"/app/data/orders_{codec}.parquet", compression=codec)
    elapsed = time.time() - start
    size = os.path.getsize(f"/app/data/orders_{codec}.parquet")
    print(f"{codec:8s}: {size / 1e6:8.1f} MB  write: {elapsed:.2f}s")
```
</details>

**Q9.** Now compare read times for each compression. Which compression gives the
best balance of size and speed?

<details>
<summary><b>Hint</b></summary>

```python
for codec in ["snappy", "zstd", "none", "gzip"]:
    start = time.time()
    t = pq.read_table(f"/app/data/orders_{codec}.parquet")
    elapsed = time.time() - start
    print(f"{codec:8s}: read {elapsed:.2f}s")
```
</details>

### Exercise 4: Column pruning

One of the key advantages of columnar formats is **column pruning**: reading only
the columns you need. In a row-oriented format (CSV), you must read every column
for every row even if you only need two of them. In Parquet, each column is stored
independently, so the reader can skip columns entirely.

With `pq.read_table()`, pass the `columns` parameter with a list of column names
to read only those columns. For example:

```python
pq.read_table("file.parquet", columns=["col_a", "col_b"])
```

**Q10.** Read only the `category` and `total_amount` columns from the Parquet file.
Compare the read time to reading all columns.

<details>
<summary><b>Hint</b></summary>

```python
start = time.time()
t_all = pq.read_table("/app/data/orders_default.parquet")
time_all = time.time() - start

start = time.time()
t_pruned = pq.read_table("/app/data/orders_default.parquet",
                          columns=["category", "total_amount"])
time_pruned = time.time() - start

print(f"All columns: {time_all:.2f}s")
print(f"2 columns:   {time_pruned:.2f}s")
```
</details>

Why is reading fewer columns faster in Parquet but wouldn't make a difference in CSV?

### Exercise 5: Partitioned Parquet

When working with large datasets, **partitioning** organizes data into separate
directories based on column values. For example, partitioning by year creates:

```
orders_partitioned/
├── year=2020/
│   └── data.parquet
├── year=2021/
│   └── data.parquet
└── ...
```

When you query `WHERE year = 2024`, the reader only opens the `year=2024/`
directory, skipping all other years entirely. This is called **partition pruning**.

To write partitioned Parquet, use `pq.write_to_dataset()` instead of
`pq.write_table()`. It takes a `partition_cols` parameter with a list of column
names to partition by.

#### Adding a year column with PyArrow compute

Our data has `order_date` as a `date32` column (PyArrow auto-detects it when
reading the CSV). PyArrow provides compute functions in `pyarrow.compute`
(usually imported as `pc`) to work with dates:

- `pc.year(date_column)` --> extract the year from a date column
- `table.append_column("name", array)` --> add a new column to a table

Documentation: https://arrow.apache.org/docs/python/compute.html

**Q11.** Write the dataset as partitioned Parquet, partitioned by `year` extracted
from `order_date`. You need to add a `year` column first to the table.

<details>
<summary><b>Hint 1</b></summary>

```python
import pyarrow.compute as pc

years = pc.year(table.column("order_date"))
table_with_year = table.append_column("year", years)
```
</details>

<details>
<summary><b>Hint 2</b></summary>

Then write the partitioned dataset:

```python
pq.write_to_dataset(table_with_year, "/app/data/orders_partitioned",
                     partition_cols=["year"])
```
</details>

<details>
<summary><b>Answer</b></summary>

```python
import pyarrow.compute as pc

years = pc.year(table.column("order_date"))
table_with_year = table.append_column("year", years)

pq.write_to_dataset(table_with_year, "/app/data/orders_partitioned",
                     partition_cols=["year"])
```
</details>

**Q12.** Look at the directory structure created. How many subdirectories are there?
What are they named?

```bash
ls /app/data/orders_partitioned/
```

#### Reading partitioned datasets

To read a partitioned dataset, use `pyarrow.dataset` (usually imported as `ds`).
The `ds.dataset()` function reads the directory structure and understands the
`key=value` naming convention (called "hive" partitioning). You can then filter
data using `dataset.to_table(filter=...)`.

```python
import pyarrow.dataset as ds

dataset = ds.dataset("/app/data/orders_partitioned", partitioning="hive")
# Read only one partition:
subset = dataset.to_table(filter=ds.field("year") == 2024)
```

**Q13.** Read only orders from 2024 using partition pruning. Compare the read time
to reading the full non-partitioned file and filtering in memory.

<details>
<summary><b>Hint</b></summary>

```python
import pyarrow.dataset as ds
import pyarrow.compute as pc

# With partition pruning (only reads year=2024 directory)
start = time.time()
dataset = ds.dataset("/app/data/orders_partitioned", partitioning="hive")
t_pruned = dataset.to_table(filter=ds.field("year") == 2024)
time_pruned = time.time() - start

# Without pruning (reads everything, filters in memory)
start = time.time()
t_all = pq.read_table("/app/data/orders_default.parquet")
t_filtered = t_all.filter(pc.equal(pc.year(t_all.column("order_date")), 2024))
time_full = time.time() - start

print(f"With partition pruning: {time_pruned:.2f}s ({t_pruned.num_rows:,} rows)")
print(f"Without pruning:       {time_full:.2f}s ({t_filtered.num_rows:,} rows)")
```
</details>

### Exercise 6: Inspect Parquet metadata with DuckDB

DuckDB is an embedded analytical database (like SQLite, but optimized for
analytics). It can query Parquet and CSV files directly using SQL, without loading
the data into memory first. Just pass the file path as a string in the `FROM`
clause.

```python
import duckdb
con = duckdb.connect()
con.sql("SELECT count(*) FROM 'file.parquet'").show()
```

DuckDB also provides special functions to inspect Parquet file internals:
- `parquet_metadata('file.parquet')` --> shows row groups, column chunks, sizes
- `parquet_schema('file.parquet')` --> shows the schema stored in the file

Documentation: https://duckdb.org/docs/data/parquet/overview

**Q14.** Use DuckDB to query the Parquet file directly. Count the total number of
rows and find the top 5 categories by total revenue.

<details>
<summary><b>Hint</b></summary>

```python
con = duckdb.connect()

# Count rows
con.sql("SELECT count(*) FROM '/app/data/orders_default.parquet'").show()

# Top 5 categories by revenue
con.sql("""
    SELECT category, SUM(total_amount) as revenue
    FROM '/app/data/orders_default.parquet'
    GROUP BY category
    ORDER BY revenue DESC
    LIMIT 5
""").show()
```
</details>

**Q15.** Use DuckDB's `parquet_metadata` function to inspect the Parquet file
internals:

```python
con.sql("SELECT * FROM parquet_metadata('/app/data/orders_default.parquet') LIMIT 10").show()
```

What information do you see? Can you find the row group sizes?

**Q16.** Use `parquet_schema` to see the schema:

```python
con.sql("SELECT * FROM parquet_schema('/app/data/orders_default.parquet')").show()
```

**Q17.** Run the same query (average `total_amount` per `category`) on both the CSV
and the Parquet file using DuckDB. Compare the execution times.

<details>
<summary><b>Hint</b></summary>

```python
# On CSV
start = time.time()
con.sql("""
    SELECT category, AVG(total_amount)
    FROM '/app/data/orders.csv'
    GROUP BY category
""").show()
csv_time = time.time() - start

# On Parquet
start = time.time()
con.sql("""
    SELECT category, AVG(total_amount)
    FROM '/app/data/orders_default.parquet'
    GROUP BY category
""").show()
parquet_time = time.time() - start

print(f"CSV:     {csv_time:.2f}s")
print(f"Parquet: {parquet_time:.2f}s")
```
</details>

### Exercise 7: PyArrow - zero-copy and interoperability

Apache Arrow defines a standard **in-memory columnar format**. This means
different tools (PyArrow, Pandas, DuckDB, Polars, Spark) can share data without
copying it - they all understand the same memory layout.

- `table.to_pandas()` - converts an Arrow Table to a Pandas DataFrame
- `table.nbytes` - the total memory size of the table in bytes
- DuckDB can query Arrow Tables directly by referencing the Python variable name
  in SQL (e.g., `SELECT * FROM my_table` where `my_table` is a Python variable)

**Q18.** Read the Parquet file into an Arrow Table and then convert it to a Pandas
DataFrame. Measure the time for each step separately. Also check how much memory
the Arrow Table uses with `table.nbytes`.

<details>
<summary><b>Hint</b></summary>

```python
# Arrow Table
start = time.time()
table = pq.read_table("/app/data/orders_default.parquet")
arrow_time = time.time() - start

# Arrow --> Pandas
start = time.time()
df = table.to_pandas()
pandas_time = time.time() - start

print(f"Parquet --> Arrow: {arrow_time:.2f}s")
print(f"Arrow --> Pandas:  {pandas_time:.2f}s")
print(f"Arrow memory:    {table.nbytes / 1e6:.0f} MB")
```
</details>

**Q19.** Query the Arrow Table directly from DuckDB, count the number of
orders per category. You don't need to load or convert anything, just reference the
Python variable name in the SQL `FROM` clause.

Note: `table` is a reserved keyword in SQL — name your Arrow Table variable
something like `arrow_table` or `orders` to avoid a parser error.

<details>
<summary><b>Hint</b></summary>

```python
arrow_table = pq.read_table("/app/data/orders_default.parquet")

con = duckdb.connect()
result = con.sql("SELECT category, count(*) as cnt FROM arrow_table GROUP BY category ORDER BY cnt DESC")
result.show()
```
</details>

DuckDB can query Arrow Tables directly, no copy needed. This is zero-copy
interoperability via the Arrow format.

---

### Summary

Fill in your observations:

| Metric | CSV | JSON | Parquet (snappy) | Parquet (zstd) |
|--------|-----|------|------------------|----------------|
| File size (MB) | | | | |
| Read time (s) | | | | |
| 2-column read (s) | | | | |
| DuckDB query (s) | | | | |

Key takeaways:
1. Columnar formats (Parquet) are much smaller than row formats (CSV, JSON)
2. Column pruning only reads the columns you need
3. Partitioning lets you skip entire chunks of data
4. DuckDB + Parquet is an efficient combination for analytics
5. Arrow enables zero-copy data sharing between tools

---

### Cleanup

```bash
docker compose down
```

To also remove generated data:

```bash
docker compose down -v
rm -rf data/
```
