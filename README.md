# <img src="https://github.com/user-attachments/assets/5b0a4a37-ecab-4ca6-b955-1a2bbccad0b4" />

# <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=25 /> GigAPI DuckDB Extension

This extension provides transparent, metadata-driven query support for [GigAPI](https://github.com/gigapi)

## Overview

The `gigapi` extension seamlessly rewrites DuckDB SQL queries using GigAPI metadata indices.

- If an index is found, the extension dynamically rewrites the query to read the specific data files (e.g., Parquet files on S3) relevant to the query's time range and other filters.
- If no index is found, the query is passed on to DuckDB's default planner, allowing you to work with regular tables as usual.

## Configuration

To use this extension, you must first configure a secret in DuckDB to store the connection details for your Redis instance. The extension will look for a `redis` type secret with the name `gigapi`.

### Creating the Secret

You can create the secret using the following SQL command. Replace the values for `host`, `port`, and `password` with your Redis instance's details.

```sql
CREATE SECRET gigapi (
    TYPE redis,
    HOST 'localhost',
    PORT '6379',
    PASSWORD 'your-password'
);
```

**Parameters:**

- `TYPE`: Must be `redis`.
- `HOST`: The hostname or IP address of your Redis server. (Default: 'localhost')
- `PORT`: The port number for your Redis server. (Default: '6379')
- `PASSWORD`: The password for your Redis server. (Optional)


## Usage: `gigapi()` Table Function

The primary way to use the extension is via the `gigapi()` table function. You pass a complete SQL query as a string to this function. The extension will then rewrite it using the metadata from Redis and execute it.

### Example

```sql
-- Load the extension
INSTALL 'gigapi';
LOAD 'gigapi';

-- Create the Redis secret for the GigAPI backend
CREATE SECRET gigapi (
    TYPE redis,
    HOST '127.0.0.1',
    PORT '6379',
    PASSWORD ''
);

-- Use the gigapi() table function to run a query
SELECT * FROM gigapi('SELECT * FROM my_measurement WHERE time > now() - interval ''1 hour''');
```

Behind the scenes, the extension will perform the following steps:
1. Parse the inner `SELECT` query.
2. Check Redis for a key named `giga:idx:ts:my_measurement`.
3. If the key exists, extract the time range from the `WHERE` clause.
4. Fetch the relevant list of data files from the Redis sorted set.
5. Rewrite the query to be `SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet', ...]) WHERE time > now() - interval '1 hour'`.
6. Pass the rewritten query to the DuckDB planner for execution.

## Transparent Query Hijacking with `GIGAPI`

As a more powerful alternative to the `gigapi()` table function, you can use the `GIGAPI` keyword at the beginning of any query. This will trigger the same query rewriting logic but allows you to use standard SQL syntax without wrapping your query in a string.

### How it Works
Any query prefixed with `GIGAPI ` will be automatically intercepted by the extension's query planner. The planner then rewrites the query based on the metadata found in Redis, just like the `gigapi()` function does.

### Example

```sql
-- The same query, but using the GIGAPI keyword for transparent hijacking.
GIGAPI SELECT * FROM my_measurement WHERE time > now() - interval '1 hour';
```

Behind the scenes, the extension performs the same steps as the `gigapi()` table function, rewriting the query to read from specific data files before execution. If a query is not prefixed with `GIGAPI`, it will be handled by DuckDB's default planner.

## ATTACH Support for GigAPI

The extension supports DuckDB's `ATTACH` statement, allowing you to create a virtual schema that pipes all table references through the GigAPI metadata engine. This enables seamless integration with tools and workflows that expect standard DuckDB schemas and tables.

### Usage

```sql
-- Attach a GigAPI database as schema 'bq'
ATTACH 'mydb' AS bq (TYPE gigapi, READ_ONLY);

-- List all tables in the attached GigAPI schema
SHOW TABLES;

-- Query a table from the attached schema
SELECT * FROM bq.example;
```

**How it works:**
- Any table reference in the attached schema (e.g., `bq.example`) is transparently routed through the `gigapi()` table function.
- You do not need to manually create views or enumerate tables; the extension handles all resolution dynamically.
- Metadata queries (e.g., `SHOW TABLES`, `DESCRIBE`, etc.) are also supported and routed through GigAPI.

This makes it easy to use GigAPI-backed data as if it were a native DuckDB database.

## Developer Information

### Dry Run Function

For debugging and development, the extension provides a scalar function `gigapi_dry_run(sql_query)` that shows you how a query would be rewritten without actually connecting to Redis. It uses a dummy list of Parquet files in its place.

**Example:**
```sql
SELECT gigapi_dry_run('SELECT * FROM my_table WHERE value > 10');
```

**Output:**
```
SELECT * FROM read_parquet(['dummy/file1.parquet', 'dummy/file2.parquet']) WHERE ("value" > 10)
```
