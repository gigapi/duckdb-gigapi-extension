# GigAPI DuckDB Extension

This extension provides transparent, metadata-driven query acceleration for time-series data indexed by GigAPI's Redis-based backend.

## Overview

The `gigapi` extension works by seamlessly intercepting SQL queries. When a query is made against a table, the extension first checks a Redis instance to see if a GigAPI index exists for that table.

- If an index is found, the extension dynamically rewrites the query to read the specific data files (e.g., Parquet files on S3) relevant to the query's time range and other filters.
- If no index is found, the query is passed on to DuckDB's default planner, allowing you to work with regular tables as usual.

This provides a powerful query acceleration layer without changing how you write your SQL.

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


## Usage

Once the secret is configured, you can query your GigAPI-indexed tables as if they were regular tables directly in DuckDB. There are no special functions to call.

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
    PASSWORD 'eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81'
);

-- Directly query a measurement. The extension will handle the rest.
SELECT * FROM my_measurement WHERE time > now() - interval '1 hour';
```

Behind the scenes, the extension will automatically perform the following steps:
1. Intercept the `SELECT` query.
2. Check Redis for a key named `giga:idx:ts:my_measurement`.
3. If the key exists, extract the time range from the `WHERE` clause.
4. Fetch the relevant list of data files from the Redis sorted set.
5. Rewrite the query to be `SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet', ...]) WHERE time > now() - interval '1 hour'`.
6. Pass the rewritten query to the DuckDB planner for execution.

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
