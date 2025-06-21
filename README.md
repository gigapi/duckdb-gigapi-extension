# GigAPI DuckDB Extension

This extension provides a table function `gigapi()` that allows querying time-series data indexed by GigAPI's Redis-based metadata backend.

## Overview

The `gigapi` extension works by intercepting a SQL query, parsing it to identify the target measurement (table), and then querying a Redis instance to discover the underlying data files (e.g., Parquet files on S3). It then rewrites the SQL query to read directly from these files, abstracting the data location from the user.

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

Once the secret is configured, you can use the `gigapi()` table function to query your data. The argument to the function is a standard SQL `SELECT` query string.

### Example

```sql
-- Load the extension
INSTALL 'gigapi';
LOAD 'gigapi';

-- Create the Redis secret
CREATE SECRET gigapi (
    TYPE redis,
    HOST '127.0.0.1',
    PORT '6379',
    PASSWORD 'eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81'
);

-- Query a measurement using the gigapi table function
SELECT * FROM gigapi('SELECT * FROM my_measurement WHERE time > now() - interval ''1 hour''');
```

The extension will automatically perform the following steps:
1. Parse the `SELECT` query.
2. Extract the table name `my_measurement`.
3. Connect to the Redis instance defined in the `gigapi` secret.
4. Fetch the list of data files from the Redis key `giga:idx:ts:my_measurement`.
5. Rewrite the query to be `SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet', ...]) WHERE time > now() - interval '1 hour'`.
6. Execute the rewritten query and return the results.
