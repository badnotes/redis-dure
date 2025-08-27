# Redis-dure

A powerful command-line tool for exporting and importing Redis data between clusters or instances.

## Overview

Redis-dure is a Go-based utility designed to facilitate seamless data migration between Redis instances or clusters. It supports exporting Redis data to JSON files and importing data from JSON files to Redis, while preserving data types, values, and expiration times.

## Features

- **Data Export**: Export Redis data to JSON files with support for key pattern filtering
- **Data Import**: Import data from JSON files to Redis with batch processing optimization
- **Multi-type Support**: Handles all major Redis data types (string, list, set, zset, hash)
- **Cluster Aware**: Works with both Redis standalone instances and clusters
- **Flexible Options**: Support for overwriting existing keys or skipping them during import
- **Expiration Preservation**: Maintains key expiration times during export/import
- **Pattern Matching**: Use wildcards or regular expressions to filter keys during export

## Installation

1. Ensure you have Go installed (version 1.16 or higher)
2. Clone this repository
3. Build the binary

```bash
# Clone the repository
git clone https://github.com/yourusername/redis-dure.git
cd redis-dure

# Build the binary
go build -o redis-dure
```

## Usage

### Command Line Arguments

```
Options:
  -addrs string
        Redis addresses, multiple addresses separated by commas (cluster mode) (default "localhost:6379")
  -password string
        Redis password
  -db int
        Redis database (non-cluster mode) (default 0)
  -export
        Export mode
  -import
        Import mode
  -output string
        Export file path (default "redis_data.json")
  -input string
        Import file path (default "redis_data.json")
  -pattern string
        Key matching pattern (supports regular expressions) (default "*")
  -cluster
        Force cluster mode
  -overwrite
        Overwrite existing keys (default true)
  -batch int
        Import batch size (optimize speed, number of keys per batch) (default 100)
  -skip-existing
        Skip existing keys during import
```

### Export Data

To export data from a Redis instance or cluster:

```bash
# Export all keys from a local Redis instance
./redis-dure -export -addrs localhost:6379

# Export keys matching a pattern from a Redis cluster
./redis-dure -export -addrs redis1:6379,redis2:6379,redis3:6379 -pattern "user:*" -output users.json

# Export with authentication
./redis-dure -export -addrs localhost:6379 -password yourpassword -output data.json
```

### Import Data

To import data into a Redis instance or cluster:

```bash
# Import data to a local Redis instance
./redis-dure -import -addrs localhost:6379

# Import to a Redis cluster with batch processing
./redis-dure -import -addrs redis1:6379,redis2:6379,redis3:6379 -input data.json -batch 500

# Import without overwriting existing keys
./redis-dure -import -addrs localhost:6379 -input data.json -overwrite=false

# Import and skip existing keys completely
./redis-dure -import -addrs localhost:6379 -input data.json -skip-existing
```

## Data Format

Exported data is stored in a JSON format with each line representing a Redis key and its associated data:

```json
{
  "key": "user:1001",
  "type": "hash",
  "value": {
    "name": "John Doe",
    "email": "john@example.com"
  },
  "expiration": 3600
}
{
  "key": "recent:posts",
  "type": "list",
  "value": ["post:101", "post:102", "post:103"],
  "expiration": 0
}
```

- `key`: The Redis key name
- `type`: The Redis data type (string, list, set, zset, hash)
- `value`: The value associated with the key, formatted according to the data type
- `expiration`: Time-to-live in seconds (0 means no expiration)

## Notes

- When working with large datasets, using the `-batch` option with a higher value can significantly improve import performance
- For Redis clusters, providing multiple addresses helps with automatic cluster discovery
- Regular expression patterns for key filtering should follow Go's regex syntax
- The tool automatically detects cluster mode when multiple addresses are provided, but you can force cluster mode with the `-cluster` flag

## License

[MIT](LICENSE)


