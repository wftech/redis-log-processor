
# Redis to SQLite Log Collector

This script collects logs from a Redis list and saves them into a SQLite database. It is designed to process logs in real-time while supporting flexible configuration through command-line arguments, environment variables, and a YAML configuration file.

---

## Features

- **Real-Time Log Processing**: Continuously fetches logs from Redis and stores them in SQLite.
- **Dynamic Field Handling**: Automatically adapts the SQLite schema to include custom fields from the logs.
- **Error Handling**: Sanitizes invalid JSON logs and handles common parsing errors gracefully.
- **Configuration Flexibility**:
  - Command-line arguments
  - Environment variables
  - YAML configuration file (`config.yml`)
- **Log Rotation**: Removes logs older than 24 hours from the SQLite database.

---

## Requirements

- Python 3.8+
- Redis server
- SQLite

---

## Configuration

### YAML Config File (`config.yml`)

Create a `config.yml` file with the following structure:

```yaml
redis:
  host: "localhost"
  port: 6379

postgresql:
  host: postgres
  port: 5432
  dbname: mydatabase
  user: myuser
  password: mypassword

fields:
  - http_time
  - http_status
  - http_path

pause: 5
log_level: "INFO"

queries:
  - name: top_ips_by_cpu
    query: >
      SELECT http_remote_addr, 
             SUM(COALESCE(CAST(http_request_time AS numeric), 0)) AS total_time
      FROM logs
      WHERE 
          http_time::timestamp(6) > NOW() - INTERVAL '1 hour'
      GROUP BY http_remote_addr
      ORDER BY total_time DESC
      LIMIT 20
    redis_key: analysis:top_ips_by_cpu
  - name: top_ips_by_post
    query: >
      SELECT http_remote_addr, COUNT(*) AS post_count
      FROM logs
      WHERE http_method = 'POST'
      GROUP BY http_remote_addr
      ORDER BY post_count DESC
      LIMIT 20
    redis_key: analysis:top_ips_by_post
  - name: top_ip_ranges
    query: >
      SELECT SUBSTR(http_remote_addr, 1, LENGTH(http_remote_addr) - LENGTH(REPLACE(http_remote_addr, '.', '')) - 1) AS ip_range,
             COUNT(*) AS request_count
      FROM logs
      WHERE http_remote_addr LIKE '%.%.%.%'
      GROUP BY ip_range
      ORDER BY request_count DESC
      LIMIT 10
    redis_key: analysis:top_ip_ranges
```

- `redis.host` and `redis.port`: Redis server connection details.
- `postgresql`: PostgreSQL server connection details.
- `fields`: List of fields to extract from each log.
- `pause`: Time (in seconds) to wait between processing iterations.
- `log_level`: Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).
- `queries`: List of SQL queries to execute and export to Redis.

---

## Environment Variables

Override the configuration using environment variables:

- `REDIS_HOST`: Redis server host.
- `REDIS_PORT`: Redis server port.
- `POSTGRESQL_HOST`: PostgreSQL server host.
- `POSTGRESQL_PORT`: PostgreSQL server port.
- `POSTGRESQL_DBNAME`: PostgreSQL database name.
- `POSTGRESQL_USER`: PostgreSQL username.
- `POSTGRESQL_PASSWORD`: PostgreSQL password.
- `FIELDS`: Comma-separated list of fields to extract.
- `PAUSE`: Time (in seconds) to wait between processing iterations.
- `LOG_LEVEL`: Logging level.

---

## Command-Line Arguments

Override configuration using command-line arguments:

```bash
python main.py [options]
```

### Options

- `-r, --redis`: Redis server host.
- `-p, --port`: Redis server port.
- `-d, --db`: PostgreSQL database name
- `-u`, `--user`: PostgreSQL username.
- `-w`, `--password`: PostgreSQL password.
- `-P`, `--postgres_port`: PostgreSQL port.
- `-H`, `--postgres_host`: PostgreSQL host.
- `-f, --fields`: Comma-separated list of fields to extract.
- `-t, --time`: Time (in seconds) to wait between processing iterations.
- `-l, --log_level`: Logging level.

---

## Usage

### Example Command

```bash
python main.py -r localhost -p 6379 -d mydatabase -u myuser -w mypassword -P 5432 -H postgres -f http_time,http_status,http_path -t 5 -l INFO 
```

### Running the Script

1. Prepare your `config.yml`.
2. Start the Redis server.
3. Run the script:

```bash
python main.py
```

### Stopping the Script

Press `CTRL+C` to terminate.

---

## Database Schema

The SQLite database consists of two tables:

### `logs`
Stores the processed log data.

| Column Name  | Type    | Description                     |
|--------------|---------|---------------------------------|
| `id`         | INTEGER | Auto-increment primary key.    |
| `created_at` | TIMESTAMP | Log creation timestamp.       |
| `<fields>`   | TEXT    | Dynamic columns based on config.|

---

## Error Handling

- Invalid JSON logs (e.g., missing values) are sanitized before parsing.
- Parsing errors are logged, and the script continues processing the next log.

---

## Logging

Logs are saved to the console with the specified logging level. Example format:

```
2024-11-19 10:00:00 - INFO - 100 logs saved.
2024-11-19 10:05:00 - WARNING - Column http_status already exists.
```

---

## Customization

### Adding Fields
This script supports extracting custom fields from logs. Add the field names to the `fields` list in the configuration. 

---

# Log Analysis and Redis Exporter

This script analyzes logs stored in an SQLite database and saves the results to Redis. 
The script supports executing predefined SQL queries and exporting the results to Redis keys.

## Requirements

- Python 3.x
- `redis` Python package
- `sqlite3` Python module
- `pyyaml` Python package

## Configuration

The script's configuration is stored in the `config.yml` file. Here is an example configuration:

```yaml
redis:
  host: 'localhost'
  port: 6379

postgresql:
  host: postgres
  port: 5432
  dbname: mydatabase
  user: myuser
  password: mypassword

log_level: 'INFO'

queries:
  - name: 'Top 20 IPs by CPU time'
    query: |
      SELECT http_remote_addr, SUM(http_request_time) AS total_time
      FROM logs
      WHERE http_time > datetime('now', '-1 hour')
      GROUP BY http_remote_addr
      ORDER BY total_time DESC
      LIMIT 20
    redis_key: 'analysis:top_ips_by_cpu'
  
  - name: 'Top 20 IPs by POST requests'
    query: |
      SELECT http_remote_addr, COUNT(*) AS post_count
      FROM logs
      WHERE http_method = 'POST'
      GROUP BY http_remote_addr
      ORDER BY post_count DESC
      LIMIT 20
    redis_key: 'analysis:top_ips_by_post'
  
  - name: 'Top 10 IP ranges by requests'
    query: |
      SELECT SUBSTR(http_remote_addr, 1, LENGTH(http_remote_addr) - LENGTH(REPLACE(http_remote_addr, '.', '')) - 1) AS ip_range,
             COUNT(*) AS request_count
      FROM logs
      WHERE http_remote_addr LIKE '%.%.%.%'
      GROUP BY ip_range
      ORDER BY request_count DESC
      LIMIT 10
    redis_key: 'analysis:top_ip_ranges'
```

## Usage

### Run the script:

To execute the analysis and export the results to Redis:

```bash
python analyze_logs.py
```

This will execute all queries defined in the `config.yml` file and save the results to Redis.

### Dry-run mode:

If you don't want to save the results to Redis but only want to see the output, you can enable dry-run mode with the `--dry-run` flag:

```bash
python analyze_logs.py --dry-run
```

This will run the analysis but not save any data to Redis.


## Development
- use `docker-compose up` to start the redis and postgres services
- if you want to use fixtures, enter the redis-logger container (as root) and run `pip install faker` and `python generate_fixtures.py` to generate logs
- script `generate_fixtures.py` will generate 1000 logs and push them to the redis list
- this script will not work without installing the `faker` package, this is only for testing purposes (failed if accidentally run in production)
