import argparse
import logging
import os
import sqlite3
import redis
import json
import yaml


def load_config():
    """Load configuration from config.yml and environment variables."""
    config_file = 'config.yml'
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    config['REDIS_HOST'] = os.getenv('REDIS_HOST', config['redis']['host'])
    config['REDIS_PORT'] = int(os.getenv('REDIS_PORT', config['redis']['port']))
    config['SQLITE_DB_PATH'] = os.getenv('SQLITE_DB_PATH', config['sqlite']['db_path'])
    config['LOG_LEVEL'] = os.getenv('LOG_LEVEL', config['log_level'])

    logging.info("Configuration loaded successfully.")
    return config


def connect_redis(host, port):
    """Connect to Redis."""
    return redis.Redis(host=host, port=port, decode_responses=True)


def connect_sqlite(db_path):
    """Connect to SQLite."""
    return sqlite3.connect(db_path)


def execute_queries(conn, queries):
    """Execute a list of queries and return results."""
    cursor = conn.cursor()
    results = {}

    for query_info in queries:
        name = query_info['name']
        query = query_info['query']
        try:
            cursor.execute(query)
            results[name] = {
                'data': cursor.fetchall(),
                'redis_key': query_info['redis_key']
            }
            logging.info(f"Query '{name}' executed successfully.")
        except sqlite3.Error as e:
            logging.error(f"Error executing query '{name}': {e}")
            results[name] = None

    return results


def save_results_to_redis(redis_client, results, dry_run):
    """Save query results to Redis."""
    for name, result in results.items():
        if result is None:
            logging.warning(f"Skipping saving results for query '{name}' due to errors.")
            continue

        redis_key = result['redis_key']
        data = result['data']
        if not dry_run:
            redis_client.set(redis_key, json.dumps(data))
            logging.info(f"Results for query '{name}' saved to Redis key '{redis_key}'.")
        else:
            logging.info(f"--dry-run enabled. Results for query '{name}' not saved to Redis.")


def main():
    config = load_config()

    # Argument parsing
    parser = argparse.ArgumentParser(description="Analyze logs from SQLite and save results to Redis.")
    parser.add_argument('--dry-run', action='store_true', help="Perform analysis without saving to Redis.")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=getattr(logging, config['LOG_LEVEL'].upper(), None),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging configured successfully.")

    # Connect to Redis and SQLite
    redis_client = connect_redis(config['REDIS_HOST'], config['REDIS_PORT'])
    with connect_sqlite(config['SQLITE_DB_PATH']) as conn:
        try:
            # Execute queries from config
            queries = config.get('queries', [])
            results = execute_queries(conn, queries)

            # Save results to Redis
            save_results_to_redis(redis_client, results, args.dry_run)
        except Exception as e:
            logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
