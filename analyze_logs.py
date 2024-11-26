import argparse
import logging
import os
import psycopg2
import redis
import json
import yaml
import decimal


def load_config():
    """Load configuration from config.yml and environment variables."""
    config_file = 'config.yml'
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    config['REDIS_HOST'] = os.getenv('REDIS_HOST', config['redis']['host'])
    config['REDIS_PORT'] = int(os.getenv('REDIS_PORT', config['redis']['port']))
    config['POSTGRES_HOST'] = os.getenv('POSTGRES_HOST', config['postgresql']['host'])
    config['POSTGRES_PORT'] = int(os.getenv('POSTGRES_PORT', config['postgresql']['port']))
    config['POSTGRES_DBNAME'] = os.getenv('POSTGRES_DBNAME', config['postgresql']['dbname'])
    config['POSTGRES_USER'] = os.getenv('POSTGRES_USER', config['postgresql']['user'])
    config['POSTGRES_PASSWORD'] = os.getenv('POSTGRES_PASSWORD', config['postgresql']['password'])
    config['LOG_LEVEL'] = os.getenv('LOG_LEVEL', config['log_level'])

    logging.info("Configuration loaded successfully.")
    return config


def connect_redis(host, port):
    """Connect to Redis."""
    return redis.Redis(host=host, port=port, decode_responses=True)


def connect_postgresql(host, port, dbname, user, password):
    """Connect to PostgreSQL."""
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn


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
        except psycopg2.Error as e:
            logging.error(f"Error executing query '{name}': {e}")
            results[name] = None

    return results

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)

def save_results_to_redis(redis_client, results, dry_run):
    """Save query results to Redis."""
    for name, result in results.items():
        if result is None:
            logging.warning(f"Skipping saving results for query '{name}' due to errors.")
            continue

        redis_key = result['redis_key']
        data = result['data']
        if not dry_run:
            try:
                redis_client.set(redis_key, json.dumps(data, cls=DecimalEncoder))
            except Exception as e:
                logging.error(f"Error saving results for query '{name}' to Redis: {e}; {data}")
            logging.info(f"Results for query '{name}' saved to Redis key '{redis_key}'.")
        else:
            logging.info(f"--dry-run enabled. Results for query '{name}' not saved to Redis.")


def main():
    config = load_config()

    # Argument parsing
    parser = argparse.ArgumentParser(description="Analyze logs from PostgreSQL and save results to Redis.")
    parser.add_argument('--dry-run', action='store_true', help="Perform analysis without saving to Redis.")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=getattr(logging, config['LOG_LEVEL'].upper(), None),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging configured successfully.")

    # Connect to Redis and PostgreSQL
    redis_client = connect_redis(config['REDIS_HOST'], config['REDIS_PORT'])
    with connect_postgresql(
        config['POSTGRES_HOST'],
        config['POSTGRES_PORT'],
        config['POSTGRES_DBNAME'],
        config['POSTGRES_USER'],
        config['POSTGRES_PASSWORD']
    ) as conn:
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
