import argparse
import json
import logging
import os
import re
import psycopg2
import time
import redis
import yaml


def load_config():
    """Loads configuration from a YAML file and environment variables."""
    config_file = 'config.yml'
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    config.update({
        'REDIS_HOST': os.getenv('REDIS_HOST', config['redis']['host']),
        'REDIS_PORT': int(os.getenv('REDIS_PORT', config['redis']['port'])),
        'POSTGRESQL': {
            'host': os.getenv('POSTGRESQL_HOST', config['postgresql']['host']),
            'port': int(os.getenv('POSTGRESQL_PORT', config['postgresql']['port'])),
            'dbname': os.getenv('POSTGRESQL_DBNAME', config['postgresql']['dbname']),
            'user': os.getenv('POSTGRESQL_USER', config['postgresql']['user']),
            'password': os.getenv('POSTGRESQL_PASSWORD', config['postgresql']['password'])
        },
        'FIELDS': os.getenv('FIELDS', ','.join(config['fields'])).split(','),
        'PAUSE': int(os.getenv('PAUSE', config['pause'])),
        'LOG_LEVEL': os.getenv('LOG_LEVEL', config['log_level'])
    })

    logging.info("Configuration loaded successfully.")
    return config


def parse_arguments():
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(description="Script for collecting logs from Redis to PostgreSQL.")
    parser.add_argument('-r', '--redis', type=str, help="Redis server host.")
    parser.add_argument('-p', '--port', type=int, help="Redis server port.")
    parser.add_argument('-d', '--db', type=str, help="PostgreSQL database name.")
    parser.add_argument('-u', '--user', type=str, help="PostgreSQL user.")
    parser.add_argument('-w', '--password', type=str, help="PostgreSQL password.")
    parser.add_argument('-P', '--postgres_port', type=int, help="PostgreSQL port.")
    parser.add_argument('-H', '--postgres_host', type=str, help="PostgreSQL host.")
    parser.add_argument('-f', '--fields', type=str, help="Comma-separated list of fields.")
    parser.add_argument('-t', '--time', type=int, help="Pause between iterations (in seconds).")
    parser.add_argument('-l', '--log_level', type=str, help="Logging level.")
    return parser.parse_args()


def initialize_database(config):
    """Initializes the PostgreSQL database and creates a table for logs."""
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        dbname=config['dbname'],
        user=config['user'],
        password=config['password']
    )
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    logging.info("Database initialized and table created if not exists.")
    return conn


def create_dynamic_columns(conn, fields):
    cursor = conn.cursor()
    for field in fields:
        cursor.execute(f"""
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'logs' AND column_name = %s
        """, (field,))
        if not cursor.fetchone():
            try:
                cursor.execute(f"ALTER TABLE logs ADD COLUMN {field} TEXT")
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                logging.error(f"Error adding column {field}: {e}")
    logging.info("Dynamic columns created/verified.")


def clean_old_logs(conn):
    """Removes logs older than 24 hours."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM logs WHERE created_at < NOW() - INTERVAL '1 day'")
    conn.commit()
    logging.info("Old logs have been deleted.")


def check_redis_connection(redis_client):
    """Checks if the connection to Redis is working."""
    try:
        redis_client.ping()
        logging.info("Connection to Redis established.")
    except redis.exceptions.ConnectionError as e:
        logging.error("Connection to Redis failed. Error: %s", e)
        raise

def sanitize_json(log):
    """Fixes invalid JSON by replacing empty values (e.g., "key":,) with "key": null."""
    invalid_field_pattern = r'"\w+":\s*,'
    sanitized_log = re.sub(invalid_field_pattern, lambda match: match.group(0).replace(":", ": null"), log)
    return sanitized_log


def process_logs(conn, redis_client, fields, batch_size=100):
    """Processes logs from Redis and saves them to PostgreSQL."""
    cursor = conn.cursor()
    while True:
        logs = [redis_client.lpop("logs") for _ in range(batch_size)]
        logs = [log for log in logs if log]  # Remove None values
        if not logs:
            logging.info("No logs to process.")
            break

        for log in logs:
            try:
                log = sanitize_json(log)
                parsed_log = json.loads(log)
            except json.JSONDecodeError as e:
                logging.error(f"Error parsing log: {log}; {e}")
                continue

            values = [parsed_log.get(field, None) for field in fields]
            cursor.execute(f"""
            INSERT INTO logs ({', '.join(fields)})
            VALUES ({', '.join(['%s'] * len(fields))})
            """, values)
    conn.commit()
    logging.info("Logs saved.")


def main():
    """Main function of the script."""
    config = load_config()
    args = parse_arguments()

    config.update({
        'REDIS_HOST': args.redis or config['REDIS_HOST'],
        'REDIS_PORT': args.port or config['REDIS_PORT'],
        'POSTGRESQL': {
            'host': config['POSTGRESQL']['host'],
            'port': config['POSTGRESQL']['port'],
            'dbname': args.db or config['POSTGRESQL']['dbname'],
            'user': config['POSTGRESQL']['user'],
            'password': config['POSTGRESQL']['password']
        },
        'FIELDS': args.fields.split(',') if args.fields else config['FIELDS'],
        'PAUSE': args.time or config['PAUSE'],
        'LOG_LEVEL': args.log_level or config['LOG_LEVEL']
    })

    logging.basicConfig(level=getattr(logging, config['LOG_LEVEL'].upper(), None),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging configured successfully.")

    # log configuration all values
    logging.info(f"Configuration: {config}")

    redis_client = redis.Redis(host=config['REDIS_HOST'], port=config['REDIS_PORT'], decode_responses=True)
    logging.info("Connected to Redis.")

    try:
        conn = initialize_database(config['POSTGRESQL'])
    except psycopg2.Error as e:
        logging.error(f"Error initializing database: {e}")
        return

    try:
        check_redis_connection(redis_client)
    except redis.exceptions.ConnectionError:
        return
    create_dynamic_columns(conn, config['FIELDS'])

    try:
        while True:
            process_logs(conn, redis_client, config['FIELDS'])
            clean_old_logs(conn)
            time.sleep(config['PAUSE'])
    except KeyboardInterrupt:
        logging.info("Script terminated by user.")
    finally:
        logging.info("Script finished.")
        conn.close()


if __name__ == "__main__":
    main()