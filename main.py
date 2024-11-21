import argparse
import json
import logging
import os
import re
import sqlite3
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
        'SQLITE_DB_PATH': os.getenv('SQLITE_DB_PATH', config['sqlite']['db_path']),
        'FIELDS': os.getenv('FIELDS', ','.join(config['fields'])).split(','),
        'PAUSE': int(os.getenv('PAUSE', config['pause'])),
        'LOG_LEVEL': os.getenv('LOG_LEVEL', config['log_level'])
    })

    logging.info("Configuration loaded successfully.")
    return config


def parse_arguments():
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(description="Script for collecting logs from Redis to SQLite.")
    parser.add_argument('-r', '--redis', type=str, help="Redis server host.")
    parser.add_argument('-p', '--port', type=int, help="Redis server port.")
    parser.add_argument('-d', '--db', type=str, help="Path to SQLite database.")
    parser.add_argument('-f', '--fields', type=str, help="Comma-separated list of fields.")
    parser.add_argument('-t', '--time', type=int, help="Pause between iterations (in seconds).")
    parser.add_argument('-l', '--log_level', type=str, help="Logging level.")
    return parser.parse_args()


def initialize_database(db_path):
    """Initializes the SQLite database and creates a table for logs."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        logging.info("Database initialized and table created if not exists.")
    return db_path


def create_dynamic_columns(db_path, fields):
    """Creates dynamic columns for the SQLite table based on fields from the configuration."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        for field in fields:
            try:
                cursor.execute(f"ALTER TABLE logs ADD COLUMN {field} TEXT")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logging.warning(f"Column {field} already exists.")
                else:
                    logging.error(f"Unexpected error: {e}")
        conn.commit()
        logging.info("Dynamic columns created/verified.")


def clean_old_logs(db_path):
    """Removes logs older than 24 hours."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM logs WHERE created_at < datetime('now', '-1 day')")
        conn.commit()
        logging.info("Old logs have been deleted.")


def sanitize_json(log):
    """Fixes invalid JSON by replacing empty values (e.g., "key":,) with "key": null."""
    invalid_field_pattern = r'"\w+":\s*,'
    sanitized_log = re.sub(invalid_field_pattern, lambda match: match.group(0).replace(":", ": null"), log)
    return sanitized_log


def process_logs(db_path, redis_client, fields, batch_size=100):
    """Processes logs from Redis and saves them to SQLite."""
    with sqlite3.connect(db_path) as conn:
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
                except json.JSONDecodeError:
                    logging.error(f"Error parsing log: {log}")
                    continue

                values = [parsed_log.get(field, None) for field in fields]
                cursor.execute(f"""
                INSERT INTO logs ({', '.join(fields)})
                VALUES ({', '.join(['?'] * len(fields))})
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
        'SQLITE_DB_PATH': args.db or config['SQLITE_DB_PATH'],
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

    db_path = initialize_database(config['SQLITE_DB_PATH'])
    create_dynamic_columns(db_path, config['FIELDS'])

    try:
        while True:
            process_logs(db_path, redis_client, config['FIELDS'])
            clean_old_logs(db_path)
            time.sleep(config['PAUSE'])
    except KeyboardInterrupt:
        logging.info("Script terminated by user.")
    finally:
        logging.info("Script finished.")


if __name__ == "__main__":
    main()