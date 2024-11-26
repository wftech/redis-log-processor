import redis
import json
import random
from faker import Faker
import time

fake = Faker()

def generate_fake_log():
    """Generates a fake log with predefined fields."""
    log = {
        "http_time": fake.date_time_this_year().isoformat(),
        "http_vhost": fake.domain_name(),
        "http_remote_addr": fake.ipv4(),
        "http_method": random.choice(["GET", "POST", "PUT", "DELETE"]),
        "http_status": random.randint(100, 599),
        "http_request_time": random.uniform(0.1, 5.0),
        "http_uri": fake.uri_path()
    }
    return log

def sanitize_json(log):
    """Ensure the JSON is well-formed, fixing invalid fields."""
    log_str = json.dumps(log)  # Convert to valid JSON string
    return log_str

def insert_logs_to_redis(redis_client, num_logs=1000):
    """Inserts generated logs into Redis."""
    for _ in range(num_logs):
        log = generate_fake_log()
        log_str = sanitize_json(log)
        redis_client.rpush("logs", log_str)
        print(f"Inserted log: {log_str}")
        time.sleep(0.1)  # Add a small delay to avoid overwhelming Redis

def connect_to_redis():
    """Connects to Redis and returns the client."""
    client = redis.Redis(host='redis', port=6379, decode_responses=True)
    return client

if __name__ == "__main__":
    # Connect to Redis
    redis_client = connect_to_redis()

    # Insert 1000 fake logs into Redis
    insert_logs_to_redis(redis_client, num_logs=1000)
