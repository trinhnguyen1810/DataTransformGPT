# src/app/config.py
from dotenv import load_dotenv
import os

load_dotenv()

# File and Size settings
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
SUPPORTED_FORMATS = ['csv', 'xlsx']

# Processing settings
CHUNK_SIZE = 50
BATCH_SIZE = 50  # For non-distributed operations
MAX_PARALLEL_QUERIES = 4
TIMEOUT = 3600  # 1 hour
MAX_RETRIES = 3

# Redis settings
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'region': 'us-west-2'  # Added from your snowflake_handler.py
}