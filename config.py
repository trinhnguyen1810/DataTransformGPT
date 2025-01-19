from dotenv import load_dotenv
import os

load_dotenv()

# Size limits
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
BATCH_SIZE = 50
MAX_PARALLEL_QUERIES = 4

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

# File settings
SUPPORTED_FORMATS = ['csv', 'xlsx']