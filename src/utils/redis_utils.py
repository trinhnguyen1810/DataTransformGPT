# src/utils/redis_utils.py
import redis
from config import REDIS_HOST, REDIS_PORT, REDIS_DB
import json
from typing import Optional, Any, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisManager:
    _instance = None

    def __init__(self):
        """Initialize Redis connection"""
        self.client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        self._test_connection()
        logger.info("Redis connection established")

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisManager, cls).__new__(cls)
        return cls._instance

    def _test_connection(self):
        """Test Redis connection"""
        try:
            self.client.ping()
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error: {str(e)}")
            raise Exception(f"Could not connect to Redis: {str(e)}")

    def set_job_metadata(self, job_id: str, metadata: dict):
        try:
            self.client.hset(f'job:{job_id}', mapping=metadata)
            logger.info(f"Set metadata for job {job_id}: {metadata}")
        except Exception as e:
            logger.error(f"Error setting job metadata: {str(e)}")
            raise

    def get_job_metadata(self, job_id: str) -> dict:
        try:
            metadata = self.client.hgetall(f'job:{job_id}')
            logger.debug(f"Got metadata for job {job_id}: {metadata}")
            return metadata
        except Exception as e:
            logger.error(f"Error getting job metadata: {str(e)}")
            return {}

    def add_task(self, task_data: dict):
        try:
            self.client.lpush('task_queue', json.dumps(task_data))
            logger.debug(f"Added task to queue: {task_data}")
        except Exception as e:
            logger.error(f"Error adding task to queue: {str(e)}")
            raise

    def get_task(self, timeout: int = 1) -> Optional[dict]:
        try:
            task = self.client.brpop('task_queue', timeout=timeout)
            if task:
                task_data = json.loads(task[1])
                logger.debug(f"Got task from queue: {task_data}")
                return task_data
            return None
        except Exception as e:
            logger.error(f"Error getting task from queue: {str(e)}")
            return None

    def store_result(self, job_id: str, chunk_id: int, result_data: Any):
        try:
            key = f'result:{job_id}:{chunk_id}'
            data = json.dumps(result_data) if isinstance(result_data, dict) else result_data
            self.client.set(key, data)
            logger.debug(f"Stored result for chunk {chunk_id} of job {job_id}")
        except Exception as e:
            logger.error(f"Error storing result: {str(e)}")
            raise

    def get_result(self, job_id: str, chunk_id: int) -> Optional[str]:
        try:
            result = self.client.get(f'result:{job_id}:{chunk_id}')
            logger.debug(f"Got result for chunk {chunk_id} of job {job_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting result: {str(e)}")
            return None

    def increment_completed(self, job_id: str) -> int:
        try:
            count = self.client.hincrby(f'job:{job_id}', 'completed_chunks', 1)
            logger.info(f"Job {job_id} progress: {count} chunks completed")
            return count
        except Exception as e:
            logger.error(f"Error incrementing completed count: {str(e)}")
            return 0

    def cleanup_job(self, job_id: str):
        try:
            self.client.delete(f'job:{job_id}')
            
            keys = self.client.keys(f'result:{job_id}:*')
            if keys:
                self.client.delete(*keys)
            logger.info(f"Cleaned up job {job_id}")
        except Exception as e:
            logger.error(f"Error cleaning up job: {str(e)}")

    def monitor_progress(self, job_id: str) -> Tuple[int, int]:
        """Monitor job progress"""
        try:
            metadata = self.get_job_metadata(job_id)
            completed = int(metadata.get('completed_chunks', 0))
            total = int(metadata.get('total_chunks', 0))
            logger.info(f"Progress for job {job_id}: {completed}/{total} chunks")
            return completed, total
        except Exception as e:
            logger.error(f"Error monitoring progress: {str(e)}")
            return 0, 0
    

    def close(self):
        try:
            self.client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {str(e)}")