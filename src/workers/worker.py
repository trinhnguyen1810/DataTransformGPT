# src/workers/worker.py
import pandas as pd
from src.utils.redis_utils import RedisManager
from src.core.snowflake_handler import SnowflakeHandler
import time
import json
import signal
from typing import List
import sys
from contextlib import contextmanager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True

@contextmanager
def get_snowflake_connection():
    """Context manager for Snowflake connection"""
    snowflake = None
    try:
        snowflake = SnowflakeHandler()
        yield snowflake
    finally:
        if snowflake:
            snowflake.close()

# In process_transform_chunk function in worker.py

def process_transform_chunk(chunk_df: pd.DataFrame, commands: dict, snowflake: SnowflakeHandler) -> pd.DataFrame:
    """Process a transformation chunk"""
    result = chunk_df.copy()
    start_time = time.time()
    operations_count = 0
    
    logger.info(f"Starting chunk processing: {len(chunk_df)} rows")
    
    for col, settings in commands.items():
        output_col = settings['new_name']
        if output_col not in result.columns:
            result[output_col] = result[col]
        
        logger.info(f"Processing column {col} -> {output_col}")
        col_start_time = time.time()
        
        for idx in result.index:
            result.at[idx, output_col] = snowflake.transform_text(
                str(result.at[idx, col]),
                settings['command']
            )
            operations_count += 1
            
        logger.info(f"Column {col} processed in {time.time() - col_start_time:.2f}s")
    
    total_time = time.time() - start_time
    avg_time = total_time / operations_count if operations_count > 0 else 0
    
    logger.info(f"Chunk processing complete:")
    logger.info(f"- Total operations: {operations_count}")
    logger.info(f"- Total time: {total_time:.2f}s")
    logger.info(f"- Average time per operation: {avg_time:.3f}s")
    
    return result

def process_generate_chunk(chunk_df: pd.DataFrame, reference_columns: List[str], 
                         new_column_name: str, prompt: str, 
                         snowflake: SnowflakeHandler) -> pd.DataFrame:
    """Process a generation chunk"""
    result = chunk_df.copy()
    result[new_column_name] = None
    
    batch_data = [
        {col: row[col] for col in reference_columns}
        for _, row in chunk_df.iterrows()
    ]
    
    generated_values = snowflake.batch_generate_column(batch_data, prompt)
    result[new_column_name] = generated_values
    
    return result

def worker_process():
    """Main worker process"""
    killer = GracefulKiller()
    redis_manager = RedisManager()
    
    logger.info("Worker started, waiting for tasks...")
    
    while not killer.kill_now:
        try:
            # Get task from queue
            task = redis_manager.get_task()
            if not task:
                continue
            
            job_id = task['job_id']
            chunk_id = task['chunk_id']
            task_type = task.get('task_type', 'transform')  # Default to transform for backward compatibility
            
            logger.info(f"Processing {task_type} chunk {chunk_id} of job {job_id}")
            
            with get_snowflake_connection() as snowflake:
                # Get job metadata
                job_data = redis_manager.get_job_metadata(job_id)
                if not job_data:
                    logger.warning(f"No metadata found for job {job_id}")
                    continue
                
                # Load chunk data
                chunk_df = pd.read_json(task['data'])
                
                if task_type == 'transform':
                    # Handle transformation task
                    commands = json.loads(job_data['commands'])
                    processed_df = process_transform_chunk(chunk_df, commands, snowflake)
                else:
                    # Handle generation task
                    reference_columns = json.loads(job_data['reference_columns'])
                    new_column_name = job_data['new_column_name']
                    generation_prompt = job_data['generation_prompt']
                    processed_df = process_generate_chunk(
                        chunk_df, 
                        reference_columns, 
                        new_column_name, 
                        generation_prompt, 
                        snowflake
                    )
                
                # Store result
                redis_manager.store_result(job_id, chunk_id, processed_df.to_json())
                
                # Update progress
                redis_manager.increment_completed(job_id)
                logger.info(f"Completed chunk {chunk_id} of job {job_id}")
                
        except Exception as e:
            logger.error(f"Error in worker process: {str(e)}")
            time.sleep(1)
    
    logger.info("Worker shutting down...")

if __name__ == "__main__":
    worker_process()