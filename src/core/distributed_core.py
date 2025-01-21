# src/core/distributed_core.py
import pandas as pd
from typing import Optional, Dict, List, Tuple
import uuid
from src.utils.redis_utils import RedisManager
from src.core.snowflake_handler import SnowflakeHandler
from src.core.transform_core import TransformCore
from config import CHUNK_SIZE, TIMEOUT
import time
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DistributedTransformCore:
    def __init__(self):
        self.distributed = False
        try:
            self.redis = RedisManager()
            self.distributed = True
            logger.info("Redis connected - running in distributed mode")
        except Exception as e:
            logger.info("Redis not available - falling back to non-distributed mode")
            self._fallback = TransformCore()
            
        self.snowflake = SnowflakeHandler()
        
    def _split_dataframe(self, df: pd.DataFrame) -> List[pd.DataFrame]:
        """Split dataframe into chunks with balanced distribution"""
        total_rows = len(df)
        
        if total_rows <= CHUNK_SIZE:
            logger.info(f"Small dataframe ({total_rows} rows), using single chunk")
            return [df]
        
        num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        if num_chunks > 1:
            last_chunk_size = total_rows % CHUNK_SIZE
            if last_chunk_size > 0 and last_chunk_size < CHUNK_SIZE * 0.5:
                num_chunks -= 1
        
        base_chunk_size = total_rows // num_chunks
        remainder = total_rows % num_chunks
        
        chunks = []
        start_idx = 0
        
        logger.info(f"Splitting {total_rows} rows into {num_chunks} chunks")
        
        for i in range(num_chunks):
            current_chunk_size = base_chunk_size + (1 if i < remainder else 0)
            end_idx = start_idx + current_chunk_size
            
            chunk = df[start_idx:end_idx]
            chunks.append(chunk)
            
            logger.info(f"Created chunk {i}: {len(chunk)} rows")
            start_idx = end_idx
        
        chunk_sizes = [len(chunk) for chunk in chunks]
        logger.info(f"Chunk sizes: min={min(chunk_sizes)}, max={max(chunk_sizes)}, avg={sum(chunk_sizes)/len(chunk_sizes):.1f}")
        
        return chunks

    def process_dataframe(
        self,
        df: pd.DataFrame,
        column_commands: Dict[str, Dict],
        search_description: Optional[str] = None,
        progress_callback=None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        # Fallback to non-distributed mode if Redis is not available
        if not self.distributed:
            logger.info("Using non-distributed processing")
            return self._fallback.process_dataframe(df, column_commands, search_description, progress_callback)
            
        try:
            job_id = str(uuid.uuid4())
            logger.info(f"Starting distributed job {job_id}")
            logger.info(f"Processing dataframe: {len(df)} rows, {len(df.columns)} columns")
            
            filtered_df = df.copy()
            if search_description:
                search_texts = []
                for idx in df.index:
                    row_text = " ".join(str(df.at[idx, col]) for col in column_commands.keys())
                    search_texts.append(row_text)
                
                matches = self.snowflake.find_matching_rows(search_texts, search_description)
                matching_rows = [idx for idx, match in enumerate(matches) if match]
                
                if not matching_rows:
                    return df, "No matching rows found"
                    
                filtered_df = df.iloc[matching_rows]
                logger.info(f"Found {len(filtered_df)} matching rows")
            
            # split into chunks and distribute work
            chunks = self._split_dataframe(filtered_df)
            total_chunks = len(chunks)
            
            if total_chunks == 0:
                return df, "No chunks created for processing"
                
            # initialize job in Redis
            self.redis.set_job_metadata(
                job_id,
                {
                    'total_chunks': total_chunks,
                    'completed_chunks': 0,
                    'status': 'processing',
                    'commands': json.dumps(column_commands),
                    'task_type': 'transform'
                }
            )
            
            # queue chunks for processing
            for chunk_id, chunk in enumerate(chunks):
                task = {
                    'job_id': job_id,
                    'chunk_id': chunk_id,
                    'data': chunk.to_json(),
                    'total_chunks': total_chunks,
                    'task_type': 'transform'
                }
                self.redis.add_task(task)
                logger.debug(f"Queued chunk {chunk_id} with {len(chunk)} rows")
            
            # Wait for results
            result_df = self._wait_for_results(job_id, df, total_chunks, progress_callback)
            return result_df, None
                
        except Exception as e:
            logger.error(f"Error in distributed processing: {str(e)}")
            if 'job_id' in locals():
                self.redis.cleanup_job(job_id)
            return df, f"Error during transformation: {str(e)}"

    def generate_column(
        self,
        df: pd.DataFrame,
        reference_columns: List[str],
        new_column_name: str,
        generation_prompt: str,
        progress_callback=None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        # fallback to non-distributed mode if Redis is not available
        if not self.distributed:
            logger.info("Using non-distributed column generation")
            return self._fallback.generate_column(
                df, reference_columns, new_column_name, generation_prompt, progress_callback
            )
            
        try:
            job_id = str(uuid.uuid4())
            logger.info(f"Starting distributed column generation job {job_id}")
            
            chunks = self._split_dataframe(df)
            total_chunks = len(chunks)
            
            self.redis.set_job_metadata(
                job_id,
                {
                    'total_chunks': total_chunks,
                    'completed_chunks': 0,
                    'status': 'processing',
                    'reference_columns': json.dumps(reference_columns),
                    'new_column_name': new_column_name,
                    'generation_prompt': generation_prompt,
                    'task_type': 'generate'
                }
            )
            
            for chunk_id, chunk in enumerate(chunks):
                task = {
                    'job_id': job_id,
                    'chunk_id': chunk_id,
                    'data': chunk.to_json(),
                    'total_chunks': total_chunks,
                    'task_type': 'generate'
                }
                self.redis.add_task(task)
            
            result_df = df.copy()
            result_df[new_column_name] = None
            
            result_df = self._wait_for_results(job_id, result_df, total_chunks, progress_callback)
            return result_df, None
            
        except Exception as e:
            logger.error(f"Error in distributed column generation: {str(e)}")
            if 'job_id' in locals():
                self.redis.cleanup_job(job_id)
            return df, f"Error generating column: {str(e)}"

    def _wait_for_results(self, job_id: str, base_df: pd.DataFrame, total_chunks: int, 
                         progress_callback=None) -> pd.DataFrame:
        """Wait for and combine results from workers"""
        start_time = time.time()
        last_completed = 0
        
        while time.time() - start_time < TIMEOUT:
            # check job status
            job_data = self.redis.get_job_metadata(job_id)
            completed = int(job_data.get('completed_chunks', 0))
            
            # update progress if changed
            if completed != last_completed:
                if progress_callback:
                    progress = completed / total_chunks
                    progress_callback(progress)
                    logger.info(f"Progress: {progress*100:.1f}%")
                last_completed = completed
            
            # check if all chunks are processed
            if completed >= total_chunks:
                if progress_callback:
                    progress_callback(1.0)  # ensure we show 100%
                break
            
            time.sleep(0.1)  # small sleep to prevent excessive Redis calls
        
        if last_completed < total_chunks:
            raise TimeoutError(f"Processing timeout: Only {last_completed}/{total_chunks} chunks completed")
        
        # collect and combine results
        processed_chunks = []
        for chunk_id in range(total_chunks):
            chunk_data = self.redis.get_result(job_id, chunk_id)
            if chunk_data:
                chunk_df = pd.read_json(chunk_data)
                processed_chunks.append((chunk_id, chunk_df))
            else:
                logger.warning(f"Missing result for chunk {chunk_id}")
        
        # combine results in order
        processed_chunks.sort(key=lambda x: x[0])
        if processed_chunks:
            result_df = pd.concat([chunk for _, chunk in processed_chunks], ignore_index=True)
        else:
            logger.warning("No processed chunks found, using base dataframe")
            result_df = base_df
        
        logger.info(f"Job {job_id} completed successfully")
        self.redis.cleanup_job(job_id)
        
        return result_df

    def close(self):
        """Cleanup and close connections"""
        if self.distributed:
            self.redis.close()
        else:
            self._fallback.close()
        self.snowflake.close()