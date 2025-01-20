# src/core/distributed_core.py
import pandas as pd
from typing import Optional, Dict, List, Tuple
import uuid
from src.utils.redis_utils import RedisManager
from src.core.snowflake_handler import SnowflakeHandler
from config import CHUNK_SIZE, TIMEOUT
import time
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DistributedTransformCore:
    def __init__(self):
        self.redis = RedisManager()
        self.snowflake = SnowflakeHandler()  # We still need Snowflake for task prep
        
    def _split_dataframe(self, df: pd.DataFrame) -> List[pd.DataFrame]:
        """Split dataframe into chunks with balanced distribution"""
        total_rows = len(df)
        
        # If total rows is less than chunk size, just return one chunk
        if total_rows <= CHUNK_SIZE:
            logger.info(f"Small dataframe ({total_rows} rows), using single chunk")
            return [df]
        
        # Calculate number of chunks, ensuring last chunk isn't too small
        num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        if num_chunks > 1:
            last_chunk_size = total_rows % CHUNK_SIZE
            if last_chunk_size > 0 and last_chunk_size < CHUNK_SIZE * 0.5:
                # If last chunk would be less than half size, reduce number of chunks
                num_chunks -= 1
        
        # Calculate size for each chunk
        base_chunk_size = total_rows // num_chunks
        remainder = total_rows % num_chunks
        
        chunks = []
        start_idx = 0
        
        logger.info(f"Splitting {total_rows} rows into {num_chunks} chunks")
        
        for i in range(num_chunks):
            # Add one extra row to some chunks to distribute remainder
            current_chunk_size = base_chunk_size + (1 if i < remainder else 0)
            end_idx = start_idx + current_chunk_size
            
            chunk = df[start_idx:end_idx]
            chunks.append(chunk)
            
            logger.info(f"Created chunk {i}: {len(chunk)} rows")
            start_idx = end_idx
        
        # Log chunk distribution
        chunk_sizes = [len(chunk) for chunk in chunks]
        logger.info(f"Chunk distribution: {chunk_sizes}")
        logger.info(f"Min size: {min(chunk_sizes)}, Max size: {max(chunk_sizes)}")
        
        return chunks

    def process_dataframe(
        self,
        df: pd.DataFrame,
        column_commands: Dict[str, Dict],
        search_description: Optional[str] = None,
        progress_callback=None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        try:
            # Generate unique job ID and log start
            job_id = str(uuid.uuid4())
            logger.info(f"Starting job {job_id}")
            logger.info(f"Processing dataframe: {len(df)} rows, {len(df.columns)} columns")
            
            # Filter rows if search description provided
            matching_rows = self._prepare_search_filter(df, search_description, column_commands)
            if not matching_rows:
                return df, "No matching rows found"
                
            filtered_df = df.iloc[matching_rows]
            logger.info(f"Processing {len(filtered_df)} rows after filtering")
            
            # Split into chunks
            chunks = self._split_dataframe(filtered_df)
            total_chunks = len(chunks)
            
            if total_chunks == 0:
                return df, "No chunks created for processing"
                
            # Initialize job in Redis
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
            
            # Queue chunks for processing
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
            
            # Wait for and collect results
            try:
                result_df = self._wait_for_results(job_id, df, total_chunks, progress_callback)
                return result_df, None
            except TimeoutError as e:
                logger.error(f"Timeout while waiting for results: {str(e)}")
                self.redis.cleanup_job(job_id)
                return df, f"Processing timeout: {str(e)}"
                
        except Exception as e:
            logger.error(f"Error in process_dataframe: {str(e)}")
            if 'job_id' in locals():
                self.redis.cleanup_job(job_id)
            return df, f"Error during transformation: {str(e)}"
    
    def _prepare_search_filter(self, df: pd.DataFrame, search_description: str, 
                         column_commands: Dict) -> List[int]:
        """Prepare search filter - done in main process to avoid redundant worker operations"""
        if not search_description:
            return list(df.index)
            
        search_texts = []
        for idx in df.index:
            row_text = " ".join(str(df.at[idx, col]) for col in column_commands.keys())
            search_texts.append(row_text)
        
        matches = self.snowflake.find_matching_rows(search_texts, search_description)
        return [idx for idx, match in enumerate(matches) if match]
        
    def _wait_for_results(self, job_id: str, base_df: pd.DataFrame, total_chunks: int, 
                     progress_callback=None) -> pd.DataFrame:
        """Wait for and combine results from workers"""
        start_time = time.time()
        last_progress = -1  # Initialize to -1 to ensure first update
        
        while time.time() - start_time < TIMEOUT:
            # Get detailed progress
            current_progress = self.redis.get_total_progress(job_id, total_chunks)
            
            # Update if progress has changed
            if progress_callback and abs(current_progress - last_progress) >= 0.01:  # Update for each 1% change
                progress_callback(current_progress)
                logger.info(f"Progress: {current_progress*100:.1f}%")
                last_progress = current_progress
            
            if current_progress >= 0.999:  # Almost done
                break
            
            time.sleep(0.1)

    def generate_column(
        self,
        df: pd.DataFrame,
        reference_columns: List[str],
        new_column_name: str,
        generation_prompt: str,
        progress_callback=None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        try:
            # Generate unique job ID
            job_id = str(uuid.uuid4())
            logger.info(f"Starting column generation job {job_id}")
            
            # Split dataframe into chunks
            chunks = self._split_dataframe(df)
            total_chunks = len(chunks)
            
            # Initialize job metadata in Redis
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
            
            # Add chunks to processing queue
            for chunk_id, chunk in enumerate(chunks):
                task = {
                    'job_id': job_id,
                    'chunk_id': chunk_id,
                    'data': chunk.to_json(),
                    'total_chunks': total_chunks,
                    'task_type': 'generate'
                }
                self.redis.add_task(task)
            
            logger.info(f"Added {total_chunks} generation chunks to queue")
            
            # Create result dataframe with new column
            result_df = df.copy()
            result_df[new_column_name] = None
            
            # Wait for results with progress updates
            result_df = self._wait_for_results(job_id, result_df, total_chunks, progress_callback)
            return result_df, None
            
        except Exception as e:
            logger.error(f"Error in generate_column: {str(e)}")
            try:
                self.redis.cleanup_job(job_id)
            except:
                pass
            return df, f"Error generating column: {str(e)}"

    def _wait_for_results(self, job_id: str, base_df: pd.DataFrame, total_chunks: int, 
                         progress_callback=None) -> pd.DataFrame:
        """Wait for and combine results from workers"""
        start_time = time.time()
        last_progress_update = 0
        
        while time.time() - start_time < TIMEOUT:
            # Check job status
            job_data = self.redis.get_job_metadata(job_id)
            completed = int(job_data.get('completed_chunks', 0))
            
            # Update progress at most once per second
            current_time = time.time()
            if current_time - last_progress_update >= 1:
                if progress_callback:
                    progress_callback(completed / total_chunks)
                last_progress_update = current_time
            
            # Check if all chunks are processed
            if completed == total_chunks:
                break
            
            time.sleep(0.1)
        
        if completed != total_chunks:
            raise TimeoutError("Processing timeout")
        
        # Collect all results
        processed_chunks = []
        for chunk_id in range(total_chunks):
            chunk_data = self.redis.get_result(job_id, chunk_id)
            if chunk_data:
                chunk_df = pd.read_json(chunk_data)
                processed_chunks.append((chunk_id, chunk_df))
        
        # Combine results in order
        processed_chunks.sort(key=lambda x: x[0])
        if processed_chunks:
            result_df = pd.concat([chunk for _, chunk in processed_chunks], ignore_index=True)
        else:
            result_df = base_df
        
        logger.info(f"Job {job_id} completed successfully")
        
        # Cleanup Redis
        self.redis.cleanup_job(job_id)
        
        return result_df

    def close(self):
        """Cleanup and close connections"""
        self.redis.close()
        self.snowflake.close()