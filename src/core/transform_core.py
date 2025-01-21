import pandas as pd
from typing import Optional, Tuple, List, Dict
from .snowflake_handler import SnowflakeHandler
from config import BATCH_SIZE

class TransformCore:
    def __init__(self):
        self.snowflake = SnowflakeHandler()

    def process_dataframe(
        self,
        df: pd.DataFrame,
        column_commands: Dict[str, Dict],
        search_description: Optional[str] = None,
        progress_callback=None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        try:
            result_df = df.copy()
            total_operations = len(df) * len(column_commands)
            completed = 0
            
            if search_description:
                search_texts = []
                for idx in df.index:
                    row_text = " ".join(str(df.at[idx, col]) for col in column_commands.keys())
                    search_texts.append(row_text)
                
                matches = self.snowflake.find_matching_rows(search_texts, search_description)
                matching_rows = [idx for idx, match in enumerate(matches) if match]
                
                if not matching_rows:
                    return df, "No matching rows found"
            else:
                matching_rows = df.index

            for col, settings in column_commands.items():
                output_col = settings['new_name']
                if output_col not in result_df.columns:
                    result_df[output_col] = result_df[col]
                
                for i in range(0, len(matching_rows), BATCH_SIZE):
                    batch_rows = matching_rows[i:i+BATCH_SIZE]
                    
                    for idx in batch_rows:
                        result_df.at[idx, output_col] = self.snowflake.transform_text(
                            str(df.at[idx, col]),
                            settings['command']
                        )
                        completed += 1
                        if progress_callback:
                            progress_callback(completed / total_operations)

            return result_df, None

        except Exception as e:
            return df, f"Error during transformation: {str(e)}"

    def generate_column(
        self,
        df: pd.DataFrame,
        reference_columns: List[str],
        new_column_name: str,
        generation_prompt: str,
        progress_callback=None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        try:
            result_df = df.copy()
            result_df[new_column_name] = None
            total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
            
            # Process in batches
            for i in range(0, len(df), BATCH_SIZE):
                if progress_callback:
                    # Calculate progress as a value between 0 and 1
                    current_batch = i // BATCH_SIZE + 1
                    progress = min(current_batch / total_batches, 1.0)
                    progress_callback(progress)
                    
                batch_df = df.iloc[i:i+BATCH_SIZE]
                batch_data = [
                    {col: batch_df.iloc[j][col] for col in reference_columns}
                    for j in range(len(batch_df))
                ]
                
                batch_results = self.snowflake.batch_generate_column(
                    batch_data, 
                    generation_prompt
                )
                
                end_idx = min(i + BATCH_SIZE, len(df))
                result_df.iloc[i:end_idx, 
                            result_df.columns.get_loc(new_column_name)] = batch_results[:end_idx-i]
            
            if progress_callback:
                progress_callback(1.0)  
                
            return result_df, None
            
        except Exception as e:
            return df, f"Error generating column: {str(e)}"

    def close(self):
        self.snowflake.close()