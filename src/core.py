import pandas as pd
from typing import Optional, Tuple, List, Dict
from .snowflake_handler import SnowflakeHandler

class TransformCore:
    def __init__(self):
        self.snowflake = SnowflakeHandler()

    def process_dataframe(
        self,
        df: pd.DataFrame,
        column_commands: Dict[str, Dict],
        search_description: Optional[str] = None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """Process dataframe with column-specific transformations"""
        try:
            result_df = df.copy()
            
            # Get rows to process
            if search_description:
                # Combine all relevant columns for search
                search_texts = []
                for idx in df.index:
                    row_text = " ".join(str(df.at[idx, col]) for col in column_commands.keys())
                    search_texts.append(row_text)
                
                # Find matching rows
                matches = self.snowflake.find_matching_rows(search_texts, search_description)
                matching_rows = [idx for idx, match in enumerate(matches) if match]
                
                if not matching_rows:
                    return df, "No matching rows found"
            else:
                matching_rows = df.index

            # Process each column with its specific command
            for col, settings in column_commands.items():
                command = settings['command']
                output_col = settings['new_name']
                
                # Initialize new column if needed
                if output_col not in result_df.columns:
                    result_df[output_col] = result_df[col]
                
                # Transform matching rows
                for idx in matching_rows:
                    result_df.at[idx, output_col] = self.snowflake.transform_text(
                        str(df.at[idx, col]),
                        command
                    )

            return result_df, None

        except Exception as e:
            return df, f"Error during transformation: {str(e)}"

    def generate_column(
        self,
        df: pd.DataFrame,
        reference_columns: List[str],
        new_column_name: str,
        generation_prompt: str
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """Generate a new column based on existing data"""
        try:
            result_df = df.copy()
            result_df[new_column_name] = None
            
            for idx in df.index:
                # Get reference data for this row
                row_data = {
                    col: df.at[idx, col] 
                    for col in reference_columns
                }
                
                # Generate new value
                generated_value = self.snowflake.generate_new_column(
                    row_data,
                    generation_prompt
                )
                
                result_df.at[idx, new_column_name] = generated_value
            
            return result_df, None
            
        except Exception as e:
            return df, f"Error generating column: {str(e)}"

    def close(self):
        """Clean up resources"""
        self.snowflake.close()