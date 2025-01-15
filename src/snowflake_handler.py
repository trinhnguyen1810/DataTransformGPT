import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List

load_dotenv()

class SnowflakeHandler:
    def __init__(self):
        self.conn = self._create_connection()

    def _create_connection(self):
        """Create Snowflake connection"""
        try:
            return snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account='enfmtbh-mgb45671',
                region='us-west-2',
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA')
            )
        except ProgrammingError as e:
            print(f"Failed to connect to Snowflake: {e}")
            raise

    def transform_text(self, text: str, command: str) -> str:
        """Transform text using Mistral via Snowflake Cortex"""
        try:
            cur = self.conn.cursor()
            
            # Clean and escape input text and command
            clean_text = text.replace("'", "''").replace('\n', ' ')
            clean_command = command.replace("'", "''").replace('\n', ' ')
            
            # Create prompt as a properly formatted single line
            prompt = f"Transform this text according to the command. Text: {clean_text} Command: {clean_command} Rules: Follow the command exactly, return only the transformed text."
            
            transform_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large2',
                '{prompt}'
            ) AS transformed_text
            """
            
            cur.execute(transform_query)
            result = cur.fetchone()
            return result[0] if result else text

        except Exception as e:
            print(f"Transformation error: {e}")
            return text

    def find_matching_rows(self, texts: List[str], search_description: str) -> List[bool]:
        """Use Mistral to find matching rows based on description"""
        try:
            cur = self.conn.cursor()
            matches = []
            
            # Clean search description
            clean_description = search_description.replace("'", "''").replace('\n', ' ')
            
            for text in texts:
                # Clean input text
                clean_text = text.replace("'", "''").replace('\n', ' ')
                prompt = f"Does this text match the criteria? Criteria: {clean_description} Text: {clean_text} Answer only true or false."
                
                query = f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large2',
                    '{prompt}'
                ) AS is_match
                """
                
                cur.execute(query)
                result = cur.fetchone()[0].lower().strip()
                matches.append(result == 'true')
            
            return matches

        except Exception as e:
            print(f"Search error: {e}")
            return [False] * len(texts)

    def generate_new_column(self, row_data: dict, column_desc: str) -> str:
        """Generate new column value based on existing data"""
        try:
            cur = self.conn.cursor()
            
            # Clean and prepare data for single-line format
            clean_data = {
                k: str(v).replace("'", "''").replace('\n', ' ').strip() 
                for k, v in row_data.items()
            }
            clean_desc = column_desc.replace("'", "''").replace('\n', ' ').strip()
            
            # Create context string
            context = ", ".join(f"{k}: {v}" for k, v in clean_data.items())
            
            # Create a single-line prompt without special characters
            prompt = f"Based on these details ({context}) {clean_desc}"
            
            # Simple SQL query without line breaks or special formatting
            query = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{prompt}') AS generated_value"""
            
            cur.execute(query)
            result = cur.fetchone()[0]
            return result.strip()

        except Exception as e:
            print(f"Generation error: {e}")
            return "Generation failed"

    def close(self):
        """Close Snowflake connection"""
        if hasattr(self, 'conn'):
            self.conn.close()