import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import streamlit as st
from typing import Optional, Dict, Any, List

class SnowflakeHandler:
    def __init__(self):
        self.conn = self._create_connection()

    def _create_connection(self):
        """Create Snowflake connection"""
        try:
            return snowflake.connector.connect(
                user=st.secrets["SNOWFLAKE_USER"],
                password=st.secrets["SNOWFLAKE_PASSWORD"],
                account=st.secrets["SNOWFLAKE_ACCOUNT"],
                warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
                database=st.secrets["SNOWFLAKE_DATABASE"],
                schema=st.secrets["SNOWFLAKE_SCHEMA"]
            )
        except ProgrammingError as e:
            print(f"Failed to connect to Snowflake: {e}")
            raise

    def transform_text(self, text: str, command: str) -> str:
        """Transform text using Mistral via Snowflake Cortex"""
        try:
            cur = self.conn.cursor()
            clean_text = text.replace("'", "''").replace('\n', ' ')
            clean_command = command.replace("'", "''").replace('\n', ' ')
            prompt = f"Transform this text according to the command. Text: {clean_text} Command: {clean_command} Rules: Follow the command exactly, return only the transformed text. No introductory phrases or fillers allowed"

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
        
    def batch_generate_column(self, batch_data: List[Dict], column_desc: str) -> List[str]:
        try:
            cur = self.conn.cursor()
            results = []
            
            for row_data in batch_data:
                clean_data = {k: str(v).replace("'", "''").replace('\n', ' ') 
                             for k, v in row_data.items()}
                clean_desc = column_desc.replace("'", "''").replace('\n', ' ')
                
                context = " | ".join(f"{k}: {v}" for k, v in clean_data.items())
                
                prompt = f"Based on this data: {context} | Generate: {clean_desc} | Answer the following question directly, without introductory phrases or fillers. Return directly only the generated value."
                
                query = f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large2',
                    '{prompt}'
                ) AS generated_value
                """
                
                cur.execute(query)
                result = cur.fetchone()[0]
                results.append(result)
            
            return results

        except Exception as e:
            print(f"Batch generation error: {e}")
            return ["(generation failed)"] * len(batch_data)
        
    def find_matching_rows(self, texts: List[str], search_description: str) -> List[bool]:
        """Use Mistral to find matching rows based on description"""
        try:
            cur = self.conn.cursor()
            matches = []
            
            clean_description = search_description.replace("'", "''").replace('\n', ' ')
            
            for text in texts:
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
            
            clean_data = {k: str(v).replace("'", "''").replace('\n', ' ') 
                         for k, v in row_data.items()}
            clean_desc = column_desc.replace("'", "''").replace('\n', ' ')
            
            context = " | ".join(f"{k}: {v}" for k, v in clean_data.items())
            
            prompt = f"""
            Based on this data: {context}
            Generate: {clean_desc}
            Return only the generated value.
            """
            
            query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large2',
                '{prompt}'
            ) AS generated_value
            """
            
            cur.execute(query)
            result = cur.fetchone()[0]
            return result

        except Exception as e:
            print(f"Generation error: {e}")
            return "(generation failed)"

    def close(self):
        """Close Snowflake connection"""
        if hasattr(self, 'conn'):
            self.conn.close()