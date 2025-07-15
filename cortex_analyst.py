import json
import pandas as pd
from typing import Dict, Any, Optional
from snowflake_client import SnowflakeClient

class CortexAnalyst:
    """Snowflake Cortex Analyst integration for natural language to SQL conversion"""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        """
        Initialize Cortex Analyst with Snowflake client
        
        Args:
            snowflake_client: Configured SnowflakeClient instance
        """
        self.client = snowflake_client
        self.semantic_model = None
        self._initialize_semantic_model()
    
    def _initialize_semantic_model(self):
        """Initialize semantic model by analyzing available tables and schemas"""
        try:
            # Get available tables
            tables = self.client.get_tables()
            if tables is not None and not tables.empty:
                self.semantic_model = {
                    'database': self.client.database,
                    'schema': self.client.schema,
                    'tables': {}
                }
                
                # For each table, get its schema
                for _, table_row in tables.iterrows():
                    table_name = table_row['TABLE_NAME']
                    schema_info = self.client.get_table_schema(table_name)
                    
                    if schema_info is not None:
                        self.semantic_model['tables'][table_name] = {
                            'columns': schema_info.to_dict('records'),
                            'row_count': table_row.get('ROW_COUNT', 0),
                            'table_type': table_row.get('TABLE_TYPE', 'TABLE')
                        }
        except Exception as e:
            print(f"Error initializing semantic model: {str(e)}")
    
    def _create_context_prompt(self, question: str) -> str:
        """
        Create context prompt for Cortex Analyst
        
        Args:
            question: User's natural language question
            
        Returns:
            str: Formatted context prompt
        """
        if not self.semantic_model:
            return f"Convert this question to SQL: {question}"
        
        # Build context with available tables and columns
        context = f"Database: {self.semantic_model['database']}\n"
        context += f"Schema: {self.semantic_model['schema']}\n\n"
        context += "Available Tables and Columns:\n"
        
        for table_name, table_info in self.semantic_model['tables'].items():
            context += f"\n{table_name}:\n"
            for column in table_info['columns']:
                context += f"  - {column['COLUMN_NAME']} ({column['DATA_TYPE']})\n"
        
        context += f"\nQuestion: {question}\n"
        context += "Generate a SQL query to answer this question. Return only the SQL query without any explanations."
        
        return context
    
    def _call_cortex_analyst(self, prompt: str) -> Optional[str]:
        """
        Call Snowflake Cortex Analyst to generate SQL
        
        Args:
            prompt: Context prompt for SQL generation
            
        Returns:
            str: Generated SQL query or None if error
        """
        try:
            # Use Snowflake's Cortex Complete function for SQL generation
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-8b',
                '{prompt.replace("'", "''")}'
            ) as generated_sql
            """
            
            result = self.client.execute_query(cortex_query)
            
            if result is not None and not result.empty:
                generated_text = result.iloc[0]['GENERATED_SQL']
                
                # Extract SQL from the response
                sql_query = self._extract_sql_from_response(generated_text)
                return sql_query
            
            return None
            
        except Exception as e:
            print(f"Error calling Cortex Analyst: {str(e)}")
            return None
    
    def _extract_sql_from_response(self, response: str) -> str:
        """
        Extract SQL query from Cortex response
        
        Args:
            response: Raw response from Cortex
            
        Returns:
            str: Cleaned SQL query
        """
        # Remove common prefixes and suffixes
        response = response.strip()
        
        # Look for SQL keywords to identify the query
        sql_keywords = ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE']
        
        lines = response.split('\n')
        sql_lines = []
        capturing = False
        
        for line in lines:
            line = line.strip()
            if any(line.upper().startswith(keyword) for keyword in sql_keywords):
                capturing = True
            
            if capturing:
                # Stop if we hit explanatory text
                if line.startswith('--') or line.startswith('#') or line.startswith('/*'):
                    continue
                sql_lines.append(line)
                
                # Stop if we hit a semicolon at the end of a line
                if line.endswith(';'):
                    break
        
        sql_query = '\n'.join(sql_lines).strip()
        
        # Remove any trailing semicolons for Snowflake
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]
        
        return sql_query
    
    def _validate_and_execute_sql(self, sql_query: str) -> Dict[str, Any]:
        """
        Validate and execute the generated SQL query
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            dict: Result with success status, data, and any errors
        """
        try:
            # Basic SQL injection prevention
            forbidden_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
            sql_upper = sql_query.upper()
            
            for keyword in forbidden_keywords:
                if keyword in sql_upper:
                    return {
                        'success': False,
                        'error': f'Query contains forbidden keyword: {keyword}',
                        'data': None,
                        'sql_query': sql_query
                    }
            
            # Execute the query
            result_df = self.client.execute_query(sql_query)
            
            if result_df is not None:
                return {
                    'success': True,
                    'data': result_df,
                    'sql_query': sql_query,
                    'error': None
                }
            else:
                return {
                    'success': False,
                    'error': 'Query execution failed',
                    'data': None,
                    'sql_query': sql_query
                }
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Query execution error: {str(e)}',
                'data': None,
                'sql_query': sql_query
            }
    
    def process_question(self, question: str) -> Dict[str, Any]:
        """
        Process natural language question and return SQL results
        
        Args:
            question: User's natural language question
            
        Returns:
            dict: Result containing success status, data, SQL query, and any errors
        """
        try:
            # Create context prompt
            prompt = self._create_context_prompt(question)
            
            # Generate SQL using Cortex Analyst
            sql_query = self._call_cortex_analyst(prompt)
            
            if not sql_query:
                return {
                    'success': False,
                    'error': 'Failed to generate SQL query from your question',
                    'data': None,
                    'sql_query': None
                }
            
            # Validate and execute the SQL
            result = self._validate_and_execute_sql(sql_query)
            return result
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Error processing question: {str(e)}',
                'data': None,
                'sql_query': None
            }
    
    def get_table_summary(self) -> Optional[Dict[str, Any]]:
        """
        Get summary of available tables and their schemas
        
        Returns:
            dict: Summary of database structure
        """
        return self.semantic_model
