import json
import pandas as pd
import yaml
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
        self.custom_semantic_model = None
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
        active_model = self._get_active_semantic_model()
        
        if not active_model:
            # Fallback using direct database/schema info from client
            if self.client.database and self.client.schema:
                return f"""Convert this question to SQL using these database details:
Database: {self.client.database}
Schema: {self.client.schema}

CRITICAL: Always use the full qualified table names: {self.client.database}.{self.client.schema}.TABLE_NAME
Example format: SELECT * FROM {self.client.database}.{self.client.schema}.TABLE_NAME

Question: {question}

Return only the SQL query without any explanations or markdown formatting."""
            else:
                return f"Convert this question to SQL: {question}"
        
        # Handle custom semantic model format
        if self.custom_semantic_model:
            return self._create_custom_context_prompt(question, active_model)
        
        # Handle auto-discovered semantic model
        context = f"Database: {active_model['database']}\n"
        context += f"Schema: {active_model['schema']}\n\n"
        context += "Available Tables and Columns:\n"
        
        for table_name, table_info in active_model['tables'].items():
            context += f"\n{table_name}:\n"
            for column in table_info['columns']:
                context += f"  - {column['COLUMN_NAME']} ({column['DATA_TYPE']})\n"
        
        context += f"\nQuestion: {question}\n"
        context += f"Generate a SQL query to answer this question using the database '{active_model['database']}' and schema '{active_model['schema']}'. "
        context += "Always include the database and schema in table references (e.g., DATABASE.SCHEMA.TABLE_NAME). Return only the SQL query without any explanations."
        
        return context
    
    def _create_custom_context_prompt(self, question: str, semantic_model: Dict[str, Any]) -> str:
        """
        Create context prompt using custom semantic model
        
        Args:
            question: User's natural language question
            semantic_model: Custom semantic model data
            
        Returns:
            str: Formatted context prompt
        """
        context = ""
        
        # Handle semantic model structure
        if 'semantic_model' in semantic_model:
            model_info = semantic_model['semantic_model']
            
            if 'name' in model_info:
                context += f"Semantic Model: {model_info['name']}\n"
            if 'description' in model_info:
                context += f"Description: {model_info['description']}\n"
            
            context += "\nAvailable Tables and Columns:\n"
            
            # Process tables
            if 'tables' in model_info:
                for table in model_info['tables']:
                    table_name = table.get('name', 'UNKNOWN')
                    context += f"\n{table_name}:"
                    
                    if 'description' in table:
                        context += f" - {table['description']}"
                    context += "\n"
                    
                    # Process columns
                    if 'columns' in table:
                        for column in table['columns']:
                            col_name = column.get('name', 'UNKNOWN')
                            col_type = column.get('type', 'UNKNOWN')
                            col_desc = column.get('description', '')
                            
                            context += f"  - {col_name} ({col_type})"
                            if col_desc:
                                context += f" - {col_desc}"
                            context += "\n"
            
            # Add relationships if available
            if 'relationships' in model_info:
                context += "\nTable Relationships:\n"
                for rel in model_info['relationships']:
                    from_table = rel.get('from_table', '')
                    from_col = rel.get('from_column', '')
                    to_table = rel.get('to_table', '')
                    to_col = rel.get('to_column', '')
                    rel_type = rel.get('type', 'related to')
                    
                    context += f"  - {from_table}.{from_col} {rel_type} {to_table}.{to_col}\n"
        
        context += f"\nQuestion: {question}\n"
        context += "Generate a SQL query to answer this question using the tables and columns described above.\n"
        
        # Get database and schema names from the model_info (within custom semantic model context)
        if 'semantic_model' in semantic_model:
            model_data = semantic_model['semantic_model']
            db_name = model_data.get('database', self.client.database or 'DATABASE')
            schema_name = model_data.get('schema', self.client.schema or 'SCHEMA')
        else:
            # Fallback to client information
            db_name = self.client.database or 'DATABASE'
            schema_name = self.client.schema or 'SCHEMA'
            
        context += f"CRITICAL: Always use the full qualified table names: {db_name}.{schema_name}.TABLE_NAME\n"
        context += f"Database: {db_name}\n"
        context += f"Schema: {schema_name}\n"
        context += f"Example format: SELECT * FROM {db_name}.{schema_name}.TABLE_NAME\n"
        context += "Return only the SQL query without any explanations or markdown formatting."
        
        return context
    
    def _call_cortex_analyst(self, prompt: str, model: str = 'llama3.1-8b') -> Optional[str]:
        """
        Call Snowflake Cortex Analyst to generate SQL
        
        Args:
            prompt: Context prompt for SQL generation
            model: LLM model to use for generation
            
        Returns:
            str: Generated SQL query or None if error
        """
        try:
            # Use Snowflake's Cortex Complete function for SQL generation
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                '{prompt.replace("'", "''")}'
            ) as generated_sql
            """
            
            result = self.client.execute_query(cortex_query)
            
            if result is not None and not result.empty:
                generated_text = result.iloc[0]['GENERATED_SQL']
                print(f"Raw Cortex response: {generated_text}")  # Debug output
                
                # Extract SQL from the response
                sql_query = self._extract_sql_from_response(generated_text)
                print(f"Extracted SQL: {sql_query}")  # Debug output
                return sql_query
            else:
                print("No result from Cortex query execution")
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
    
    def process_question(self, question: str, model: str = 'llama3.1-8b') -> Dict[str, Any]:
        """
        Process natural language question and return SQL results
        
        Args:
            question: User's natural language question
            model: LLM model to use for generation
            
        Returns:
            dict: Result containing success status, data, SQL query, and any errors
        """
        sql_query = None
        
        try:
            # Debug: Print database/schema information
            print(f"DEBUG: Client database: {self.client.database}")
            print(f"DEBUG: Client schema: {self.client.schema}")
            print(f"DEBUG: Active semantic model available: {self._get_active_semantic_model() is not None}")
            
            # Create context prompt
            prompt = self._create_context_prompt(question)
            print(f"DEBUG: Generated prompt preview: {prompt[:200]}...")
            
            # Generate SQL using Cortex Analyst with specified model
            sql_query = self._call_cortex_analyst(prompt, model)
            
            if not sql_query:
                return {
                    'success': False,
                    'error': 'Failed to generate SQL query from your question. This could be due to unclear question or database connection issues.',
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
                'sql_query': sql_query  # Include any partially generated SQL
            }
    
    def get_table_summary(self) -> Optional[Dict[str, Any]]:
        """
        Get summary of available tables and their schemas
        
        Returns:
            dict: Summary of database structure
        """
        return self.semantic_model
    
    def load_custom_semantic_model(self, yaml_data: Dict[str, Any]):
        """
        Load custom semantic model from YAML data
        
        Args:
            yaml_data: Parsed YAML data containing semantic model definition
        """
        try:
            self.custom_semantic_model = yaml_data
            print("Custom semantic model loaded successfully")
        except Exception as e:
            print(f"Error loading custom semantic model: {str(e)}")
    
    def _get_active_semantic_model(self) -> Optional[Dict[str, Any]]:
        """
        Get the active semantic model (custom if available, otherwise auto-discovered)
        
        Returns:
            dict: Active semantic model
        """
        if self.custom_semantic_model:
            return self.custom_semantic_model
        return self.semantic_model
