import snowflake.connector
import pandas as pd
import os
from typing import Optional, Dict, Any

class SnowflakeClient:
    """Snowflake database client for handling connections and queries"""
    
    def __init__(self, account: str, user: str, password: str, 
                 warehouse: str, database: str, schema: str):
        """
        Initialize Snowflake client with connection parameters
        
        Args:
            account: Snowflake account identifier
            user: Username for authentication
            password: Password for authentication
            warehouse: Snowflake warehouse to use
            database: Database name
            schema: Schema name
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
    
    def connect(self) -> bool:
        """
        Establish connection to Snowflake
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                client_session_keep_alive=True
            )
            return True
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test the Snowflake connection
        
        Returns:
            bool: True if connection test successful, False otherwise
        """
        try:
            if self.connect():
                cursor = self.connection.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                result = cursor.fetchone()
                cursor.close()
                return result is not None
            return False
        except Exception as e:
            print(f"Connection test failed: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute SQL query and return results as pandas DataFrame
        
        Args:
            query: SQL query string
            
        Returns:
            pandas.DataFrame or None: Query results or None if error
        """
        try:
            if not self.connection:
                if not self.connect():
                    raise Exception("Failed to establish connection")
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            cursor.close()
            
            # Convert to DataFrame
            if results:
                df = pd.DataFrame(results, columns=columns)
                return df
            else:
                return pd.DataFrame()
        
        except Exception as e:
            print(f"Query execution error: {str(e)}")
            return None
    
    def get_tables(self) -> Optional[pd.DataFrame]:
        """
        Get list of available tables in the current database and schema
        
        Returns:
            pandas.DataFrame: List of tables with metadata
        """
        query = f"""
        SELECT 
            TABLE_NAME,
            TABLE_TYPE,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{self.schema}'
        ORDER BY TABLE_NAME
        """
        return self.execute_query(query)
    
    def get_table_schema(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Get schema information for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            pandas.DataFrame: Table schema information
        """
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{self.schema}' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query)
    
    def close_connection(self):
        """Close the Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __del__(self):
        """Cleanup: close connection when object is destroyed"""
        self.close_connection()
