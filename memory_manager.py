import sqlite3
import datetime
import json
from typing import List, Dict, Any, Optional
import pandas as pd


class MemoryManager:
    """In-memory database for managing chat history and user interactions"""
    
    def __init__(self):
        """Initialize in-memory SQLite database"""
        self.connection = sqlite3.connect(':memory:', check_same_thread=False)
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary tables for chat history"""
        cursor = self.connection.cursor()
        
        # Chat sessions table
        cursor.execute('''
            CREATE TABLE chat_sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                semantic_model_used BOOLEAN DEFAULT FALSE,
                snowflake_account TEXT,
                database_name TEXT,
                schema_name TEXT
            )
        ''')
        
        # Chat messages table
        cursor.execute('''
            CREATE TABLE chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                message_type TEXT, -- 'user', 'assistant', 'system'
                content TEXT,
                sql_query TEXT,
                execution_status TEXT, -- 'success', 'error', 'warning'
                result_rows INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                semantic_model_version TEXT,
                FOREIGN KEY (session_id) REFERENCES chat_sessions (session_id)
            )
        ''')
        
        # Query performance tracking
        cursor.execute('''
            CREATE TABLE query_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                question TEXT,
                sql_query TEXT,
                execution_time_ms INTEGER,
                rows_returned INTEGER,
                has_semantic_model BOOLEAN,
                success BOOLEAN,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES chat_sessions (session_id)
            )
        ''')
        
        self.connection.commit()
    
    def create_session(self, session_id: str, user_id: str = "default", 
                      snowflake_account: str = None, database: str = None, 
                      schema: str = None) -> bool:
        """
        Create a new chat session
        
        Args:
            session_id: Unique session identifier
            user_id: User identifier
            snowflake_account: Snowflake account being used
            database: Database name
            schema: Schema name
            
        Returns:
            bool: True if session created successfully
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO chat_sessions 
                (session_id, user_id, snowflake_account, database_name, schema_name)
                VALUES (?, ?, ?, ?, ?)
            ''', (session_id, user_id, snowflake_account, database, schema))
            
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error creating session: {str(e)}")
            return False
    
    def add_message(self, session_id: str, message_type: str, content: str, 
                   sql_query: str = None, execution_status: str = None, 
                   result_rows: int = None, semantic_model_version: str = None) -> bool:
        """
        Add a message to the chat history
        
        Args:
            session_id: Session identifier
            message_type: Type of message ('user', 'assistant', 'system')
            content: Message content
            sql_query: SQL query if applicable
            execution_status: Query execution status
            result_rows: Number of rows returned
            semantic_model_version: Version of semantic model used
            
        Returns:
            bool: True if message added successfully
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT INTO chat_messages 
                (session_id, message_type, content, sql_query, execution_status, 
                 result_rows, semantic_model_version)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (session_id, message_type, content, sql_query, execution_status, 
                  result_rows, semantic_model_version))
            
            # Update session activity
            cursor.execute('''
                UPDATE chat_sessions 
                SET last_activity = CURRENT_TIMESTAMP 
                WHERE session_id = ?
            ''', (session_id,))
            
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error adding message: {str(e)}")
            return False
    
    def get_chat_history(self, session_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Retrieve chat history for a session
        
        Args:
            session_id: Session identifier
            limit: Maximum number of messages to retrieve
            
        Returns:
            List of chat messages
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                SELECT message_type, content, sql_query, execution_status, 
                       result_rows, timestamp, semantic_model_version
                FROM chat_messages 
                WHERE session_id = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (session_id, limit))
            
            messages = []
            for row in cursor.fetchall():
                messages.append({
                    'message_type': row[0],
                    'content': row[1],
                    'sql_query': row[2],
                    'execution_status': row[3],
                    'result_rows': row[4],
                    'timestamp': row[5],
                    'semantic_model_version': row[6]
                })
            
            return list(reversed(messages))  # Return in chronological order
        except Exception as e:
            print(f"Error retrieving chat history: {str(e)}")
            return []
    
    def update_semantic_model_status(self, session_id: str, has_semantic_model: bool):
        """
        Update semantic model status for a session
        
        Args:
            session_id: Session identifier
            has_semantic_model: Whether semantic model is being used
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                UPDATE chat_sessions 
                SET semantic_model_used = ? 
                WHERE session_id = ?
            ''', (has_semantic_model, session_id))
            
            self.connection.commit()
        except Exception as e:
            print(f"Error updating semantic model status: {str(e)}")
    
    def log_query_performance(self, session_id: str, question: str, sql_query: str, 
                            execution_time_ms: int, rows_returned: int, 
                            has_semantic_model: bool, success: bool):
        """
        Log query performance metrics
        
        Args:
            session_id: Session identifier
            question: Original user question
            sql_query: Generated SQL query
            execution_time_ms: Execution time in milliseconds
            rows_returned: Number of rows returned
            has_semantic_model: Whether semantic model was used
            success: Whether query was successful
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT INTO query_performance 
                (session_id, question, sql_query, execution_time_ms, rows_returned, 
                 has_semantic_model, success)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (session_id, question, sql_query, execution_time_ms, rows_returned, 
                  has_semantic_model, success))
            
            self.connection.commit()
        except Exception as e:
            print(f"Error logging query performance: {str(e)}")
    
    def get_session_stats(self, session_id: str) -> Dict[str, Any]:
        """
        Get statistics for a session
        
        Args:
            session_id: Session identifier
            
        Returns:
            Dictionary with session statistics
        """
        try:
            cursor = self.connection.cursor()
            
            # Get basic session info
            cursor.execute('''
                SELECT semantic_model_used, created_at, last_activity
                FROM chat_sessions 
                WHERE session_id = ?
            ''', (session_id,))
            session_info = cursor.fetchone()
            
            if not session_info:
                return {}
            
            # Get message counts
            cursor.execute('''
                SELECT COUNT(*) as total_messages,
                       SUM(CASE WHEN message_type = 'user' THEN 1 ELSE 0 END) as user_messages,
                       SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) as successful_queries
                FROM chat_messages 
                WHERE session_id = ?
            ''', (session_id,))
            message_stats = cursor.fetchone()
            
            return {
                'semantic_model_used': bool(session_info[0]),
                'created_at': session_info[1],
                'last_activity': session_info[2],
                'total_messages': message_stats[0] if message_stats else 0,
                'user_messages': message_stats[1] if message_stats else 0,
                'successful_queries': message_stats[2] if message_stats else 0
            }
        except Exception as e:
            print(f"Error getting session stats: {str(e)}")
            return {}
    
    def clear_session_history(self, session_id: str):
        """
        Clear chat history for a specific session
        
        Args:
            session_id: Session identifier
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute('DELETE FROM chat_messages WHERE session_id = ?', (session_id,))
            cursor.execute('DELETE FROM query_performance WHERE session_id = ?', (session_id,))
            self.connection.commit()
        except Exception as e:
            print(f"Error clearing session history: {str(e)}")
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()