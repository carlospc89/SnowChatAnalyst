"""
LangChain-based Snowflake Cortex Agent
Integrates Snowflake Cortex with LangChain for enhanced AI capabilities
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime

from langchain.agents import AgentType, initialize_agent, Tool
from langchain.memory import ConversationBufferWindowMemory, ConversationSummaryBufferMemory
from langchain.schema import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain.tools import BaseTool
from langchain.callbacks.manager import CallbackManagerForToolRun
from langchain_core.language_models.llms import LLM
from langchain_core.outputs import LLMResult, Generation
from pydantic import Field

from snowflake_client import SnowflakeClient
from web_search_handler import WebSearchHandler

# Configure logger
logger = logging.getLogger(__name__)


class SnowflakeCortexLLM(LLM):
    """
    LangChain-compatible wrapper for Snowflake Cortex Complete
    """
    
    def __init__(self, snowflake_client: SnowflakeClient, model: str = "llama3.1-8b", **kwargs):
        super().__init__(**kwargs)
        self._snowflake_client = snowflake_client
        self._model = model
    
    @property
    def _llm_type(self) -> str:
        return "snowflake_cortex"
    
    def _call(self, prompt: str, stop: Optional[List[str]] = None) -> str:
        """Call Snowflake Cortex Complete"""
        try:
            # Clean the prompt for SQL injection prevention
            clean_prompt = prompt.replace("'", "''")
            
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{self._model}',
                '{clean_prompt}'
            ) as response
            """
            
            result = self._snowflake_client.execute_query(cortex_query)
            
            if result is not None and not result.empty:
                return result.iloc[0]['RESPONSE']
            else:
                return "I apologize, but I couldn't process your request at this time."
                
        except Exception as e:
            logger.error(f"Error calling Snowflake Cortex: {str(e)}")
            return f"Error: {str(e)}"


class SnowflakeQueryTool(BaseTool):
    """
    LangChain tool for executing SQL queries against Snowflake
    """
    
    name: str = "snowflake_query"
    description: str = """Execute SQL queries against Snowflake database. Input should be a valid SQL query string. Returns query results as a formatted string."""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        super().__init__()
        self.snowflake_client = snowflake_client
    
    def _run(self, query: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        """Execute SQL query and return results"""
        try:
            logger.info(f"Executing SQL query: {query}")
            
            result = self.snowflake_client.execute_query(query)
            
            if result is not None and not result.empty:
                # Format results for better readability
                if len(result) > 10:
                    # Show first 10 rows for large results
                    formatted_result = f"Query returned {len(result)} rows. First 10 rows:\n{result.head(10).to_string()}"
                else:
                    formatted_result = f"Query returned {len(result)} rows:\n{result.to_string()}"
                
                return formatted_result
            else:
                return "Query executed successfully but returned no results."
                
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}"
            logger.error(error_msg)
            return error_msg


class SnowflakeSchemaTool(BaseTool):
    """
    LangChain tool for exploring Snowflake database schema
    """
    
    name: str = "snowflake_schema"
    description: str = """Get information about Snowflake database schema including tables, columns, and data types. Input can be: 'tables' - list all tables, 'table_name' - get schema for specific table, 'search:keyword' - search for tables containing keyword"""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        super().__init__()
        self.snowflake_client = snowflake_client
    
    def _run(self, query: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        """Get schema information"""
        try:
            if query.lower() == 'tables':
                # Get all tables
                tables = self.snowflake_client.get_tables()
                if tables is not None and not tables.empty:
                    return f"Available tables:\n{tables.to_string()}"
                else:
                    return "No tables found in the current database/schema."
            
            elif query.lower().startswith('search:'):
                # Search for tables
                keyword = query[7:].strip()
                tables = self.snowflake_client.get_tables()
                if tables is not None and not tables.empty:
                    filtered_tables = tables[tables['TABLE_NAME'].str.contains(keyword, case=False, na=False)]
                    if not filtered_tables.empty:
                        return f"Tables containing '{keyword}':\n{filtered_tables.to_string()}"
                    else:
                        return f"No tables found containing '{keyword}'"
                else:
                    return "No tables found to search."
            
            else:
                # Get table schema
                table_schema = self.snowflake_client.get_table_schema(query)
                if table_schema is not None and not table_schema.empty:
                    return f"Schema for table '{query}':\n{table_schema.to_string()}"
                else:
                    return f"Table '{query}' not found or no schema information available."
                    
        except Exception as e:
            error_msg = f"Error getting schema information: {str(e)}"
            logger.error(error_msg)
            return error_msg


class WebSearchTool(BaseTool):
    """
    LangChain tool for web search using Tavily
    """
    
    name: str = "web_search"
    description: str = """Search the web for current information using Tavily API. Input should be a search query string. Returns formatted search results."""
    
    def __init__(self, web_search_handler: WebSearchHandler):
        super().__init__()
        self.web_search_handler = web_search_handler
    
    def _run(self, query: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        """Perform web search"""
        try:
            if not self.web_search_handler.is_available():
                return "Web search is not available. Please configure Tavily API key."
            
            logger.info(f"Performing web search: {query}")
            
            search_results = self.web_search_handler.search(query)
            
            if search_results.get('success'):
                return self.web_search_handler.get_context_for_llm(search_results)
            else:
                return f"Web search failed: {search_results.get('error', 'Unknown error')}"
                
        except Exception as e:
            error_msg = f"Error performing web search: {str(e)}"
            logger.error(error_msg)
            return error_msg


class SnowflakeAnalystTool(BaseTool):
    """
    LangChain tool for advanced data analysis using Snowflake Cortex Analyst
    """
    
    name: str = "snowflake_analyst"
    description: str = """Generate SQL queries from natural language using Snowflake Cortex Analyst. Input should be a natural language question about data. Returns SQL query and analysis results."""
    
    def __init__(self, snowflake_client: SnowflakeClient, semantic_model: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.snowflake_client = snowflake_client
        self.semantic_model = semantic_model
    
    def _run(self, question: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        """Generate and execute SQL from natural language"""
        try:
            logger.info(f"Analyzing question with Cortex Analyst: {question}")
            
            # Create context prompt for Cortex Analyst
            context_prompt = self._create_analyst_prompt(question)
            
            # Use Cortex Analyst to generate SQL
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.ANALYST(
                '{context_prompt.replace("'", "''")}'
            ) as analysis_result
            """
            
            result = self.snowflake_client.execute_query(cortex_query)
            
            if result is not None and not result.empty:
                analysis_result = result.iloc[0]['ANALYSIS_RESULT']
                
                # Try to parse the result and extract SQL
                try:
                    parsed_result = json.loads(analysis_result)
                    sql_query = parsed_result.get('sql', '')
                    
                    if sql_query:
                        # Execute the generated SQL
                        query_result = self.snowflake_client.execute_query(sql_query)
                        
                        if query_result is not None and not query_result.empty:
                            return f"Generated SQL:\n{sql_query}\n\nResults:\n{query_result.to_string()}"
                        else:
                            return f"Generated SQL:\n{sql_query}\n\nQuery executed but returned no results."
                    else:
                        return f"Analysis result: {analysis_result}"
                        
                except json.JSONDecodeError:
                    return f"Analysis result: {analysis_result}"
            else:
                return "Could not generate analysis with Cortex Analyst."
                
        except Exception as e:
            error_msg = f"Error with Cortex Analyst: {str(e)}"
            logger.error(error_msg)
            return error_msg
    
    def _create_analyst_prompt(self, question: str) -> str:
        """Create prompt for Cortex Analyst"""
        base_prompt = f"""
        You are a data analyst assistant. Analyze the following question and generate appropriate SQL query.
        
        Question: {question}
        
        Context:
        - Database: Snowflake
        - Use proper table and column references
        - Include database and schema in table names when necessary
        
        Please provide your analysis in JSON format with the following structure:
        {{
            "sql": "generated SQL query",
            "explanation": "explanation of the query",
            "assumptions": "any assumptions made"
        }}
        """
        
        if self.semantic_model:
            # Add semantic model context
            base_prompt += f"\n\nSemantic Model Context:\n{json.dumps(self.semantic_model, indent=2)}"
        
        return base_prompt


class LangChainSnowflakeAgent:
    """
    Main LangChain-based Snowflake Cortex Agent
    """
    
    def __init__(self, snowflake_client: SnowflakeClient, 
                 web_search_handler: Optional[WebSearchHandler] = None,
                 semantic_model: Optional[Dict[str, Any]] = None,
                 model: str = "llama3.1-8b"):
        
        self.snowflake_client = snowflake_client
        self.web_search_handler = web_search_handler
        self.semantic_model = semantic_model
        self.model = model
        
        # Initialize LLM
        self.llm = SnowflakeCortexLLM(snowflake_client, model)
        
        # Initialize tools
        self.tools = self._initialize_tools()
        
        # Initialize memory
        self.memory = ConversationBufferWindowMemory(
            memory_key="chat_history",
            return_messages=True,
            k=10  # Keep last 10 exchanges
        )
        
        # Initialize agent
        self.agent = initialize_agent(
            tools=self.tools,
            llm=self.llm,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            memory=self.memory,
            verbose=True,
            max_iterations=5,
            early_stopping_method="generate"
        )
    
    def _initialize_tools(self) -> List[Tool]:
        """Initialize all available tools"""
        tools = [
            SnowflakeQueryTool(self.snowflake_client),
            SnowflakeSchemaTool(self.snowflake_client),
            SnowflakeAnalystTool(self.snowflake_client, self.semantic_model)
        ]
        
        # Add web search tool if available
        if self.web_search_handler and self.web_search_handler.is_available():
            tools.append(WebSearchTool(self.web_search_handler))
        
        return tools
    
    def process_query(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process user query using LangChain agent
        
        Args:
            query: User's question
            context: Additional context information
            
        Returns:
            dict: Processing result with response, metadata, and tools used
        """
        try:
            start_time = datetime.now()
            
            # Add context to the query if provided
            if context:
                contextual_query = f"Context: {json.dumps(context)}\n\nQuery: {query}"
            else:
                contextual_query = query
            
            # Process with agent
            result = self.agent.run(contextual_query)
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            return {
                'success': True,
                'response': result,
                'processing_time': processing_time,
                'tools_used': self._get_tools_used(),
                'memory_messages': len(self.memory.chat_memory.messages)
            }
            
        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'response': "I apologize, but I encountered an error processing your request."
            }
    
    def _get_tools_used(self) -> List[str]:
        """Get list of tools used in the last interaction"""
        # This is a simplified implementation
        # In a real scenario, you'd track tool usage through callbacks
        return [tool.name for tool in self.tools]
    
    def update_semantic_model(self, semantic_model: Dict[str, Any]):
        """Update semantic model for the analyst tool"""
        self.semantic_model = semantic_model
        
        # Update the analyst tool
        for tool in self.tools:
            if isinstance(tool, SnowflakeAnalystTool):
                tool.semantic_model = semantic_model
    
    def clear_memory(self):
        """Clear conversation memory"""
        self.memory.clear()
    
    def get_memory_summary(self) -> str:
        """Get a summary of the conversation memory"""
        messages = self.memory.chat_memory.messages
        if not messages:
            return "No conversation history."
        
        summary = f"Conversation has {len(messages)} messages:\n"
        for i, msg in enumerate(messages[-5:]):  # Show last 5 messages
            msg_type = "Human" if isinstance(msg, HumanMessage) else "AI"
            summary += f"{i+1}. {msg_type}: {msg.content[:100]}{'...' if len(msg.content) > 100 else ''}\n"
        
        return summary
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get list of available tools with descriptions"""
        return [
            {
                'name': tool.name,
                'description': tool.description
            }
            for tool in self.tools
        ]