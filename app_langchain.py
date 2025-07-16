"""
Enhanced Snowflake Cortex Analyst Chatbot with LangChain Integration
"""

import streamlit as st
import pandas as pd
import logging
import os
import yaml
import time
import uuid
from typing import Dict, Any, List, Optional

from snowflake_client import SnowflakeClient
from web_search_handler import WebSearchHandler
from langchain_snowflake_agent import LangChainSnowflakeAgent
from memory_manager import MemoryManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('snowflake_langchain_chatbot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Snowflake Cortex Analyst with LangChain",
    page_icon="🤖",
    layout="wide"
)


def initialize_session_state():
    """Initialize session state variables"""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'snowflake_client' not in st.session_state:
        st.session_state.snowflake_client = None
    if 'langchain_agent' not in st.session_state:
        st.session_state.langchain_agent = None
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'connection_status' not in st.session_state:
        st.session_state.connection_status = None
    if 'semantic_model_uploaded' not in st.session_state:
        st.session_state.semantic_model_uploaded = False
    if 'semantic_model_content' not in st.session_state:
        st.session_state.semantic_model_content = None
    if 'memory_manager' not in st.session_state:
        st.session_state.memory_manager = MemoryManager()
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if 'web_search_handler' not in st.session_state:
        st.session_state.web_search_handler = None
    if 'selected_model' not in st.session_state:
        st.session_state.selected_model = 'llama3.1-8b'
    if 'agent_tools_enabled' not in st.session_state:
        st.session_state.agent_tools_enabled = {
            'snowflake_query': True,
            'snowflake_schema': True,
            'snowflake_analyst': True,
            'web_search': False
        }


def reset_connection():
    """Reset connection state"""
    st.session_state.authenticated = False
    st.session_state.snowflake_client = None
    st.session_state.langchain_agent = None
    st.session_state.connection_status = None
    st.session_state.chat_history = []
    st.session_state.semantic_model_uploaded = False
    st.session_state.semantic_model_content = None


def authentication_tab():
    """Handle authentication and Snowflake connection setup"""
    st.header("🔐 Snowflake Authentication")
    
    if not st.session_state.authenticated:
        st.info("Please configure your Snowflake connection to get started.")
        
        with st.form("snowflake_connection"):
            col1, col2 = st.columns(2)
            
            with col1:
                account = st.text_input("Account Identifier", help="Your Snowflake account identifier")
                user = st.text_input("Username", help="Your Snowflake username")
                warehouse = st.text_input("Warehouse", help="Snowflake warehouse to use")
                
            with col2:
                database = st.text_input("Database", help="Database name")
                schema = st.text_input("Schema", help="Schema name")
                role = st.text_input("Role (Optional)", help="Snowflake role to use")
            
            # Web search configuration
            st.subheader("🌐 Web Search Configuration (Optional)")
            tavily_api_key = st.text_input("Tavily API Key", type="password", 
                                         help="Optional: API key for web search capabilities")
            
            submit_button = st.form_submit_button("Connect to Snowflake")
            
            if submit_button:
                if account and user and warehouse and database and schema:
                    with st.spinner("Connecting to Snowflake..."):
                        try:
                            # Initialize Snowflake client
                            snowflake_client = SnowflakeClient(
                                account=account,
                                user=user,
                                warehouse=warehouse,
                                database=database,
                                schema=schema,
                                role=role
                            )
                            
                            # Test connection
                            if snowflake_client.connect():
                                st.session_state.snowflake_client = snowflake_client
                                st.session_state.account = account
                                st.session_state.user = user
                                st.session_state.warehouse = warehouse
                                st.session_state.database = database
                                st.session_state.schema = schema
                                st.session_state.role = role
                                
                                # Initialize web search handler if API key provided
                                if tavily_api_key:
                                    st.session_state.web_search_handler = WebSearchHandler(tavily_api_key)
                                    st.session_state.agent_tools_enabled['web_search'] = True
                                
                                # Initialize LangChain agent
                                st.session_state.langchain_agent = LangChainSnowflakeAgent(
                                    snowflake_client=snowflake_client,
                                    web_search_handler=st.session_state.web_search_handler,
                                    semantic_model=st.session_state.semantic_model_content,
                                    model=st.session_state.selected_model
                                )
                                
                                # Create session in memory manager
                                st.session_state.memory_manager.create_session(
                                    session_id=st.session_state.session_id,
                                    snowflake_account=account,
                                    database=database,
                                    schema=schema
                                )
                                
                                st.session_state.authenticated = True
                                st.session_state.connection_status = "Connected"
                                st.success("✅ Successfully connected to Snowflake!")
                                st.rerun()
                                
                            else:
                                st.error("❌ Failed to connect to Snowflake. Please check your credentials.")
                                
                        except Exception as e:
                            st.error(f"❌ Connection error: {str(e)}")
                            logger.error(f"Connection error: {str(e)}")
                else:
                    st.error("Please fill in all required fields.")
    
    else:
        st.success("✅ Connected to Snowflake successfully!")
        
        # Display connection info
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Account:** {st.session_state.get('account', 'N/A')}")
            st.info(f"**User:** {st.session_state.get('user', 'N/A')}")
            st.info(f"**Warehouse:** {st.session_state.get('warehouse', 'N/A')}")
        
        with col2:
            st.info(f"**Database:** {st.session_state.get('database', 'N/A')}")
            st.info(f"**Schema:** {st.session_state.get('schema', 'N/A')}")
            st.info(f"**Role:** {st.session_state.get('role', 'Default')}")
        
        # Web search status
        if st.session_state.web_search_handler:
            st.success("🌐 Web search enabled via Tavily API")
        else:
            st.warning("🌐 Web search not configured")
        
        # Agent tools status
        st.subheader("🛠️ Available Agent Tools")
        available_tools = st.session_state.langchain_agent.get_available_tools()
        for tool in available_tools:
            st.info(f"**{tool['name']}:** {tool['description']}")
        
        if st.button("Disconnect", type="secondary"):
            reset_connection()
            st.rerun()


def semantic_model_tab():
    """Handle semantic model upload and management"""
    st.header("📊 Semantic Model Management")
    
    if not st.session_state.authenticated:
        st.warning("Please connect to Snowflake first in the Authentication tab.")
        return
    
    st.info("Upload a YAML semantic model to improve data query accuracy and context understanding.")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Upload Semantic Model (YAML)",
        type=['yaml', 'yml'],
        help="Upload a YAML file containing your semantic model definition"
    )
    
    if uploaded_file is not None:
        try:
            # Parse YAML content
            yaml_content = yaml.safe_load(uploaded_file.read())
            
            # Display parsed content
            st.subheader("📋 Parsed Semantic Model")
            st.json(yaml_content)
            
            if st.button("Load Semantic Model"):
                # Update session state
                st.session_state.semantic_model_content = yaml_content
                st.session_state.semantic_model_uploaded = True
                
                # Update LangChain agent
                if st.session_state.langchain_agent:
                    st.session_state.langchain_agent.update_semantic_model(yaml_content)
                
                # Update memory manager
                st.session_state.memory_manager.update_semantic_model_status(
                    st.session_state.session_id, True
                )
                
                st.success("✅ Semantic model loaded successfully!")
                st.rerun()
                
        except yaml.YAMLError as e:
            st.error(f"❌ Error parsing YAML file: {str(e)}")
        except Exception as e:
            st.error(f"❌ Error loading semantic model: {str(e)}")
    
    # Display current semantic model status
    if st.session_state.semantic_model_uploaded:
        st.success("✅ Semantic model is currently loaded")
        
        with st.expander("View Current Semantic Model"):
            st.json(st.session_state.semantic_model_content)
        
        if st.button("Remove Semantic Model"):
            st.session_state.semantic_model_content = None
            st.session_state.semantic_model_uploaded = False
            
            # Update LangChain agent
            if st.session_state.langchain_agent:
                st.session_state.langchain_agent.update_semantic_model(None)
            
            # Update memory manager
            st.session_state.memory_manager.update_semantic_model_status(
                st.session_state.session_id, False
            )
            
            st.success("✅ Semantic model removed")
            st.rerun()
    
    else:
        st.info("No semantic model currently loaded. The agent will use automatic schema discovery.")


def agent_chat_tab():
    """Handle LangChain agent chat interface"""
    st.header("🤖 LangChain Agent Chat")
    
    if not st.session_state.authenticated:
        st.warning("Please connect to Snowflake first in the Authentication tab.")
        return
    
    # Sidebar for agent configuration
    with st.sidebar:
        st.subheader("⚙️ Agent Configuration")
        
        # Model selection
        model_options = {
            'llama3.1-8b': 'Llama 3.1 8B (Fast)',
            'llama3.1-70b': 'Llama 3.1 70B (High Quality)',
            'mistral-7b': 'Mistral 7B (Balanced)'
        }
        
        selected_model = st.selectbox(
            "LLM Model",
            options=list(model_options.keys()),
            index=list(model_options.keys()).index(st.session_state.selected_model),
            format_func=lambda x: model_options[x]
        )
        
        if selected_model != st.session_state.selected_model:
            st.session_state.selected_model = selected_model
            # Recreate agent with new model
            if st.session_state.langchain_agent:
                st.session_state.langchain_agent = LangChainSnowflakeAgent(
                    snowflake_client=st.session_state.snowflake_client,
                    web_search_handler=st.session_state.web_search_handler,
                    semantic_model=st.session_state.semantic_model_content,
                    model=selected_model
                )
        
        # Agent tools configuration
        st.subheader("🛠️ Agent Tools")
        
        # Tool toggles
        for tool_name, description in [
            ('snowflake_query', 'Direct SQL execution'),
            ('snowflake_schema', 'Schema exploration'),
            ('snowflake_analyst', 'Natural language to SQL'),
            ('web_search', 'Web search (requires API key)')
        ]:
            enabled = st.checkbox(
                description,
                value=st.session_state.agent_tools_enabled.get(tool_name, True),
                key=f"tool_{tool_name}",
                disabled=(tool_name == 'web_search' and not st.session_state.web_search_handler)
            )
            st.session_state.agent_tools_enabled[tool_name] = enabled
        
        # Memory management
        st.subheader("🧠 Memory Management")
        
        if st.session_state.langchain_agent:
            memory_summary = st.session_state.langchain_agent.get_memory_summary()
            st.text_area("Memory Summary", value=memory_summary, height=100, disabled=True)
            
            if st.button("Clear Memory"):
                st.session_state.langchain_agent.clear_memory()
                st.success("Memory cleared!")
                st.rerun()
        
        # Session statistics
        st.subheader("📈 Session Stats")
        session_stats = st.session_state.memory_manager.get_session_stats(st.session_state.session_id)
        st.metric("User Messages", session_stats.get('user_messages', 0))
        st.metric("Assistant Messages", session_stats.get('assistant_messages', 0))
        st.metric("Successful Queries", session_stats.get('successful_queries', 0))
    
    # Main chat interface
    st.subheader("💬 Chat with LangChain Agent")
    
    # Display chat history
    chat_container = st.container()
    
    with chat_container:
        for message in st.session_state.chat_history:
            with st.chat_message(message["role"]):
                st.write(message["content"])
                
                # Show additional metadata for agent responses
                if message["role"] == "assistant" and "metadata" in message:
                    metadata = message["metadata"]
                    
                    with st.expander("Agent Metadata", expanded=False):
                        st.json({
                            "processing_time": f"{metadata.get('processing_time', 0):.2f}s",
                            "tools_used": metadata.get('tools_used', []),
                            "memory_messages": metadata.get('memory_messages', 0)
                        })
    
    # Chat input
    if prompt := st.chat_input("Ask me anything about your data..."):
        # Add user message to history
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        
        # Add to memory manager
        st.session_state.memory_manager.add_message(
            st.session_state.session_id,
            'user',
            prompt
        )
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Process with LangChain agent
        with st.chat_message("assistant"):
            with st.spinner("LangChain agent is thinking..."):
                try:
                    # Prepare context
                    context = {
                        'database': st.session_state.get('database', ''),
                        'schema': st.session_state.get('schema', ''),
                        'semantic_model_available': st.session_state.semantic_model_uploaded,
                        'tools_enabled': st.session_state.agent_tools_enabled
                    }
                    
                    # Process query
                    result = st.session_state.langchain_agent.process_query(prompt, context)
                    
                    if result['success']:
                        response = result['response']
                        st.write(response)
                        
                        # Add to chat history with metadata
                        st.session_state.chat_history.append({
                            "role": "assistant",
                            "content": response,
                            "metadata": {
                                "processing_time": result.get('processing_time', 0),
                                "tools_used": result.get('tools_used', []),
                                "memory_messages": result.get('memory_messages', 0)
                            }
                        })
                        
                        # Add to memory manager
                        st.session_state.memory_manager.add_message(
                            st.session_state.session_id,
                            'assistant',
                            response,
                            execution_status='success'
                        )
                        
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        st.error(f"❌ Error: {error_msg}")
                        
                        # Add error to chat history
                        st.session_state.chat_history.append({
                            "role": "assistant",
                            "content": f"I apologize, but I encountered an error: {error_msg}"
                        })
                        
                        # Add to memory manager
                        st.session_state.memory_manager.add_message(
                            st.session_state.session_id,
                            'assistant',
                            f"Error: {error_msg}",
                            execution_status='error'
                        )
                
                except Exception as e:
                    error_msg = f"Unexpected error: {str(e)}"
                    st.error(f"❌ {error_msg}")
                    logger.error(error_msg)
                    
                    # Add error to chat history
                    st.session_state.chat_history.append({
                        "role": "assistant",
                        "content": f"I apologize, but I encountered an unexpected error: {str(e)}"
                    })


def main():
    """Main application function"""
    initialize_session_state()
    
    st.title("🤖 Snowflake Cortex Analyst with LangChain")
    st.markdown("*Powerful data analytics with AI agents, tools, and memory*")
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["🔐 Authentication", "📊 Semantic Model", "🤖 Agent Chat"])
    
    with tab1:
        authentication_tab()
    
    with tab2:
        semantic_model_tab()
    
    with tab3:
        agent_chat_tab()


if __name__ == "__main__":
    main()