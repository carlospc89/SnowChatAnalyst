import streamlit as st
import pandas as pd
from snowflake_client import SnowflakeClient
from cortex_analyst import CortexAnalyst
from memory_manager import MemoryManager
import os
import yaml
import sqlite3
import datetime
import time
import uuid
from typing import List, Dict, Any

# Page configuration
st.set_page_config(
    page_title="Snowflake Cortex Analyst Chatbot",
    page_icon="❄️",
    layout="wide"
)

def initialize_session_state():
    """Initialize session state variables"""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'snowflake_client' not in st.session_state:
        st.session_state.snowflake_client = None
    if 'cortex_analyst' not in st.session_state:
        st.session_state.cortex_analyst = None
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

def reset_connection():
    """Reset connection state"""
    st.session_state.authenticated = False
    st.session_state.snowflake_client = None
    st.session_state.cortex_analyst = None
    st.session_state.connection_status = None
    st.session_state.chat_history = []
    st.session_state.semantic_model_uploaded = False
    st.session_state.semantic_model_content = None

def authentication_tab():
    """Handle authentication and Snowflake connection setup"""
    st.header("🔐 Snowflake Authentication")
    
    if st.session_state.authenticated:
        st.success("✅ Successfully connected to Snowflake!")
        
        # Display connection info
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Account:** {st.session_state.account}")
            st.info(f"**Database:** {st.session_state.database}")
            st.info(f"**Schema:** {st.session_state.schema}")
        with col2:
            st.info(f"**Warehouse:** {st.session_state.warehouse}")
            st.info(f"**User:** {st.session_state.username}")
            if hasattr(st.session_state, 'role') and st.session_state.role:
                st.info(f"**Role:** {st.session_state.role}")
        
        if st.button("🔄 Disconnect", type="secondary"):
            reset_connection()
            st.rerun()
        
        return
    
    st.write("**External Browser Authentication**: This app uses Snowflake's secure browser-based authentication (SSO).")
    st.info("💡 When you click 'Connect', your default browser will open for Snowflake authentication. Make sure you're logged into Snowflake in your browser.")
    
    # Warning for browser environments
    st.warning("⚠️ **Note**: External browser authentication requires access to your local browser. This works best when running locally or in environments that support browser access.")
    
    with st.form("snowflake_auth"):
        col1, col2 = st.columns(2)
        
        with col1:
            account = st.text_input(
                "Account Identifier", 
                help="Your Snowflake account identifier (e.g., abc12345.us-east-1)"
            )
            username = st.text_input(
                "Username",
                help="Your Snowflake username"
            )
            warehouse = st.text_input(
                "Warehouse", 
                help="Snowflake warehouse to use for queries"
            )
        
        with col2:
            database = st.text_input(
                "Database", 
                help="Database containing your data"
            )
            schema = st.text_input(
                "Schema", 
                value="PUBLIC",
                help="Schema within the database"
            )
            role = st.text_input(
                "Role (Optional)", 
                help="Snowflake role to use for connection"
            )
        
        submitted = st.form_submit_button("🔗 Connect via Browser", type="primary")
    
    if submitted:
        if not all([account, username, warehouse, database, schema]):
            st.error("❌ Please fill in all required fields.")
            return
        
        with st.spinner("Opening browser for authentication... Please complete the login in your browser."):
            try:
                # Create Snowflake client with external browser authentication
                client = SnowflakeClient(
                    account=account,
                    user=username,
                    warehouse=warehouse,
                    database=database,
                    schema=schema,
                    role=role if role.strip() else None
                )
                
                # Test connection (this will open browser)
                if client.test_connection():
                    # Store in session state
                    st.session_state.snowflake_client = client
                    st.session_state.cortex_analyst = CortexAnalyst(client)
                    st.session_state.authenticated = True
                    st.session_state.account = account
                    st.session_state.username = username
                    st.session_state.warehouse = warehouse
                    st.session_state.database = database
                    st.session_state.schema = schema
                    st.session_state.role = role if role.strip() else None
                    st.session_state.connection_status = "Connected"
                    
                    # Create memory session
                    st.session_state.memory_manager.create_session(
                        session_id=st.session_state.session_id,
                        snowflake_account=account,
                        database=database,
                        schema=schema
                    )
                    
                    st.success("✅ Successfully connected to Snowflake!")
                    st.rerun()
                else:
                    st.error("❌ Failed to connect to Snowflake. Please check your credentials and try again.")
            
            except Exception as e:
                st.error(f"❌ Connection error: {str(e)}")
                st.error("💡 Make sure you have access to Snowflake and complete the browser authentication.")

def _handle_general_question(question: str) -> Dict[str, Any]:
    """
    Handle general questions without SQL generation
    
    Args:
        question: User's general question
        
    Returns:
        dict: Response structure similar to Cortex Analyst but without SQL
    """
    question_lower = question.lower()
    
    # Greeting responses
    if any(greeting in question_lower for greeting in ['hello', 'hi', 'hey']):
        response = ("Hello! I'm your Snowflake Cortex Analyst assistant. I can help you with:\n\n"
                   "📊 **Data Analysis**: Ask questions about your data (works best with a semantic model)\n"
                   "💡 **SQL Help**: Get assistance with SQL queries and database concepts\n"
                   "🔧 **Technical Support**: Learn about Snowflake features and best practices\n\n"
                   "What would you like to explore today?")
    
    elif any(pattern in question_lower for pattern in ['what can you do', 'help', 'capabilities']):
        response = ("I'm designed to help you interact with your Snowflake data in two main ways:\n\n"
                   "🔍 **Data Queries**: I can convert your natural language questions into SQL queries and execute them against your Snowflake database. This works best when you upload a semantic model.\n\n"
                   "💬 **General Assistance**: I can help with SQL concepts, explain database terminology, provide best practices, and answer technical questions about Snowflake.\n\n"
                   "**Current Status**: " + 
                   ("✅ Custom semantic model loaded - ready for accurate data queries!" if st.session_state.semantic_model_uploaded 
                    else "⚠️ No semantic model uploaded - data queries may be less accurate"))
    
    elif any(pattern in question_lower for pattern in ['thank you', 'thanks']):
        response = "You're welcome! Feel free to ask me anything about your data or SQL queries."
    
    elif any(pattern in question_lower for pattern in ['bye', 'goodbye']):
        response = "Goodbye! Your session data is saved. Feel free to return anytime to continue exploring your data."
    
    else:
        # General conversational response
        response = ("I understand you're asking about general topics. While I'm specialized in helping with Snowflake data analysis, "
                   "I can also assist with:\n\n"
                   "• SQL query writing and optimization\n"
                   "• Database concepts and terminology\n"
                   "• Snowflake features and best practices\n"
                   "• Data analysis methodologies\n\n"
                   "If you have specific questions about your data, I can help generate SQL queries to find answers. "
                   "For the most accurate results, consider uploading a semantic model first.")
    
    return {
        'success': True,
        'data': None,
        'sql_query': None,
        'response': response,
        'error': None
    }

def semantic_model_tab():
    """Handle semantic model upload and management"""
    st.header("📋 Semantic Model")
    
    if not st.session_state.authenticated:
        st.warning("⚠️ Please authenticate with Snowflake first in the Authentication tab.")
        return
    
    st.write("Upload your semantic model YAML file to provide custom definitions for your data structure.")
    
    # Display current status
    if st.session_state.semantic_model_uploaded:
        st.success("✅ Semantic model loaded successfully!")
        
        with st.expander("📄 View Current Semantic Model", expanded=False):
            if st.session_state.semantic_model_content:
                st.code(st.session_state.semantic_model_content, language="yaml")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Upload New Model", type="secondary"):
                st.session_state.semantic_model_uploaded = False
                st.session_state.semantic_model_content = None
                st.rerun()
        with col2:
            if st.button("🗑️ Remove Model", type="secondary"):
                st.session_state.semantic_model_uploaded = False
                st.session_state.semantic_model_content = None
                # Reinitialize Cortex Analyst without custom model
                if st.session_state.cortex_analyst:
                    st.session_state.cortex_analyst = CortexAnalyst(st.session_state.snowflake_client)
                
                # Update memory manager
                st.session_state.memory_manager.update_semantic_model_status(
                    st.session_state.session_id, False
                )
                st.session_state.memory_manager.add_message(
                    st.session_state.session_id, 
                    'system', 
                    'Semantic model removed. Using automatic schema discovery.',
                    semantic_model_version='auto'
                )
                
                st.success("Semantic model removed. Using automatic schema discovery.")
                st.rerun()
        
        return
    
    # File upload section
    st.subheader("📤 Upload Semantic Model")
    
    uploaded_file = st.file_uploader(
        "Choose a YAML file",
        type=['yaml', 'yml'],
        help="Upload your semantic model definition in YAML format"
    )
    
    if uploaded_file is not None:
        try:
            # Read the file content
            file_content = uploaded_file.read().decode('utf-8')
            
            # Validate YAML format
            yaml_data = yaml.safe_load(file_content)
            
            # Basic validation of semantic model structure
            if not isinstance(yaml_data, dict):
                st.error("❌ Invalid YAML format. The file should contain a dictionary structure.")
                return
            
            # Store the semantic model
            st.session_state.semantic_model_content = file_content
            st.session_state.semantic_model_uploaded = True
            
            # Update Cortex Analyst with custom semantic model
            if st.session_state.cortex_analyst:
                st.session_state.cortex_analyst.load_custom_semantic_model(yaml_data)
            
            # Update memory manager
            st.session_state.memory_manager.update_semantic_model_status(
                st.session_state.session_id, True
            )
            st.session_state.memory_manager.add_message(
                st.session_state.session_id, 
                'system', 
                'Custom semantic model uploaded and loaded',
                semantic_model_version='custom'
            )
            
            st.success("✅ Semantic model uploaded successfully!")
            st.rerun()
            
        except yaml.YAMLError as e:
            st.error(f"❌ Invalid YAML format: {str(e)}")
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
    
    # Sample semantic model format
    with st.expander("📖 Semantic Model Format Example", expanded=False):
        sample_yaml = """
semantic_model:
  name: "Sales Analytics"
  description: "Sales data semantic model"
  
  tables:
    - name: "SALES"
      description: "Sales transactions table"
      columns:
        - name: "SALE_ID"
          type: "NUMBER"
          description: "Unique sale identifier"
        - name: "CUSTOMER_ID"
          type: "NUMBER"
          description: "Customer identifier"
        - name: "PRODUCT_NAME"
          type: "VARCHAR"
          description: "Product name"
        - name: "SALE_AMOUNT"
          type: "NUMBER"
          description: "Sale amount in USD"
        - name: "SALE_DATE"
          type: "DATE"
          description: "Date of sale"
        - name: "REGION"
          type: "VARCHAR"
          description: "Sales region"
    
    - name: "CUSTOMERS"
      description: "Customer information table"
      columns:
        - name: "CUSTOMER_ID"
          type: "NUMBER"
          description: "Unique customer identifier"
        - name: "CUSTOMER_NAME"
          type: "VARCHAR"
          description: "Customer full name"
        - name: "EMAIL"
          type: "VARCHAR"
          description: "Customer email address"

  relationships:
    - from_table: "SALES"
      from_column: "CUSTOMER_ID"
      to_table: "CUSTOMERS"
      to_column: "CUSTOMER_ID"
      type: "many_to_one"
"""
        st.code(sample_yaml, language="yaml")

def chatbot_tab():
    """Handle chatbot interface and natural language queries"""
    if not st.session_state.authenticated:
        st.warning("⚠️ Please authenticate with Snowflake first in the Authentication tab.")
        return
    
    st.header("🤖 Cortex Analyst Chatbot")
    
    # Enhanced semantic model status and warnings
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.session_state.semantic_model_uploaded:
            st.success("📋 **Custom Semantic Model Active** - Enhanced accuracy for data queries")
        else:
            st.warning("⚠️ **No Semantic Model** - Responses about data may be less accurate. Consider uploading a semantic model for better results.")
    
    with col2:
        # Session stats
        stats = st.session_state.memory_manager.get_session_stats(st.session_state.session_id)
        if stats:
            st.metric("Queries", stats.get('user_messages', 0))
    
    # Usage guidance based on semantic model status
    if st.session_state.semantic_model_uploaded:
        st.info("💡 You can ask detailed questions about your data. The semantic model will help generate accurate SQL queries.")
    else:
        st.info("💡 **For data questions**: Responses may be inaccurate without a semantic model. **For general questions**: Feel free to ask anything!")
    
    # Capability distinction
    with st.expander("🎯 What can I help you with?", expanded=False):
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.subheader("📊 Data Questions")
            if st.session_state.semantic_model_uploaded:
                st.write("✅ **Fully Supported** - Accurate SQL generation")
                st.write("• Sales analysis queries")
                st.write("• Complex joins and aggregations")
                st.write("• Trend analysis")
                st.write("• Custom calculations")
            else:
                st.write("⚠️ **Limited Accuracy** - No semantic context")
                st.write("• Basic queries may work")
                st.write("• Results may be incorrect")
                st.write("• Table/column names might be wrong")
                st.write("• **Recommendation**: Upload semantic model first")
        
        with col_b:
            st.subheader("💬 General Questions")
            st.write("✅ **Always Available** - No semantic model needed")
            st.write("• SQL help and explanations")
            st.write("• Database concepts")
            st.write("• Best practices")
            st.write("• Technical guidance")
    
    # Display enhanced chat history with memory
    chat_container = st.container()
    
    with chat_container:
        # Load chat history from memory
        chat_history = st.session_state.memory_manager.get_chat_history(st.session_state.session_id)
        
        if chat_history:
            st.subheader("📜 Chat History")
            for i, msg in enumerate(chat_history):
                if msg['message_type'] == 'user':
                    with st.chat_message("user"):
                        st.write(msg['content'])
                
                elif msg['message_type'] == 'assistant':
                    with st.chat_message("assistant"):
                        st.write(msg['content'])
                        
                        # Show SQL query if available
                        if msg['sql_query']:
                            with st.expander("📋 Generated SQL", expanded=False):
                                st.code(msg['sql_query'], language="sql")
                        
                        # Show execution status
                        if msg['execution_status']:
                            if msg['execution_status'] == 'success':
                                st.success(f"✅ Query executed successfully")
                                if msg['result_rows']:
                                    st.info(f"📊 Returned {msg['result_rows']} rows")
                            elif msg['execution_status'] == 'error':
                                st.error("❌ Query execution failed")
                
                elif msg['message_type'] == 'system':
                    with st.chat_message("assistant", avatar="🔧"):
                        st.info(msg['content'])
    
    # Input for new question
    with st.form("chat_form", clear_on_submit=True):
        user_question = st.text_area(
            "Ask a question about your data:",
            placeholder="e.g., What were the total sales by region last quarter?",
            height=100
        )
        
        col1, col2, col3 = st.columns([1, 1, 4])
        with col1:
            submit_button = st.form_submit_button("🚀 Ask", type="primary")
        with col2:
            clear_button = st.form_submit_button("🗑️ Clear History")
    
    if clear_button:
        st.session_state.memory_manager.clear_session_history(st.session_state.session_id)
        st.session_state.chat_history = []
        st.rerun()
    
    if submit_button and user_question.strip():
        # Enhanced data query detection - more precise patterns
        question_lower = user_question.lower()
        
        # Patterns that indicate data queries (requiring SQL generation)
        data_query_patterns = [
            # Direct SQL mentions
            'select', 'from', 'where', 'group by', 'order by', 'having', 'join',
            # Aggregation requests
            'how many', 'count of', 'total', 'sum of', 'average', 'maximum', 'minimum',
            # Analysis requests
            'show me', 'find', 'list', 'get', 'retrieve', 'analyze', 'breakdown',
            # Data-specific terms with context
            'sales by', 'revenue by', 'customers who', 'products that', 'orders where',
            'data from', 'records from', 'rows from', 'information from',
            # Time-based queries
            'last month', 'this year', 'between', 'since', 'until', 'during',
            # Comparison queries
            'compare', 'versus', 'vs', 'top', 'bottom', 'highest', 'lowest'
        ]
        
        # Exclude common greetings and general questions
        general_patterns = [
            'hello', 'hi', 'hey', 'good morning', 'good afternoon', 'good evening',
            'how are you', 'what can you do', 'help', 'explain', 'what is',
            'how to', 'can you', 'tell me about', 'what does', 'define',
            'thank you', 'thanks', 'bye', 'goodbye'
        ]
        
        # Check if it's a general question first
        is_general_question = any(pattern in question_lower for pattern in general_patterns)
        
        # Check if it's a data query
        is_data_query = (not is_general_question and 
                        any(pattern in question_lower for pattern in data_query_patterns))
        
        # Log user message
        st.session_state.memory_manager.add_message(
            st.session_state.session_id,
            'user',
            user_question
        )
        
        with st.spinner("Processing your question..."):
            start_time = time.time()
            
            try:
                if is_data_query:
                    # This is a data query - use Cortex Analyst for SQL generation
                    if not st.session_state.semantic_model_uploaded:
                        # Warning for data queries without semantic model
                        warning_msg = ("⚠️ **Limited Accuracy Warning**: You're asking about data but no semantic model is uploaded. "
                                     "The response may contain inaccuracies. For better results, please upload a semantic model first.")
                        
                        st.warning(warning_msg)
                        
                        # Add warning to memory
                        st.session_state.memory_manager.add_message(
                            st.session_state.session_id,
                            'system',
                            warning_msg
                        )
                    
                    # Process the data query with SQL generation
                    result = st.session_state.cortex_analyst.process_question(user_question)
                    
                else:
                    # This is a general question - respond without SQL generation
                    result = _handle_general_question(user_question)
                
                execution_time = int((time.time() - start_time) * 1000)
                
                if result['success']:
                    sql_query = result.get('sql_query')
                    data = result.get('data')
                    response_text = result.get('response')
                    
                    if is_data_query and sql_query:
                        # Handle data query response
                        row_count = len(data) if isinstance(data, pd.DataFrame) else 0
                        
                        # Log successful query
                        st.session_state.memory_manager.add_message(
                            st.session_state.session_id,
                            'assistant',
                            f"Query executed successfully. Returned {row_count} rows.",
                            sql_query=sql_query,
                            execution_status='success',
                            result_rows=row_count,
                            semantic_model_version='custom' if st.session_state.semantic_model_uploaded else 'auto'
                        )
                        
                        # Log performance
                        st.session_state.memory_manager.log_query_performance(
                            st.session_state.session_id,
                            user_question,
                            sql_query,
                            execution_time,
                            row_count,
                            st.session_state.semantic_model_uploaded,
                            True
                        )
                        
                        # Display the result
                        st.success("✅ Query processed successfully!")
                        
                        with st.expander("📋 Generated SQL Query", expanded=True):
                            st.code(sql_query, language="sql")
                        
                        if isinstance(data, pd.DataFrame) and not data.empty:
                            st.subheader("📊 Results")
                            st.dataframe(data, use_container_width=True)
                            
                            # Show basic statistics if numeric data
                            numeric_cols = data.select_dtypes(include=['number']).columns
                            if len(numeric_cols) > 0:
                                with st.expander("📈 Quick Statistics"):
                                    st.write(data[numeric_cols].describe())
                        else:
                            st.info("No data returned for this query.")
                    
                    else:
                        # Handle general question response
                        st.markdown(response_text)
                        
                        # Log general response
                        st.session_state.memory_manager.add_message(
                            st.session_state.session_id,
                            'assistant',
                            response_text,
                            execution_status='success'
                        )
                
                else:
                    error_msg = result.get('error', 'Unknown error occurred')
                    st.error(f"❌ Error: {error_msg}")
                    
                    # Log error
                    st.session_state.memory_manager.add_message(
                        st.session_state.session_id,
                        'assistant',
                        f"Error: {error_msg}",
                        sql_query=result.get('sql_query'),
                        execution_status='error',
                        semantic_model_version='custom' if st.session_state.semantic_model_uploaded else 'auto'
                    )
                    
                    # Log performance for failed query
                    st.session_state.memory_manager.log_query_performance(
                        st.session_state.session_id,
                        user_question,
                        result.get('sql_query', ''),
                        execution_time,
                        0,
                        st.session_state.semantic_model_uploaded,
                        False
                    )
            
            except Exception as e:
                error_msg = f"An error occurred while processing your question: {str(e)}"
                st.error(f"❌ {error_msg}")
                
                # Log system error
                st.session_state.memory_manager.add_message(
                    st.session_state.session_id,
                    'assistant',
                    error_msg,
                    execution_status='error'
                )
        
        st.rerun()

def main():
    """Main application function"""
    # Initialize session state
    initialize_session_state()
    
    # App title and description
    st.title("❄️ Snowflake Cortex Analyst Chatbot")
    st.markdown("---")
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["🔐 Authentication", "📋 Semantic Model", "🤖 Chatbot"])
    
    with tab1:
        authentication_tab()
    
    with tab2:
        semantic_model_tab()
    
    with tab3:
        chatbot_tab()
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "Powered by Snowflake Cortex Analyst | Built with Streamlit"
        "</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
