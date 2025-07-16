import streamlit as st
import pandas as pd
from snowflake_client import SnowflakeClient
from cortex_analyst import CortexAnalyst
from memory_manager import MemoryManager
from query_router import QueryRouter, QueryType
from response_generator import ResponseGenerator
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
    if 'query_router' not in st.session_state:
        st.session_state.query_router = None
    if 'response_generator' not in st.session_state:
        st.session_state.response_generator = None

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
                    st.session_state.query_router = QueryRouter(client)
                    st.session_state.response_generator = ResponseGenerator(client)
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
    
    # Display chat history but not if we just processed a question
    if 'last_result' not in st.session_state:
        st.session_state.last_result = None
    
    # Only show chat history if we're not displaying a fresh result
    if not st.session_state.get('showing_fresh_result', False):
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
        # Set flag to show fresh result instead of chat history
        st.session_state.showing_fresh_result = True
        
        # Log user message
        st.session_state.memory_manager.add_message(
            st.session_state.session_id,
            'user',
            user_question
        )
        
        with st.spinner("Analyzing your question..."):
            start_time = time.time()
            
            try:
                # Step 1: Classify the query using dynamic routing
                classification = st.session_state.query_router.classify_query(
                    user_question, 
                    st.session_state.semantic_model_uploaded
                )
                
                # Step 2: Get user context for personalized responses
                user_context = {
                    'session_stats': st.session_state.memory_manager.get_session_stats(st.session_state.session_id),
                    'has_semantic_model': st.session_state.semantic_model_uploaded,
                    'database': st.session_state.get('database', ''),
                    'schema': st.session_state.get('schema', '')
                }
                
                # Step 3: Route and process based on classification
                if classification['type'] == QueryType.DATA_QUERY:
                    # Show warning if no semantic model
                    if not st.session_state.semantic_model_uploaded:
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
                    result['classification'] = classification
                    
                else:
                    # Generate dynamic response using Cortex
                    result = st.session_state.response_generator.generate_response(
                        user_question, 
                        classification, 
                        st.session_state.semantic_model_uploaded,
                        user_context
                    )
                    result['classification'] = classification
                
                execution_time = int((time.time() - start_time) * 1000)
                
                # Display the user's question first
                with st.chat_message("user"):
                    st.write(user_question)
                
                if result['success']:
                    sql_query = result.get('sql_query')
                    data = result.get('data')
                    response_text = result.get('response')
                    classification = result.get('classification', {})
                    query_type = classification.get('type', QueryType.UNCLEAR)
                    
                    if query_type == QueryType.DATA_QUERY and sql_query:
                        # Handle data query response
                        row_count = len(data) if isinstance(data, pd.DataFrame) else 0
                        
                        with st.chat_message("assistant"):
                            # Display the result first
                            st.success(f"✅ Query executed successfully! Found {row_count} rows.")
                            
                            # Show SQL query
                            with st.expander("📋 Generated SQL Query", expanded=False):
                                st.code(sql_query, language="sql")
                            
                            # Display data results
                            if data is not None:
                                if isinstance(data, pd.DataFrame):
                                    if not data.empty:
                                        st.subheader("📊 Query Results")
                                        st.dataframe(data, use_container_width=True)
                                        
                                        # Show basic statistics if numeric data
                                        numeric_cols = data.select_dtypes(include=['number']).columns
                                        if len(numeric_cols) > 0:
                                            with st.expander("📈 Quick Statistics"):
                                                st.write(data[numeric_cols].describe())
                                    else:
                                        st.info("Query executed successfully but returned no data.")
                                else:
                                    # Handle other data types
                                    st.subheader("📊 Query Results")
                                    st.write(data)
                            else:
                                st.warning("Query executed but no data was returned.")
                        
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
                    
                    else:
                        # Handle non-data responses (greetings, help, general questions)
                        with st.chat_message("assistant"):
                            # Show classification info for debugging (can be removed later)
                            confidence = classification.get('confidence', 0.5)
                            query_type_name = query_type.value if hasattr(query_type, 'value') else str(query_type)
                            
                            with st.expander(f"🔍 Query Classification (Confidence: {confidence:.2f})", expanded=False):
                                st.write(f"**Type**: {query_type_name}")
                                st.write(f"**Reasoning**: {classification.get('reasoning', 'No reasoning provided')}")
                            
                            # Display the response
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
                    failed_sql_query = result.get('sql_query')
                    classification = result.get('classification', {})
                    query_type = classification.get('type', QueryType.UNCLEAR)
                    
                    with st.chat_message("assistant"):
                        st.error(f"❌ Error: {error_msg}")
                        
                        # Always show the generated SQL query if it exists, even on failure
                        if failed_sql_query and query_type == QueryType.DATA_QUERY:
                            with st.expander("📋 Generated SQL Query (Failed)", expanded=True):
                                st.code(failed_sql_query, language="sql")
                                st.info("💡 The SQL query was generated but failed during execution. Check the query syntax and table/column names.")
                                
                                # Add troubleshooting suggestions
                                st.markdown("**Troubleshooting Tips:**")
                                st.markdown("• Verify table names exist in your database/schema")
                                st.markdown("• Check if column names are correct")
                                st.markdown("• Ensure you have proper permissions")
                                st.markdown("• Try uploading a semantic model for better accuracy")
                        
                        elif query_type == QueryType.DATA_QUERY and not failed_sql_query:
                            st.warning("⚠️ No SQL query was generated. This could indicate:")
                            st.markdown("• The question might be too ambiguous")
                            st.markdown("• Database connection issues")
                            st.markdown("• Cortex Analyst service availability")
                        
                        # Show classification info for debugging
                        if classification:
                            confidence = classification.get('confidence', 0.5)
                            query_type_name = query_type.value if hasattr(query_type, 'value') else str(query_type)
                            
                            with st.expander(f"🔍 Query Classification (Confidence: {confidence:.2f})", expanded=False):
                                st.write(f"**Type**: {query_type_name}")
                                st.write(f"**Reasoning**: {classification.get('reasoning', 'No reasoning provided')}")
                                st.write(f"**Original Question**: {user_question}")
                                st.write(f"**Semantic Model**: {'✅ Active' if st.session_state.semantic_model_uploaded else '❌ Not uploaded'}")
                    
                    # Log error
                    st.session_state.memory_manager.add_message(
                        st.session_state.session_id,
                        'assistant',
                        f"Error: {error_msg}",
                        sql_query=failed_sql_query,
                        execution_status='error',
                        semantic_model_version='custom' if st.session_state.semantic_model_uploaded else 'auto'
                    )
                    
                    # Log performance for failed query
                    st.session_state.memory_manager.log_query_performance(
                        st.session_state.session_id,
                        user_question,
                        failed_sql_query or '',
                        execution_time,
                        0,
                        st.session_state.semantic_model_uploaded,
                        False
                    )
            
            except Exception as e:
                error_msg = f"An error occurred while processing your question: {str(e)}"
                
                with st.chat_message("assistant"):
                    st.error(f"❌ {error_msg}")
                    
                    # Try to show any SQL query that might have been generated before the error
                    if 'result' in locals() and isinstance(result, dict):
                        potential_sql = result.get('sql_query')
                        if potential_sql:
                            with st.expander("📋 Partially Generated SQL Query", expanded=True):
                                st.code(potential_sql, language="sql")
                                st.warning("⚠️ This query was generated before the error occurred. It may be incomplete or incorrect.")
                
                # Log system error
                st.session_state.memory_manager.add_message(
                    st.session_state.session_id,
                    'assistant',
                    error_msg,
                    execution_status='error'
                )
        
        # Add button to return to chat history after viewing results
        if st.session_state.get('showing_fresh_result', False):
            if st.button("📜 Back to Chat History"):
                st.session_state.showing_fresh_result = False
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
