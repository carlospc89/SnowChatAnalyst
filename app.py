import streamlit as st
import pandas as pd
from snowflake_client import SnowflakeClient
from cortex_analyst import CortexAnalyst
import os

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

def reset_connection():
    """Reset connection state"""
    st.session_state.authenticated = False
    st.session_state.snowflake_client = None
    st.session_state.cortex_analyst = None
    st.session_state.connection_status = None
    st.session_state.chat_history = []

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
        
        if st.button("🔄 Disconnect", type="secondary"):
            reset_connection()
            st.rerun()
        
        return
    
    st.write("Please enter your Snowflake credentials to connect:")
    
    with st.form("snowflake_auth"):
        col1, col2 = st.columns(2)
        
        with col1:
            account = st.text_input(
                "Account Identifier", 
                help="Your Snowflake account identifier (e.g., abc12345.us-east-1)"
            )
            username = st.text_input("Username")
            warehouse = st.text_input(
                "Warehouse", 
                help="Snowflake warehouse to use for queries"
            )
        
        with col2:
            password = st.text_input("Password", type="password")
            database = st.text_input(
                "Database", 
                help="Database containing your data"
            )
            schema = st.text_input(
                "Schema", 
                value="PUBLIC",
                help="Schema within the database"
            )
        
        submitted = st.form_submit_button("🔗 Connect to Snowflake", type="primary")
    
    if submitted:
        if not all([account, username, password, warehouse, database, schema]):
            st.error("❌ Please fill in all required fields.")
            return
        
        with st.spinner("Connecting to Snowflake..."):
            try:
                # Create Snowflake client
                client = SnowflakeClient(
                    account=account,
                    user=username,
                    password=password,
                    warehouse=warehouse,
                    database=database,
                    schema=schema
                )
                
                # Test connection
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
                    st.session_state.connection_status = "Connected"
                    
                    st.success("✅ Successfully connected to Snowflake!")
                    st.rerun()
                else:
                    st.error("❌ Failed to connect to Snowflake. Please check your credentials.")
            
            except Exception as e:
                st.error(f"❌ Connection error: {str(e)}")

def chatbot_tab():
    """Handle chatbot interface and natural language queries"""
    if not st.session_state.authenticated:
        st.warning("⚠️ Please authenticate with Snowflake first in the Authentication tab.")
        return
    
    st.header("🤖 Cortex Analyst Chatbot")
    st.write("Ask questions about your data in natural language, and I'll convert them to SQL queries using Snowflake Cortex Analyst.")
    
    # Display chat history
    chat_container = st.container()
    
    with chat_container:
        for i, (question, response, sql_query) in enumerate(st.session_state.chat_history):
            with st.expander(f"💬 Query {i+1}: {question[:50]}{'...' if len(question) > 50 else ''}", expanded=False):
                st.write("**Question:**", question)
                if sql_query:
                    st.code(sql_query, language="sql")
                if isinstance(response, pd.DataFrame) and not response.empty:
                    st.dataframe(response, use_container_width=True)
                elif isinstance(response, str):
                    st.error(response)
    
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
        st.session_state.chat_history = []
        st.rerun()
    
    if submit_button and user_question.strip():
        with st.spinner("Processing your question..."):
            try:
                # Use Cortex Analyst to process the question
                result = st.session_state.cortex_analyst.process_question(user_question)
                
                if result['success']:
                    sql_query = result['sql_query']
                    data = result['data']
                    
                    # Add to chat history
                    st.session_state.chat_history.append((user_question, data, sql_query))
                    
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
                    error_msg = result.get('error', 'Unknown error occurred')
                    st.error(f"❌ Error: {error_msg}")
                    st.session_state.chat_history.append((user_question, error_msg, None))
            
            except Exception as e:
                error_msg = f"An error occurred while processing your question: {str(e)}"
                st.error(f"❌ {error_msg}")
                st.session_state.chat_history.append((user_question, error_msg, None))
        
        st.rerun()

def main():
    """Main application function"""
    # Initialize session state
    initialize_session_state()
    
    # App title and description
    st.title("❄️ Snowflake Cortex Analyst Chatbot")
    st.markdown("---")
    
    # Create tabs
    tab1, tab2 = st.tabs(["🔐 Authentication", "🤖 Chatbot"])
    
    with tab1:
        authentication_tab()
    
    with tab2:
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
