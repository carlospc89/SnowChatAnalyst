import streamlit as st
import pandas as pd
from snowflake_client import SnowflakeClient
from cortex_analyst import CortexAnalyst
import os
import yaml

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
    
    # Show semantic model status
    if st.session_state.semantic_model_uploaded:
        st.info("📋 Using custom semantic model for enhanced query generation.")
    else:
        st.info("🔍 Using automatic schema discovery. Upload a semantic model for better results.")
    
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
