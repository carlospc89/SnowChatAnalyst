# Snowflake Cortex Analyst Chatbot

## Overview

This is a Streamlit-based web application that provides a conversational interface to Snowflake data using Cortex Analyst. The application allows users to authenticate with Snowflake, connect to their databases, and ask natural language questions that are converted to SQL queries through Snowflake's Cortex Analyst functionality.

## User Preferences

Preferred communication style: Simple, everyday language.
Authentication method: External browser authentication for Snowflake connections (SSO).

## System Architecture

### Frontend Architecture
- **Framework**: Streamlit - chosen for rapid development of data applications with minimal frontend complexity
- **Layout**: Wide layout configuration optimized for data visualization and chat interfaces
- **State Management**: Streamlit's session state for maintaining user authentication, connections, and chat history across interactions

### Backend Architecture
- **Modular Design**: Three main components separated into distinct modules:
  - `app.py`: Main Streamlit application and UI logic
  - `snowflake_client.py`: Database connection and query execution
  - `cortex_analyst.py`: Natural language processing and semantic modeling
- **Connection Management**: Persistent Snowflake connections with keep-alive functionality
- **Error Handling**: Graceful error handling for connection failures and query issues

## Key Components

### 1. Streamlit Application (`app.py`)
- **Purpose**: Main user interface and application orchestration
- **Key Features**:
  - Page configuration and session state initialization
  - Authentication workflow management
  - Connection status tracking
  - Chat history persistence

### 2. Snowflake Client (`snowflake_client.py`)
- **Purpose**: Database connectivity and query execution
- **Key Features**:
  - Secure connection management with credentials
  - Query execution with pandas DataFrame results
  - Table and schema introspection capabilities
  - Connection persistence with keep-alive

### 3. Cortex Analyst (`cortex_analyst.py`)
- **Purpose**: Natural language to SQL conversion using Snowflake Cortex
- **Key Features**:
  - Semantic model generation from database schema
  - Context-aware prompt creation for better query results
  - Integration with Snowflake's AI capabilities
  - Support for custom YAML semantic models

### 4. Memory Manager (`memory_manager.py`)
- **Purpose**: In-memory SQLite database for session and chat history management
- **Key Features**:
  - Persistent chat history with performance tracking
  - Session statistics and semantic model usage tracking
  - Query performance logging and analysis

## Data Flow

1. **Authentication**: User provides Snowflake credentials through external browser authentication
2. **Connection**: SnowflakeClient establishes secure connection to Snowflake instance
3. **Semantic Model Setup**: User can either upload custom YAML semantic model or use automatic schema discovery
4. **Query Classification**: System intelligently distinguishes between data queries (requiring SQL) and general questions (conversational)
5. **Query Processing**: Data questions are processed through Cortex Analyst with appropriate warnings if no semantic model is present
6. **Result Display**: SQL results are returned and displayed in the Streamlit interface with performance metrics
7. **Chat History**: All conversations are stored in in-memory SQLite database with session tracking

## External Dependencies

### Core Dependencies
- **streamlit**: Web application framework for the user interface
- **pandas**: Data manipulation and analysis for query results
- **snowflake-connector-python**: Official Snowflake database connector

### Snowflake Services
- **Snowflake Cortex Analyst**: AI-powered natural language to SQL conversion
- **Snowflake Data Cloud**: Primary data storage and compute platform

## Deployment Strategy

### Development Environment
- **Platform**: Designed for Replit deployment with Python runtime
- **Configuration**: Environment variables for sensitive credentials
- **Session Management**: Streamlit session state for user experience continuity

### Security Considerations
- External browser authentication eliminates password handling in the application
- SSO integration provides enterprise-grade security
- No hardcoded sensitive information in source code
- Connection credentials stored only in session state (not persisted)
- Browser-based authentication supports multi-factor authentication and federated identity

### Scalability Design
- Stateless application design allows for easy horizontal scaling
- Individual user sessions maintain their own Snowflake connections
- Modular architecture supports easy feature additions and modifications

## Architecture Decisions Rationale

### Streamlit Choice
- **Problem**: Need for rapid development of data application with minimal frontend complexity
- **Solution**: Streamlit provides built-in widgets, state management, and data visualization
- **Pros**: Fast development, built-in data handling, easy deployment
- **Cons**: Limited customization options, single-page application constraints

### Modular Component Design
- **Problem**: Separation of concerns between UI, database access, and AI functionality
- **Solution**: Three distinct modules with clear responsibilities
- **Pros**: Maintainable code, testable components, reusable modules
- **Cons**: Slightly more complex initial setup

### Session State Management
- **Problem**: Maintaining user context across Streamlit reruns
- **Solution**: Comprehensive session state for authentication, connections, and chat history
- **Pros**: Seamless user experience, persistent connections
- **Cons**: Memory usage scales with concurrent users