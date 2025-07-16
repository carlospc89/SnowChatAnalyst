# Snowflake Cortex Analyst Chatbot

## Overview

This is a Streamlit-based web application that provides a conversational interface to Snowflake data using Cortex Analyst. The application allows users to authenticate with Snowflake, connect to their databases, and ask natural language questions that are converted to SQL queries through Snowflake's Cortex Analyst functionality.

## User Preferences

Preferred communication style: Simple, everyday language.
Authentication method: External browser authentication for Snowflake connections (SSO).
UI Layout: Sidebar-based chat interface with settings panel and main chat area (similar to modern AI chat applications).
Database Context: Always include database and schema information in SQL queries for proper table referencing.
Data Source Configuration: Support for multiple data sources (Model knowledge via Cortex LLMs, Web search via Tavily, Semantic model data)
Time Budget Selection: LLM model selection based on response speed preference (low/med/high → different models)

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

### 5. Query Router (`query_router.py`)
- **Purpose**: Dynamic query classification using Snowflake Cortex for intelligent routing
- **Key Features**:
  - AI-powered query classification (data queries vs conversational)
  - Confidence scoring and reasoning for classifications
  - Fallback heuristics for classification failures
  - Context-aware routing decisions based on semantic model availability

### 6. Response Generator (`response_generator.py`)
- **Purpose**: Dynamic response generation using Cortex for personalized, context-aware responses
- **Key Features**:
  - Cortex-powered greeting and help responses
  - Context-aware personalization based on user session
  - Dynamic content generation instead of hard-coded templates
  - Fallback responses for system failures

## Data Flow

1. **Authentication**: User provides Snowflake credentials through external browser authentication
2. **Connection**: SnowflakeClient establishes secure connection to Snowflake instance
3. **Semantic Model Setup**: User can either upload custom YAML semantic model or use automatic schema discovery
4. **Dynamic Query Classification**: AI-powered routing using Cortex Analyst to classify queries into types (data queries, greetings, help requests, general questions)
5. **Intelligent Response Generation**: Context-aware response generation using Cortex for personalized, dynamic responses instead of hard-coded templates
6. **Query Processing**: Data questions are processed through Cortex Analyst with appropriate warnings if no semantic model is present
7. **Result Display**: SQL results are returned and displayed in the Streamlit interface with performance metrics and classification insights
8. **Chat History**: All conversations are stored in in-memory SQLite database with session tracking and routing analytics

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

## Recent Changes (July 16, 2025)

### Major Architecture Overhaul - LangChain Integration
- **Change**: Complete rewrite of the application to integrate LangChain framework with Snowflake Cortex
- **Implementation**: 
  - Created `langchain_snowflake_agent.py` with LangChain agent architecture
  - Implemented `SnowflakeCortexLLM` as LangChain-compatible wrapper
  - Built specialized tools: `SnowflakeQueryTool`, `SnowflakeSchemaTool`, `SnowflakeAnalystTool`, `WebSearchTool`
  - Added `LangChainSnowflakeAgent` with conversational memory and tool orchestration
  - Created new `app_langchain.py` as the main application interface
- **Benefits**: 
  - Advanced AI agency with tool selection and reasoning
  - Better memory management with conversation history
  - Improved error handling and tool orchestration
  - More robust query classification and intent understanding
  - Enhanced debugging capabilities with agent metadata
- **User Impact**: 
  - More intelligent and context-aware responses
  - Better tool selection based on query type
  - Improved conversation flow with memory retention
  - Enhanced troubleshooting with agent reasoning visibility

### Enhanced Tool Architecture
- **Change**: Replaced monolithic components with specialized LangChain tools
- **Implementation**: 
  - Direct SQL execution tool for complex queries
  - Schema exploration tool for database discovery
  - Cortex Analyst tool for natural language to SQL conversion
  - Web search tool for current information retrieval
- **Benefits**: Modular, maintainable, and extensible tool system
- **User Impact**: More accurate tool selection and better query handling

### Advanced Memory Management
- **Change**: Implemented LangChain conversation memory with buffer windowing
- **Implementation**: `ConversationBufferWindowMemory` with configurable message retention
- **Benefits**: Maintains conversation context while preventing memory overflow
- **User Impact**: More coherent multi-turn conversations with better context retention

### Improved Agent Configuration
- **Change**: Added comprehensive agent configuration options
- **Implementation**: Tool toggles, model selection, memory management controls
- **Benefits**: Users can customize agent behavior based on their needs
- **User Impact**: Better control over agent capabilities and performance

### Previous Improvements (Legacy System)
- UI Layout Redesign with sidebar-based chat interface
- Enhanced SQL Context with database/schema inclusion
- Error Handling Improvements with SQL query display
- Smart Data Source Integration with configurable routing
- Dynamic Model Selection based on time budget
- Web Search Integration via Tavily API

## Architecture Decisions Rationale

### Streamlit Choice
- **Problem**: Need for rapid development of data application with minimal frontend complexity
- **Solution**: Streamlit provides built-in widgets, state management, and data visualization
- **Pros**: Fast development, built-in data handling, easy deployment
- **Cons**: Limited customization options, single-page application constraints

### Sidebar-Based Chat Interface
- **Problem**: Need for modern, organized chat interface with easy access to settings
- **Solution**: Streamlit sidebar for settings and controls, main area for chat conversation
- **Pros**: Familiar chat application layout, organized settings panel, persistent chat history access
- **Cons**: Reduced main content width, potential mobile responsiveness issues

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