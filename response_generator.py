"""
Dynamic Response Generator using Snowflake Cortex for intelligent responses
"""

from typing import Dict, Any, Optional
from query_router import QueryType

class ResponseGenerator:
    """
    Dynamic response generator using Cortex for intelligent, context-aware responses
    """
    
    def __init__(self, snowflake_client):
        """
        Initialize response generator with Snowflake client
        
        Args:
            snowflake_client: SnowflakeClient instance
        """
        self.client = snowflake_client
    
    def generate_response(self, question: str, classification: Dict[str, Any], 
                         has_semantic_model: bool = False, 
                         user_context: Dict[str, Any] = None,
                         model: str = 'llama3.1-8b',
                         web_search_context: str = None) -> Dict[str, Any]:
        """
        Generate dynamic response based on classification
        
        Args:
            question: Original user question
            classification: Query classification result
            has_semantic_model: Whether semantic model is available
            user_context: Additional context about user session
            
        Returns:
            dict: Response with content, type, and metadata
        """
        query_type = classification['type']
        
        if query_type == QueryType.GREETING:
            return self._generate_greeting_response(question, has_semantic_model, user_context, model, web_search_context)
        
        elif query_type == QueryType.HELP_REQUEST:
            return self._generate_help_response(question, has_semantic_model, user_context, model, web_search_context)
        
        elif query_type == QueryType.GENERAL_QUESTION:
            return self._generate_general_response(question, user_context, model, web_search_context)
        
        else:  # UNCLEAR or fallback
            return self._generate_clarification_response(question, classification, user_context, model)
    
    def _generate_greeting_response(self, question: str, has_semantic_model: bool, 
                                  user_context: Dict[str, Any] = None, model: str = 'llama3.1-8b',
                                  web_search_context: str = None) -> Dict[str, Any]:
        """
        Generate personalized greeting response using Cortex
        
        Args:
            question: Original greeting
            has_semantic_model: Whether semantic model is available
            user_context: User session context
            
        Returns:
            dict: Greeting response
        """
        try:
            # Create context-aware greeting prompt
            context_info = ""
            if user_context:
                session_stats = user_context.get('session_stats', {})
                query_count = session_stats.get('user_messages', 0)
                
                if query_count > 0:
                    context_info = f"The user has asked {query_count} questions in this session. "
            
            semantic_status = "Custom semantic model is loaded and ready for data queries." if has_semantic_model else "No semantic model is currently loaded."
            
            prompt = f"""
You are a friendly and helpful Snowflake Cortex Analyst assistant. Generate a warm, personalized greeting response.

CONTEXT:
- User greeting: "{question}"
- Semantic model status: {semantic_status}
- Session context: {context_info}

GUIDELINES:
- Be warm and welcoming
- Briefly mention your capabilities (data analysis with Snowflake)
- Reference the semantic model status naturally
- Keep it concise but informative
- Sound natural and conversational
- Don't use technical jargon

Generate a friendly greeting response in 2-3 sentences.
"""
            
            response_text = self._call_cortex_complete(prompt, model)
            
            if response_text:
                return {
                    'success': True,
                    'response': response_text,
                    'type': 'greeting',
                    'requires_sql': False
                }
        
        except Exception as e:
            print(f"Error generating greeting response: {str(e)}")
        
        # Fallback greeting
        status_msg = "I have your semantic model loaded and ready for accurate data queries!" if has_semantic_model else "I'm ready to help, though uploading a semantic model would improve data query accuracy."
        
        return {
            'success': True,
            'response': f"Hello! I'm your Snowflake Cortex Analyst assistant. {status_msg} I can help you analyze data with natural language queries or answer questions about SQL and databases. What would you like to explore?",
            'type': 'greeting',
            'requires_sql': False
        }
    
    def _generate_help_response(self, question: str, has_semantic_model: bool, 
                               user_context: Dict[str, Any] = None, model: str = 'llama3.1-8b',
                               web_search_context: str = None) -> Dict[str, Any]:
        """
        Generate dynamic help response using Cortex
        
        Args:
            question: Original help request
            has_semantic_model: Whether semantic model is available
            user_context: User session context
            
        Returns:
            dict: Help response
        """
        try:
            semantic_status = "active with custom semantic model" if has_semantic_model else "active without semantic model"
            
            prompt = f"""
You are a Snowflake Cortex Analyst assistant. Generate a helpful response about your capabilities.

USER REQUEST: "{question}"
CURRENT STATUS: {semantic_status}

CAPABILITIES TO MENTION:
1. Data Analysis: Convert natural language to SQL queries and execute them
2. SQL Assistance: Help with SQL concepts, syntax, and best practices  
3. Database Support: Explain Snowflake features and database concepts
4. Semantic Models: Enhanced accuracy when custom semantic models are uploaded

CURRENT LIMITATIONS:
- {'Data queries will have enhanced accuracy due to semantic model' if has_semantic_model else 'Data queries may be less accurate without a semantic model'}

Generate a helpful, structured response that explains capabilities clearly and addresses their specific question. Keep it practical and actionable.
"""
            
            response_text = self._call_cortex_complete(prompt, model)
            
            if response_text:
                return {
                    'success': True,
                    'response': response_text,
                    'type': 'help',
                    'requires_sql': False
                }
        
        except Exception as e:
            print(f"Error generating help response: {str(e)}")
        
        # Fallback help response
        semantic_info = "✅ Custom semantic model loaded - ready for accurate data queries!" if has_semantic_model else "⚠️ No semantic model uploaded - data queries may be less accurate"
        
        return {
            'success': True,
            'response': f"""I'm your Snowflake Cortex Analyst assistant! Here's what I can help you with:

**🔍 Data Analysis**: Ask questions about your data in natural language, and I'll convert them to SQL queries and show you the results.

**💡 SQL Help**: Get assistance with SQL syntax, query optimization, and database concepts.

**🔧 Technical Support**: Learn about Snowflake features, best practices, and data analysis techniques.

**Current Status**: {semantic_info}

What would you like to explore? You can ask me anything from "Show me sales by region" to "How do I write a JOIN query".""",
            'type': 'help',
            'requires_sql': False
        }
    
    def _generate_general_response(self, question: str, user_context: Dict[str, Any] = None, 
                                 model: str = 'llama3.1-8b', web_search_context: str = None) -> Dict[str, Any]:
        """
        Generate response for general questions using Cortex
        
        Args:
            question: Original question
            user_context: User session context
            
        Returns:
            dict: General response
        """
        try:
            # Include web search context if available
            search_context = ""
            if web_search_context:
                search_context = f"\n\nCURRENT WEB INFORMATION:\n{web_search_context}\n"
            
            prompt = f"""
You are a knowledgeable Snowflake Cortex Analyst assistant. Answer the user's question about SQL, databases, or data analysis concepts.

USER QUESTION: "{question}"{search_context}

GUIDELINES:
- Provide accurate, helpful information
- Focus on practical examples when possible
- Keep explanations clear and accessible
- If it's about SQL, include simple examples
- If it's about Snowflake, mention relevant features
- Be concise but thorough
- Don't generate actual SQL queries - this is for conceptual help
- If web search information is provided, incorporate relevant current information

Provide a helpful, informative response.
"""
            
            response_text = self._call_cortex_complete(prompt, model)
            
            if response_text:
                return {
                    'success': True,
                    'response': response_text,
                    'type': 'general',
                    'requires_sql': False
                }
        
        except Exception as e:
            print(f"Error generating general response: {str(e)}")
        
        # Fallback general response
        return {
            'success': True,
            'response': f"""I understand you're asking about "{question}". While I'm specialized in helping with Snowflake data analysis, I can also assist with:

• SQL query writing and optimization
• Database concepts and terminology  
• Snowflake features and best practices
• Data analysis methodologies

If you have specific questions about your data, I can help generate SQL queries to find answers. For the most accurate results with data queries, consider uploading a semantic model first.

Could you provide more details about what you'd like to know?""",
            'type': 'general',
            'requires_sql': False
        }
    
    def _generate_clarification_response(self, question: str, classification: Dict[str, Any], 
                                       user_context: Dict[str, Any] = None, model: str = 'llama3.1-8b') -> Dict[str, Any]:
        """
        Generate clarification request for unclear questions
        
        Args:
            question: Original unclear question
            classification: Classification result
            user_context: User session context
            
        Returns:
            dict: Clarification response
        """
        confidence = classification.get('confidence', 0.5)
        reasoning = classification.get('reasoning', 'unclear intent')
        
        return {
            'success': True,
            'response': f"""I want to help you with "{question}", but I'm not quite sure what you're looking for.

Could you clarify if you want to:
• **Analyze data** - Ask questions about your data that I can convert to SQL queries
• **Learn about SQL** - Get help with database concepts, syntax, or best practices  
• **Get system help** - Understand my capabilities or how to use this tool

For example:
- "Show me sales data by region" (data analysis)
- "How do I write a JOIN query?" (SQL help)
- "What can you help me with?" (system help)

What specifically would you like assistance with?""",
            'type': 'clarification',
            'requires_sql': False
        }
    
    def _call_cortex_complete(self, prompt: str, model: str = 'llama3.1-8b') -> Optional[str]:
        """
        Call Snowflake Cortex Complete function
        
        Args:
            prompt: Prompt for Cortex
            model: LLM model to use
            
        Returns:
            str: Generated response or None if error
        """
        try:
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                '{prompt.replace("'", "''")}'
            ) as generated_response
            """
            
            result = self.client.execute_query(cortex_query)
            
            if result is not None and not result.empty:
                return result.iloc[0]['GENERATED_RESPONSE']
            
            return None
            
        except Exception as e:
            print(f"Error calling Cortex Complete: {str(e)}")
            return None