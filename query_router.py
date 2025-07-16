"""
Dynamic Query Router using Snowflake Cortex for intelligent classification
"""

from typing import Dict, Any, Optional
from enum import Enum
import json

class QueryType(Enum):
    DATA_QUERY = "data_query"
    GENERAL_QUESTION = "general_question"
    GREETING = "greeting"
    HELP_REQUEST = "help_request"
    UNCLEAR = "unclear"

class QueryRouter:
    """
    Dynamic query router using Cortex Analyst for intelligent classification
    """
    
    def __init__(self, snowflake_client):
        """
        Initialize router with Snowflake client
        
        Args:
            snowflake_client: SnowflakeClient instance
        """
        self.client = snowflake_client
        
    def classify_query(self, question: str, has_semantic_model: bool = False) -> Dict[str, Any]:
        """
        Classify user query using Cortex Analyst
        
        Args:
            question: User's question
            has_semantic_model: Whether semantic model is available
            
        Returns:
            dict: Classification result with type, confidence, and reasoning
        """
        try:
            classification_prompt = self._create_classification_prompt(question, has_semantic_model)
            
            # Use Cortex Complete for classification
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-8b',
                '{classification_prompt.replace("'", "''")}'
            ) as classification_result
            """
            
            result = self.client.execute_query(cortex_query)
            
            if result is not None and not result.empty:
                classification_text = result.iloc[0]['CLASSIFICATION_RESULT']
                return self._parse_classification_result(classification_text)
            
            # Fallback classification
            return {
                'type': QueryType.UNCLEAR,
                'confidence': 0.5,
                'reasoning': 'Could not classify query',
                'requires_sql': False,
                'suggested_response_type': 'general'
            }
            
        except Exception as e:
            print(f"Error in query classification: {str(e)}")
            # Fallback to simple heuristics
            return self._fallback_classification(question)
    
    def _create_classification_prompt(self, question: str, has_semantic_model: bool) -> str:
        """
        Create classification prompt for Cortex
        
        Args:
            question: User's question
            has_semantic_model: Whether semantic model is available
            
        Returns:
            str: Formatted classification prompt
        """
        prompt = f"""
You are an intelligent query classifier for a Snowflake data analytics chatbot. 
Your task is to classify user queries into specific categories to route them appropriately.

CONTEXT:
- This is a Snowflake Cortex Analyst chatbot
- Semantic model available: {has_semantic_model}
- User can ask about data or general questions

CLASSIFICATION CATEGORIES:
1. DATA_QUERY: Questions requiring SQL generation and data analysis
   - Examples: "Show me sales by region", "How many customers last month", "What's the average order value"
   - Indicators: Aggregations, comparisons, data exploration, table/column references

2. GENERAL_QUESTION: Questions about SQL, databases, or technical concepts
   - Examples: "How do I write a JOIN query", "What is a primary key", "Explain window functions"
   - Indicators: Technical explanations, how-to questions, concept definitions

3. GREETING: Simple greetings and conversation starters
   - Examples: "Hello", "Hi", "Good morning", "How are you"
   - Indicators: Social pleasantries, conversation openings

4. HELP_REQUEST: Questions about chatbot capabilities or usage
   - Examples: "What can you do", "How does this work", "Help me get started"
   - Indicators: Meta questions about the system itself

5. UNCLEAR: Ambiguous or unclear questions
   - Examples: Very short, unclear, or mixed-intent questions

USER QUESTION: "{question}"

Respond with a JSON object containing:
{{
    "type": "one of: DATA_QUERY, GENERAL_QUESTION, GREETING, HELP_REQUEST, UNCLEAR",
    "confidence": "float between 0.0 and 1.0",
    "reasoning": "brief explanation for the classification",
    "requires_sql": "boolean - true if SQL generation needed",
    "suggested_response_type": "one of: sql_generation, conversational, greeting, help, clarification"
}}

Respond with ONLY the JSON object, no additional text.
"""
        return prompt
    
    def _parse_classification_result(self, classification_text: str) -> Dict[str, Any]:
        """
        Parse classification result from Cortex response
        
        Args:
            classification_text: Raw response from Cortex
            
        Returns:
            dict: Parsed classification result
        """
        try:
            # Clean the response to extract JSON
            cleaned_text = classification_text.strip()
            
            # Find JSON object in the response
            start_idx = cleaned_text.find('{')
            end_idx = cleaned_text.rfind('}') + 1
            
            if start_idx != -1 and end_idx != -1:
                json_str = cleaned_text[start_idx:end_idx]
                result = json.loads(json_str)
                
                # Convert string type to enum
                query_type_str = result.get('type', 'UNCLEAR')
                try:
                    result['type'] = QueryType(query_type_str.lower())
                except ValueError:
                    result['type'] = QueryType.UNCLEAR
                
                # Ensure all required fields exist
                result.setdefault('confidence', 0.7)
                result.setdefault('reasoning', 'Classified by Cortex')
                result.setdefault('requires_sql', result['type'] == QueryType.DATA_QUERY)
                result.setdefault('suggested_response_type', 'conversational')
                
                return result
            
        except Exception as e:
            print(f"Error parsing classification result: {str(e)}")
        
        # Fallback if parsing fails
        return self._fallback_classification(classification_text)
    
    def _fallback_classification(self, question: str) -> Dict[str, Any]:
        """
        Fallback classification using simple heuristics
        
        Args:
            question: User's question
            
        Returns:
            dict: Classification result
        """
        question_lower = question.lower().strip()
        
        # Simple greeting detection
        greetings = ['hello', 'hi', 'hey', 'good morning', 'good afternoon', 'good evening']
        if any(greeting in question_lower for greeting in greetings):
            return {
                'type': QueryType.GREETING,
                'confidence': 0.9,
                'reasoning': 'Contains greeting words',
                'requires_sql': False,
                'suggested_response_type': 'greeting'
            }
        
        # Help request detection
        help_indicators = ['what can you do', 'help', 'capabilities', 'how does this work']
        if any(indicator in question_lower for indicator in help_indicators):
            return {
                'type': QueryType.HELP_REQUEST,
                'confidence': 0.8,
                'reasoning': 'Contains help request indicators',
                'requires_sql': False,
                'suggested_response_type': 'help'
            }
        
        # Data query detection
        data_indicators = ['select', 'show me', 'how many', 'total', 'average', 'count', 'sum', 'data', 'table']
        if any(indicator in question_lower for indicator in data_indicators):
            return {
                'type': QueryType.DATA_QUERY,
                'confidence': 0.7,
                'reasoning': 'Contains data query indicators',
                'requires_sql': True,
                'suggested_response_type': 'sql_generation'
            }
        
        # Default to general question
        return {
            'type': QueryType.GENERAL_QUESTION,
            'confidence': 0.6,
            'reasoning': 'Default classification - appears to be general question',
            'requires_sql': False,
            'suggested_response_type': 'conversational'
        }
    
    def get_response_strategy(self, classification: Dict[str, Any], has_semantic_model: bool) -> Dict[str, Any]:
        """
        Get response strategy based on classification
        
        Args:
            classification: Classification result
            has_semantic_model: Whether semantic model is available
            
        Returns:
            dict: Response strategy with handler and context
        """
        query_type = classification['type']
        
        if query_type == QueryType.DATA_QUERY:
            return {
                'handler': 'cortex_analyst',
                'show_warning': not has_semantic_model,
                'warning_message': ("⚠️ **Limited Accuracy Warning**: You're asking about data but no semantic model is uploaded. "
                                  "The response may contain inaccuracies. For better results, please upload a semantic model first.") if not has_semantic_model else None,
                'context': 'data_analysis'
            }
        
        elif query_type == QueryType.GREETING:
            return {
                'handler': 'greeting_response',
                'template': 'dynamic_greeting',
                'context': 'conversation_start'
            }
        
        elif query_type == QueryType.HELP_REQUEST:
            return {
                'handler': 'help_response',
                'template': 'capabilities_overview',
                'context': 'system_help'
            }
        
        else:  # GENERAL_QUESTION or UNCLEAR
            return {
                'handler': 'general_response',
                'template': 'conversational',
                'context': 'general_assistance'
            }