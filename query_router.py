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
                parsed_result = self._parse_classification_result(classification_text)
                print(f"DEBUG: Cortex classification successful: {parsed_result}")
                return parsed_result
            else:
                print("DEBUG: Cortex query returned empty result, using fallback")
                return self._fallback_classification(question)
            
        except Exception as e:
            print(f"Error in query classification: {str(e)}")
            print(f"DEBUG: Falling back to heuristic classification for: {question}")
            # Fallback to simple heuristics
            return self._fallback_classification(question)
    
    def _create_classification_prompt(self, question: str, has_semantic_model: bool) -> str:
        """
        Create enhanced classification prompt for Cortex with better AI agency
        
        Args:
            question: User's question
            has_semantic_model: Whether semantic model is available
            
        Returns:
            str: Formatted classification prompt
        """
        prompt = f"""
You are an expert AI assistant specialized in understanding user intent for a Snowflake data analytics chatbot. 
Your job is to deeply analyze user questions and classify them with high accuracy.

CRITICAL INSTRUCTIONS:
- Analyze the user's ACTUAL INTENT, not just keywords
- Look for subtle data-related patterns and business contexts
- Consider variations in how users might phrase data questions
- Default to DATA_QUERY when there's any possibility of data analysis intent

CONTEXT:
- This is a Snowflake Cortex Analyst chatbot for data analysis
- Semantic model available: {has_semantic_model}
- Users want to analyze business data, get insights, and generate reports
- Most questions are likely about data analysis

CLASSIFICATION CATEGORIES (in order of priority):

1. DATA_QUERY - HIGH PRIORITY - Questions requiring SQL generation and data analysis
   Key indicators:
   - Business metrics: sales, revenue, customers, orders, performance, growth
   - Time-based analysis: trends, periods, comparisons, "last month", "this year"
   - Aggregations: count, sum, average, total, maximum, minimum
   - Comparisons: vs, compared to, difference, change, increase, decrease
   - Data exploration: show, display, get, find, analyze, breakdown
   - Business entities: customers, products, regions, departments, campaigns
   - Performance questions: top, bottom, best, worst, highest, lowest
   - Quantitative terms: how many, how much, what percentage, rate
   
   Examples that are DATA_QUERY:
   - "Show me sales data"
   - "How many customers do we have?"
   - "What's our revenue this quarter?"
   - "Top performing products"
   - "Customer analysis"
   - "Sales by region"
   - "Monthly trends"
   - "Performance metrics"

2. GENERAL_QUESTION - Questions about SQL, databases, or technical concepts
   Clear indicators:
   - SQL syntax: "how to write JOIN", "what is WHERE clause"
   - Database concepts: "explain indexes", "what is normalization"
   - Technical definitions: "what is a primary key", "how does GROUP BY work"
   - Learning questions: "teach me", "explain", "how does X work"

3. GREETING - Simple social interactions
   - "Hello", "Hi", "Good morning", "How are you"
   - Must be purely social, no data intent

4. HELP_REQUEST - Questions about chatbot capabilities
   - "What can you do", "How does this work", "Help me get started"
   - "What data can you analyze", "What are your features"

5. UNCLEAR - Only for truly ambiguous questions
   - Single words without context
   - Extremely vague or nonsensical questions

ANALYSIS FRAMEWORK:
1. First, identify any business/data keywords or context
2. Look for quantitative language or measurement words
3. Consider if this could be answered with database data
4. If there's ANY possibility of data analysis, classify as DATA_QUERY
5. Be generous with DATA_QUERY classification - it's better to attempt SQL generation than miss a data question

USER QUESTION: "{question}"

Analyze the question deeply and respond with a JSON object:
{{
    "type": "one of: DATA_QUERY, GENERAL_QUESTION, GREETING, HELP_REQUEST, UNCLEAR",
    "confidence": "float between 0.0 and 1.0",
    "reasoning": "detailed explanation of why you chose this classification",
    "requires_sql": "boolean - true if SQL generation needed",
    "suggested_response_type": "one of: sql_generation, conversational, greeting, help, clarification",
    "data_keywords": "list of business/data-related keywords found",
    "intent_analysis": "deep analysis of user's likely intent"
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
                result.setdefault('data_keywords', [])
                result.setdefault('intent_analysis', 'No intent analysis provided')
                
                return result
            
        except Exception as e:
            print(f"Error parsing classification result: {str(e)}")
        
        # Fallback if parsing fails
        print(f"DEBUG: Parsing failed, using fallback for original question: {question}")
        return self._fallback_classification(question)
    
    def _fallback_classification(self, question: str) -> Dict[str, Any]:
        """
        Enhanced fallback classification using comprehensive heuristics
        
        Args:
            question: User's question
            
        Returns:
            dict: Classification result
        """
        question_lower = question.lower().strip()
        
        # Simple greeting detection
        greetings = ['hello', 'hi', 'hey', 'good morning', 'good afternoon', 'good evening', 'how are you']
        if any(greeting in question_lower for greeting in greetings) and len(question_lower.split()) <= 5:
            return {
                'type': QueryType.GREETING,
                'confidence': 0.9,
                'reasoning': 'Contains greeting words',
                'requires_sql': False,
                'suggested_response_type': 'greeting',
                'data_keywords': [],
                'intent_analysis': 'Social greeting'
            }
        
        # Help request detection
        help_indicators = ['what can you do', 'help', 'capabilities', 'how does this work', 'what are your features']
        if any(indicator in question_lower for indicator in help_indicators):
            return {
                'type': QueryType.HELP_REQUEST,
                'confidence': 0.8,
                'reasoning': 'Contains help request indicators',
                'requires_sql': False,
                'suggested_response_type': 'help',
                'data_keywords': [],
                'intent_analysis': 'Request for chatbot capabilities'
            }
        
        # Enhanced data query detection - be more aggressive
        data_indicators = [
            # Quantitative words
            'how many', 'how much', 'count', 'total', 'sum', 'average', 'avg', 'maximum', 'minimum',
            'top', 'bottom', 'highest', 'lowest', 'best', 'worst', 'most', 'least',
            
            # Business terms
            'sales', 'revenue', 'customers', 'orders', 'products', 'users', 'performance',
            'profit', 'cost', 'price', 'value', 'growth', 'trends', 'metrics', 'kpi',
            
            # Action words
            'show', 'display', 'get', 'find', 'analyze', 'breakdown', 'compare', 'list',
            'report', 'view', 'see', 'give me', 'tell me about',
            
            # Time-related
            'last month', 'this year', 'quarterly', 'monthly', 'daily', 'weekly',
            'yesterday', 'today', 'recent', 'current', 'past', 'previous',
            
            # Data words
            'data', 'table', 'database', 'records', 'rows', 'results',
            
            # SQL-like words
            'select', 'from', 'where', 'group by', 'order by',
            
            # Comparison words
            'vs', 'versus', 'compared to', 'difference', 'change', 'increase', 'decrease'
        ]
        
        found_keywords = [word for word in data_indicators if word in question_lower]
        
        if found_keywords:
            return {
                'type': QueryType.DATA_QUERY,
                'confidence': 0.8,
                'reasoning': f'Contains data query indicators: {found_keywords}',
                'requires_sql': True,
                'suggested_response_type': 'sql_generation',
                'data_keywords': found_keywords,
                'intent_analysis': 'Likely data analysis request based on keywords'
            }
        
        # SQL learning questions
        sql_learning = ['how to', 'what is', 'explain', 'join', 'query', 'primary key', 'foreign key']
        if any(term in question_lower for term in sql_learning):
            return {
                'type': QueryType.GENERAL_QUESTION,
                'confidence': 0.7,
                'reasoning': 'Contains SQL learning indicators',
                'requires_sql': False,
                'suggested_response_type': 'conversational',
                'data_keywords': [],
                'intent_analysis': 'Request for SQL/database knowledge'
            }
        
        # Default to DATA_QUERY if uncertain - better to attempt data analysis
        print(f"DEBUG: No specific indicators found, defaulting to DATA_QUERY for: {question}")
        return {
            'type': QueryType.DATA_QUERY,
            'confidence': 0.6,
            'reasoning': 'Default classification - assuming data query intent',
            'requires_sql': True,
            'suggested_response_type': 'sql_generation',
            'data_keywords': [],
            'intent_analysis': 'Uncertain intent - defaulting to data query for better user experience'
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