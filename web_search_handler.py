"""
Web Search Handler using Tavily API for enhanced query responses
"""
from typing import Optional, Dict, Any, List
from tavily import TavilyClient
import json

class WebSearchHandler:
    """
    Web search handler using Tavily API for retrieving current information
    """
    
    def __init__(self, api_key: str):
        """
        Initialize web search handler with Tavily API key
        
        Args:
            api_key: Tavily API key
        """
        self.client = TavilyClient(api_key=api_key) if api_key else None
        self.api_key = api_key
    
    def search(self, query: str, max_results: int = 5) -> Dict[str, Any]:
        """
        Perform web search using Tavily
        
        Args:
            query: Search query
            max_results: Maximum number of results to return
            
        Returns:
            dict: Search results with success status and data
        """
        if not self.client:
            return {
                'success': False,
                'error': 'Tavily API key not configured',
                'results': []
            }
        
        try:
            # Perform search
            response = self.client.search(
                query=query,
                search_depth="basic",
                max_results=max_results,
                include_answer=True,
                include_raw_content=False
            )
            
            # Format results
            formatted_results = []
            if response.get('results'):
                for result in response['results']:
                    formatted_results.append({
                        'title': result.get('title', ''),
                        'url': result.get('url', ''),
                        'content': result.get('content', ''),
                        'score': result.get('score', 0)
                    })
            
            return {
                'success': True,
                'answer': response.get('answer', ''),
                'results': formatted_results,
                'query': query
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Web search failed: {str(e)}',
                'results': []
            }
    
    def get_context_for_llm(self, search_results: Dict[str, Any]) -> str:
        """
        Format search results as context for LLM
        
        Args:
            search_results: Results from web search
            
        Returns:
            str: Formatted context string
        """
        if not search_results.get('success') or not search_results.get('results'):
            return ""
        
        context = f"Web Search Results for: {search_results.get('query', '')}\n\n"
        
        # Add direct answer if available
        if search_results.get('answer'):
            context += f"Summary: {search_results['answer']}\n\n"
        
        # Add individual results
        context += "Detailed Sources:\n"
        for i, result in enumerate(search_results['results'][:3], 1):  # Top 3 results
            context += f"{i}. {result['title']}\n"
            context += f"   URL: {result['url']}\n"
            context += f"   Content: {result['content'][:300]}...\n\n"
        
        return context
    
    def is_available(self) -> bool:
        """
        Check if web search is available
        
        Returns:
            bool: True if API key is configured
        """
        return self.api_key is not None and self.client is not None