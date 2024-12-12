import os
import json
import time
from typing import Dict, List, Optional
import requests
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

class PubMedConnector:
    def __init__(self, config_path: str = "../config/pubmed_config.json"):
        self.config = self._load_config(config_path)
        self.setup_logging()
        self.base_url = self.config['api_endpoints']['base_url']
        self.endpoints = self.config['api_endpoints']
        self.search_params = self.config['search_parameters']
        
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pubmed_connector.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def build_search_query(self, categories: Optional[List[str]] = None) -> str:
        """Build PubMed search query from config terms"""
        if not categories:
            categories = [term['category'] for term in self.search_params['search_terms']]
            
        query_parts = []
        for category in categories:
            terms = next((item['terms'] for item in self.search_params['search_terms'] 
                        if item['category'] == category), [])
            if terms:
                category_query = '(' + ' OR '.join(f'"{term}"[Title/Abstract]' for term in terms) + ')'
                query_parts.append(category_query)
        
        return ' AND '.join(query_parts)
    
    async def search_articles(self, query: str, max_results: int = 100) -> List[Dict]:
        """Search PubMed articles"""
        params = {
            'db': self.search_params['db'],
            'term': query,
            'retmax': max_results,
            'retmode': self.search_params['retmode'],
            'datetype': 'pdat',
            'mindate': self.search_params['date_range']['start'],
            'maxdate': self.search_params['date_range']['end']
        }
        
        try:
            response = requests.get(f"{self.base_url}{self.endpoints['search_endpoint']}", params=params)
            response.raise_for_status()
            data = response.json()
            
            # Extract PMIDs
            pmids = data['esearchresult']['idlist']
            self.logger.info(f"Found {len(pmids)} articles matching search criteria")
            return await self.fetch_articles(pmids)
            
        except Exception as e:
            self.logger.error(f"Error searching PubMed: {str(e)}")
            return []
    
    async def fetch_articles(self, pmids: List[str]) -> List[Dict]:
        """Fetch full article details using E-utilities"""
        params = {
            'db': self.search_params['db'],
            'id': ','.join(pmids),
            'retmode': 'json'
        }
        
        try:
            # First get summaries
            summary_response = requests.get(
                f"{self.base_url}{self.endpoints['summary_endpoint']}",
                params=params
            )
            summary_response.raise_for_status()
            summaries = summary_response.json()
            
            # Then get full records
            params['retmode'] = 'xml'  # Full records are better in XML
            full_response = requests.get(
                f"{self.base_url}{self.endpoints['fetch_endpoint']}",
                params=params
            )
            full_response.raise_for_status()
            
            # Combine and process the data
            processed_articles = self._process_article_data(summaries, full_response.text)
            return processed_articles
            
        except Exception as e:
            self.logger.error(f"Error fetching articles: {str(e)}")
            return []
    
    def _process_article_data(self, summaries: Dict, full_text: str) -> List[Dict]:
        """Process and combine article data from summary and full text"""
        # This is a placeholder - we'll implement proper XML parsing
        processed = []
        for pmid, summary in summaries['result'].items():
            if pmid != 'uids':
                article = {
                    'pmid': pmid,
                    'title': summary.get('title', ''),
                    'authors': summary.get('authors', []),
                    'publication_date': summary.get('pubdate', ''),
                    'journal': summary.get('fulljournalname', ''),
                    'doi': summary.get('elocationid', ''),
                    'abstract': summary.get('abstract', '')
                }
                processed.append(article)
        return processed
    
    def save_articles(self, articles: List[Dict], output_dir: str):
        """Save processed articles to CSV and JSON"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save as CSV
        df = pd.DataFrame(articles)
        csv_path = output_path / f"pubmed_results_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(csv_path, index=False)
        
        # Save as JSON
        json_path = output_path / f"pubmed_results_{datetime.now().strftime('%Y%m%d')}.json"
        with open(json_path, 'w') as f:
            json.dump(articles, f, indent=2)
        
        return csv_path, json_path

# Example usage
async def main():
    connector = PubMedConnector()
    
    # Build and execute search
    query = connector.build_search_query(['diabetes_tech', 'region', 'study_type'])
    articles = await connector.search_articles(query)
    
    # Save results
    if articles:
        csv_path, json_path = connector.save_articles(
            articles,
            "../../research_papers/pubmed/"
        )
        print(f"Results saved to:\nCSV: {csv_path}\nJSON: {json_path}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())