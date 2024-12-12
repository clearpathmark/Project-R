import os
import json
import time
from typing import Dict, List, Optional
import requests
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

class BaseConnector:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.setup_logging()
    
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('api_connector.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

class PubMedConnector(BaseConnector):
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.base_url = self.config['api_endpoints']['pubmed']['base_url']
        self.search_endpoint = self.config['api_endpoints']['pubmed']['search_endpoint']
        self.fetch_endpoint = self.config['api_endpoints']['pubmed']['fetch_endpoint']
    
    async def search_articles(self, query: str, max_results: int = 100) -> List[Dict]:
        """Search PubMed articles"""
        params = {
            'db': 'pubmed',
            'term': query,
            'retmax': max_results,
            'retmode': 'json'
        }
        
        try:
            response = requests.get(f"{self.base_url}{self.search_endpoint}", params=params)
            response.raise_for_status()
            data = response.json()
            
            # Extract PMIDs
            pmids = data['esearchresult']['idlist']
            return await self.fetch_articles(pmids)
            
        except Exception as e:
            self.logger.error(f"Error searching PubMed: {str(e)}")
            return []
    
    async def fetch_articles(self, pmids: List[str]) -> List[Dict]:
        """Fetch full article details"""
        params = {
            'db': 'pubmed',
            'id': ','.join(pmids),
            'retmode': 'json'
        }
        
        try:
            response = requests.get(f"{self.base_url}{self.fetch_endpoint}", params=params)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            self.logger.error(f"Error fetching articles: {str(e)}")
            return []

class ScopusConnector(BaseConnector):
    def __init__(self, config_path: str, api_key: str):
        super().__init__(config_path)
        self.base_url = self.config['api_endpoints']['scopus']['base_url']
        self.search_endpoint = self.config['api_endpoints']['scopus']['search_endpoint']
        self.api_key = api_key
        
    async def search_articles(self, query: str, max_results: int = 100) -> List[Dict]:
        """Search Scopus articles"""
        headers = {
            'X-ELS-APIKey': self.api_key,
            'Accept': 'application/json'
        }
        
        params = {
            'query': query,
            'count': max_results
        }
        
        try:
            response = requests.get(
                f"{self.base_url}{self.search_endpoint}",
                headers=headers,
                params=params
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            self.logger.error(f"Error searching Scopus: {str(e)}")
            return []

class WHOConnector(BaseConnector):
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.base_url = self.config['api_endpoints']['who_diabetes']['base_url']
        self.endpoint = self.config['api_endpoints']['who_diabetes']['endpoint']
    
    async def fetch_diabetes_data(self, country_code: Optional[str] = None) -> Dict:
        """Fetch WHO diabetes data"""
        url = f"{self.base_url}{self.endpoint}"
        if country_code:
            url = f"{url}/{country_code}"
            
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            self.logger.error(f"Error fetching WHO data: {str(e)}")
            return {}

class DataProcessor:
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def process_pubmed_data(self, data: List[Dict]) -> pd.DataFrame:
        """Process PubMed article data"""
        processed_data = []
        for article in data:
            processed_article = {
                'title': article.get('title', ''),
                'authors': article.get('authors', []),
                'publication_date': article.get('publication_date', ''),
                'journal': article.get('journal', ''),
                'abstract': article.get('abstract', ''),
                'keywords': article.get('keywords', []),
                'doi': article.get('doi', '')
            }
            processed_data.append(processed_article)
        
        df = pd.DataFrame(processed_data)
        return df
    
    def save_data(self, data: pd.DataFrame, filename: str):
        """Save processed data"""
        output_path = self.output_dir / filename
        data.to_csv(output_path, index=False)
        return output_path

async def main():
    # Initialize connectors
    config_path = "KB/apis/config/config.json"
    pubmed = PubMedConnector(config_path)
    scopus = ScopusConnector(config_path, api_key="YOUR_API_KEY")
    who = WHOConnector(config_path)
    
    # Initialize processor
    processor = DataProcessor("KB/research_papers/metadata")
    
    # Example search
    query = "Type 1 diabetes Europe technology"
    pubmed_results = await pubmed.search_articles(query)
    
    # Process and save results
    processed_data = processor.process_pubmed_data(pubmed_results)
    output_path = processor.save_data(processed_data, f"pubmed_results_{datetime.now().strftime('%Y%m%d')}.csv")
    
    print(f"Data saved to: {output_path}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())