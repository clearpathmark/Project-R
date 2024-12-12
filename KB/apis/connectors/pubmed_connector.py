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
        print(f"Initializing PubMed Connector...")
        print(f"Looking for config file at: {os.path.abspath(config_path)}")
        
        self.config = self._load_config(config_path)
        self.setup_logging()
        self.base_url = self.config['api_endpoints']['base_url']
        self.endpoints = self.config['api_endpoints']
        self.search_params = self.config['search_parameters']
        
        print("PubMed Connector initialized successfully")
        
    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print("Configuration loaded successfully")
            return config
        except FileNotFoundError:
            print(f"ERROR: Config file not found at {os.path.abspath(config_path)}")
            raise
        except json.JSONDecodeError:
            print(f"ERROR: Invalid JSON in config file")
            raise
    
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
        print("Logging setup complete")
    
    def build_search_query(self, categories: Optional[List[str]] = None) -> str:
        if not categories:
            categories = [term['category'] for term in self.search_params['search_terms']]
            
        print(f"Building search query for categories: {categories}")
        query_parts = []
        for category in categories:
            terms = next((item['terms'] for item in self.search_params['search_terms'] 
                        if item['category'] == category), [])
            if terms:
                category_query = '(' + ' OR '.join(f'"{term}"[Title/Abstract]' for term in terms) + ')'
                query_parts.append(category_query)
        
        final_query = ' AND '.join(query_parts)
        print(f"Final query: {final_query}")
        return final_query
    
    async def search_articles(self, query: str, max_results: int = 100) -> List[Dict]:
        print(f"\nSearching PubMed with query: {query}")
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
            print("Making request to PubMed search endpoint...")
            response = requests.get(f"{self.base_url}{self.endpoints['search_endpoint']}", params=params)
            response.raise_for_status()
            data = response.json()
            
            # Extract PMIDs
            pmids = data['esearchresult']['idlist']
            print(f"Found {len(pmids)} articles matching search criteria")
            
            if not pmids:
                print("No articles found matching the search criteria")
                return []
                
            return await self.fetch_articles(pmids)
            
        except requests.RequestException as e:
            print(f"Error searching PubMed: {str(e)}")
            return []
    
    async def fetch_articles(self, pmids: List[str]) -> List[Dict]:
        print(f"\nFetching details for {len(pmids)} articles...")
        params = {
            'db': self.search_params['db'],
            'id': ','.join(pmids),
            'retmode': 'json'
        }
        
        try:
            print("Fetching article summaries...")
            summary_response = requests.get(
                f"{self.base_url}{self.endpoints['summary_endpoint']}",
                params=params
            )
            summary_response.raise_for_status()
            summaries = summary_response.json()
            
            print("Fetching full article records...")
            params['retmode'] = 'xml'
            full_response = requests.get(
                f"{self.base_url}{self.endpoints['fetch_endpoint']}",
                params=params
            )
            full_response.raise_for_status()
            
            print("Processing article data...")
            processed_articles = self._process_article_data(summaries, full_response.text)
            print(f"Successfully processed {len(processed_articles)} articles")
            return processed_articles
            
        except Exception as e:
            print(f"Error fetching articles: {str(e)}")
            return []
    
    def _process_article_data(self, summaries: Dict, full_text: str) -> List[Dict]:
        processed = []
        try:
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
        except Exception as e:
            print(f"Error processing article data: {str(e)}")
            return []
    
    def save_articles(self, articles: List[Dict], output_dir: str):
        print(f"\nSaving {len(articles)} articles to {output_dir}")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        try:
            # Save as CSV
            df = pd.DataFrame(articles)
            csv_path = output_path / f"pubmed_results_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(csv_path, index=False)
            print(f"Saved CSV to: {csv_path}")
            
            # Save as JSON
            json_path = output_path / f"pubmed_results_{datetime.now().strftime('%Y%m%d')}.json"
            with open(json_path, 'w') as f:
                json.dump(articles, f, indent=2)
            print(f"Saved JSON to: {json_path}")
            
            return csv_path, json_path
        except Exception as e:
            print(f"Error saving articles: {str(e)}")
            return None, None

async def main():
    try:
        print("\n=== Starting PubMed Data Collection ===\n")
        connector = PubMedConnector()
        
        # Build and execute search
        query = connector.build_search_query(['diabetes_tech', 'region', 'study_type'])
        articles = await connector.search_articles(query)
        
        # Save results
        if articles:
            print(f"\nFound {len(articles)} articles to save")
            csv_path, json_path = connector.save_articles(
                articles,
                "../../research_papers/pubmed/"
            )
            if csv_path and json_path:
                print(f"\nResults successfully saved to:\nCSV: {csv_path}\nJSON: {json_path}")
        else:
            print("\nNo articles to save")
            
        print("\n=== PubMed Data Collection Complete ===\n")
            
    except Exception as e:
        print(f"\nERROR: Script failed with error: {str(e)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())