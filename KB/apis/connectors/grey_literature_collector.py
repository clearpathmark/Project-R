import os
import json
import logging
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pathlib import Path
import asyncio
import aiohttp
import re

class GreyLiteratureCollector:
    def __init__(self, config_path: str = "../config/grey_literature_config.json"):
        print(f"Initializing Grey Literature Collector...")
        print(f"Looking for config file at: {os.path.abspath(config_path)}")
        
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        
        # Initialize source URLs
        self.sources = self.config['sources']
        self.search_terms = self.config['search_terms']
        
        print("Grey Literature Collector initialized successfully")
    
    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {}
    
    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("GreyLitCollector")
        handler = logging.FileHandler('grey_literature_collector.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    async def collect_ec_reports(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Collect European Commission health reports"""
        reports = []
        base_url = self.sources['european_commission']['base_url']
        
        for category in self.sources['european_commission']['categories']:
            for term in self.search_terms['diabetes_technology']:
                search_url = f"{base_url}?keywords={term}&category={category}"
                
                try:
                    async with session.get(search_url) as response:
                        if response.status == 200:
                            html = await response.text()
                            soup = BeautifulSoup(html, 'html.parser')
                            
                            # Extract reports (example structure)
                            for item in soup.find_all('div', class_='publication-item'):
                                report = {
                                    'source': 'European Commission',
                                    'title': item.find('h3').text.strip() if item.find('h3') else '',
                                    'url': item.find('a')['href'] if item.find('a') else '',
                                    'publication_date': item.find('span', class_='date').text if item.find('span', class_='date') else '',
                                    'category': category,
                                    'search_term': term,
                                    'document_type': 'report',
                                    'collection_date': datetime.now().strftime("%Y-%m-%d")
                                }
                                reports.append(report)
                                print(f"Found EC report: {report['title']}")
                        else:
                            self.logger.error(f"Error {response.status} for EC search: {search_url}")
                except Exception as e:
                    self.logger.error(f"Error collecting EC reports: {str(e)}")
        
        return reports

    async def collect_nhs_evidence(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Collect NHS evidence and technology assessments"""
        evidence = []
        base_url = self.sources['nhs_evidence']['base_url']
        
        for doc_type in self.sources['nhs_evidence']['document_types']:
            for term in self.search_terms['diabetes_technology']:
                search_url = f"{base_url}?q={term}&type={doc_type}"
                
                try:
                    async with session.get(search_url) as response:
                        if response.status == 200:
                            html = await response.text()
                            soup = BeautifulSoup(html, 'html.parser')
                            
                            # Extract evidence documents
                            for item in soup.find_all('div', class_='evidence-item'):
                                doc = {
                                    'source': 'NHS Evidence',
                                    'title': item.find('h2').text.strip() if item.find('h2') else '',
                                    'url': item.find('a')['href'] if item.find('a') else '',
                                    'publication_date': item.find('span', class_='date').text if item.find('span', class_='date') else '',
                                    'document_type': doc_type,
                                    'search_term': term,
                                    'collection_date': datetime.now().strftime("%Y-%m-%d")
                                }
                                evidence.append(doc)
                                print(f"Found NHS evidence: {doc['title']}")
                        else:
                            self.logger.error(f"Error {response.status} for NHS search: {search_url}")
                except Exception as e:
                    self.logger.error(f"Error collecting NHS evidence: {str(e)}")
        
        return evidence

    async def collect_ema_reports(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Collect EMA public assessment reports"""
        reports = []
        base_url = self.sources['ema_reports']['base_url']
        
        for category in self.sources['ema_reports']['categories']:
            search_url = f"{base_url}?disease={category}"
            
            try:
                async with session.get(search_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Extract EMA reports
                        for item in soup.find_all('div', class_='medicine-item'):
                            report = {
                                'source': 'EMA',
                                'title': item.find('h3').text.strip() if item.find('h3') else '',
                                'url': item.find('a')['href'] if item.find('a') else '',
                                'category': category,
                                'document_type': 'assessment_report',
                                'publication_date': item.find('span', class_='date').text if item.find('span', class_='date') else '',
                                'collection_date': datetime.now().strftime("%Y-%m-%d")
                            }
                            reports.append(report)
                            print(f"Found EMA report: {report['title']}")
                    else:
                        self.logger.error(f"Error {response.status} for EMA search: {search_url}")
            except Exception as e:
                self.logger.error(f"Error collecting EMA reports: {str(e)}")
        
        return reports

    async def collect_all_literature(self):
        """Collect all grey literature from configured sources"""
        async with aiohttp.ClientSession() as session:
            # Collect from all sources
            ec_reports = await self.collect_ec_reports(session)
            nhs_evidence = await self.collect_nhs_evidence(session)
            ema_reports = await self.collect_ema_reports(session)
            
            # Combine all results
            all_literature = {
                'european_commission': ec_reports,
                'nhs_evidence': nhs_evidence,
                'ema_reports': ema_reports
            }
            
            return all_literature

    def save_literature_data(self, data: Dict[str, List[Dict]], output_dir: str = "../../research_papers/grey_literature/"):
        """Save collected literature data"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save each source's data separately
        for source, documents in data.items():
            if documents:
                # Save as CSV
                df = pd.DataFrame(documents)
                csv_path = output_path / f"{source}_literature_{timestamp}.csv"
                df.to_csv(csv_path, index=False)
                print(f"Saved {source} CSV to: {csv_path}")
                
                # Save as JSON
                json_path = output_path / f"{source}_literature_{timestamp}.json"
                with open(json_path, 'w') as f:
                    json.dump(documents, f, indent=2)
                print(f"Saved {source} JSON to: {json_path}")
        
        # Generate and save summary
        summary = {
            'collection_date': timestamp,
            'sources': {}
        }
        
        for source, documents in data.items():
            summary['sources'][source] = {
                'total_documents': len(documents),
                'document_types': list(set(doc.get('document_type', '') for doc in documents)),
                'date_range': {
                    'earliest': min((doc.get('publication_date', '') for doc in documents), default=''),
                    'latest': max((doc.get('publication_date', '') for doc in documents), default='')
                }
            }
        
        summary_path = output_path / f"grey_literature_summary_{timestamp}.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Saved summary to: {summary_path}")
        
        return summary

async def main():
    print("\n=== Starting Grey Literature Collection ===\n")
    
    try:
        collector = GreyLiteratureCollector()
        literature_data = await collector.collect_all_literature()
        
        if any(documents for documents in literature_data.values()):
            summary = collector.save_literature_data(literature_data)
            print("\nCollection Summary:")
            print(json.dumps(summary, indent=2))
        else:
            print("\nNo documents collected")
            
    except Exception as e:
        print(f"\nERROR: Script failed with error: {str(e)}")
    
    print("\n=== Grey Literature Collection Complete ===\n")

if __name__ == "__main__":
    asyncio.run(main())