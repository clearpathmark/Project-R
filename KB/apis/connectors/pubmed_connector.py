import os
import json
import time
from typing import Dict, List, Optional, Set, Tuple
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging
from collections import Counter
import xml.etree.ElementTree as ET
from urllib.parse import quote
import re

class EnhancedPubMedConnector:
    def __init__(self, config_path: str = "../config/pubmed_config.json"):
        print(f"Initializing Enhanced PubMed Connector...")
        print(f"Looking for config file at: {os.path.abspath(config_path)}")
        
        self.config = self._load_config(config_path)
        self.setup_logging()
        self.base_url = self.config['api_endpoints']['base_url']
        self.endpoints = self.config['api_endpoints']
        self.search_params = self.config['search_parameters']
        self.analysis_metrics = {}
        
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
        
        # Add publication type filters
        pub_types = [
            '"Clinical Trial"[Publication Type]',
            '"Meta-Analysis"[Publication Type]',
            '"Systematic Review"[Publication Type]',
            '"Randomized Controlled Trial"[Publication Type]'
        ]
        query_parts.append('(' + ' OR '.join(pub_types) + ')')
        
        final_query = ' AND '.join(query_parts)
        print(f"Final query: {final_query}")
        return final_query
    
    async def search_articles(self, query: str, max_results: int = 1000) -> List[Dict]:
        print(f"\nSearching PubMed with query: {query}")
        
        # Initialize for pagination
        total_results = []
        retstart = 0
        retmax = min(100, max_results)  # PubMed recommends max 100 per request
        
        while retstart < max_results:
            params = {
                'db': self.search_params['db'],
                'term': query,
                'retmax': retmax,
                'retstart': retstart,
                'retmode': self.search_params['retmode'],
                'datetype': 'pdat',
                'mindate': self.search_params['date_range']['start'],
                'maxdate': self.search_params['date_range']['end']
            }
            
            try:
                print(f"Making request to PubMed search endpoint (batch starting at {retstart})...")
                response = requests.get(f"{self.base_url}{self.endpoints['search_endpoint']}", params=params)
                response.raise_for_status()
                data = response.json()
                
                # Extract PMIDs
                pmids = data['esearchresult']['idlist']
                if not pmids:
                    break
                    
                print(f"Found {len(pmids)} articles in current batch")
                articles = await self.fetch_articles(pmids)
                total_results.extend(articles)
                
                retstart += retmax
                if retstart >= int(data['esearchresult']['count']) or len(total_results) >= max_results:
                    break
                    
                # Respect API rate limits
                time.sleep(0.34)  # PubMed allows 3 requests per second
                
            except requests.RequestException as e:
                print(f"Error searching PubMed: {str(e)}")
                break
        
        print(f"\nTotal articles collected: {len(total_results)}")
        return total_results[:max_results]
    
    async def fetch_articles(self, pmids: List[str]) -> List[Dict]:
        print(f"\nFetching details for {len(pmids)} articles...")
        params = {
            'db': self.search_params['db'],
            'id': ','.join(pmids),
            'retmode': 'xml'
        }
        
        try:
            print("Fetching full article records...")
            response = requests.get(
                f"{self.base_url}{self.endpoints['fetch_endpoint']}",
                params=params
            )
            response.raise_for_status()
            
            # Process XML response
            articles = self._process_article_data(response.text)
            print(f"Successfully processed {len(articles)} articles")
            return articles
            
        except Exception as e:
            print(f"Error fetching articles: {str(e)}")
            return []
    
    def _process_article_data(self, xml_text: str) -> List[Dict]:
        processed = []
        try:
            root = ET.fromstring(xml_text)
            
            for article in root.findall('.//PubmedArticle'):
                try:
                    # Extract basic metadata
                    pmid = article.find('.//PMID').text
                    article_data = {
                        'pmid': pmid,
                        'title': self._safe_find_text(article, './/ArticleTitle'),
                        'abstract': self._safe_find_text(article, './/Abstract/AbstractText'),
                        'journal': self._safe_find_text(article, './/Journal/Title'),
                        'publication_date': self._extract_publication_date(article),
                        'authors': self._extract_authors(article),
                        'doi': self._safe_find_text(article, './/ArticleId[@IdType="doi"]'),
                        
                        # Enhanced metadata
                        'mesh_terms': self._extract_mesh_terms(article),
                        'keywords': self._extract_keywords(article),
                        'publication_types': self._extract_publication_types(article),
                        'chemicals': self._extract_chemicals(article),
                        'grants': self._extract_grants(article),
                        
                        # Extracted insights
                        'study_type': self._determine_study_type(article),
                        'population_size': self._extract_population_size(article),
                        'country': self._extract_country(article),
                        'institutions': self._extract_institutions(article),
                        
                        # Analysis fields
                        'technology_mentions': self._analyze_technology_mentions(article),
                        'outcome_measures': self._extract_outcome_measures(article),
                        'limitations': self._extract_limitations(article),
                        'key_findings': self._extract_key_findings(article)
                    }
                    
                    # Add to collection
                    processed.append(article_data)
                    
                except Exception as e:
                    print(f"Error processing article {pmid}: {str(e)}")
                    continue
                    
            return processed
            
        except Exception as e:
            print(f"Error parsing XML data: {str(e)}")
            return []
    
    def _safe_find_text(self, element: ET.Element, xpath: str) -> str:
        """Safely extract text from XML element"""
        found = element.find(xpath)
        return found.text if found is not None else ""
    
    def _extract_publication_date(self, article: ET.Element) -> str:
        """Extract and format publication date"""
        pub_date = article.find('.//PubDate')
        if pub_date is not None:
            year = self._safe_find_text(pub_date, 'Year')
            month = self._safe_find_text(pub_date, 'Month')
            day = self._safe_find_text(pub_date, 'Day')
            
            if month.isalpha():
                month = {
                    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                }.get(month[:3], '01')
            
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}" if all([year, month, day]) else year
        return ""
    
def _extract_authors(self, article: ET.Element) -> List[Dict]:
    """Extract detailed author information with better error handling"""
    authors = []
    try:
        for author in article.findall('.//Author'):
            try:
                author_data = {
                    'lastname': self._safe_find_text(author, 'LastName') or '',
                    'firstname': self._safe_find_text(author, 'ForeName') or '',
                    'initials': self._safe_find_text(author, 'Initials') or '',
                    'affiliation': self._safe_find_text(author, './/Affiliation') or ''
                }
                if any(author_data.values()):  # Only add if we have any data
                    authors.append(author_data)
            except Exception as e:
                continue
        return authors
    except Exception as e:
        return []
    
    def _extract_mesh_terms(self, article: ET.Element) -> List[str]:
        """Extract MeSH terms"""
        return [mesh.find('DescriptorName').text 
                for mesh in article.findall('.//MeshHeading')
                if mesh.find('DescriptorName') is not None]
    
    def _extract_keywords(self, article: ET.Element) -> List[str]:
        """Extract keywords"""
        keywords = []
        for keyword in article.findall('.//Keyword'):
            if keyword.text:
                keywords.append(keyword.text.strip())
        return keywords
    
    def _extract_publication_types(self, article: ET.Element) -> List[str]:
        """Extract publication types"""
        return [pt.text for pt in article.findall('.//PublicationType')
                if pt.text is not None]
    
    def _extract_chemicals(self, article: ET.Element) -> List[str]:
        """Extract chemical substances"""
        return [chem.find('NameOfSubstance').text 
                for chem in article.findall('.//Chemical')
                if chem.find('NameOfSubstance') is not None]
    
    def _extract_grants(self, article: ET.Element) -> List[Dict]:
        """Extract grant information"""
        grants = []
        for grant in article.findall('.//Grant'):
            grant_data = {
                'id': self._safe_find_text(grant, 'GrantID'),
                'agency': self._safe_find_text(grant, 'Agency'),
                'country': self._safe_find_text(grant, 'Country')
            }
            grants.append(grant_data)
        return grants
    
    def _determine_study_type(self, article: ET.Element) -> str:
        """Determine study type from publication types and MeSH terms"""
        pub_types = self._extract_publication_types(article)
        mesh_terms = self._extract_mesh_terms(article)
        
        study_types = {
            'Randomized Controlled Trial': 'RCT',
            'Clinical Trial': 'Clinical Trial',
            'Meta-Analysis': 'Meta-Analysis',
            'Systematic Review': 'Systematic Review',
            'Observational Study': 'Observational',
            'Case Reports': 'Case Report'
        }
        
        for type_name, short_name in study_types.items():
            if any(type_name in pt for pt in pub_types) or any(type_name in mt for mt in mesh_terms):
                return short_name
                
        return 'Other'
    
    def _extract_population_size(self, article: ET.Element) -> Optional[int]:
        """Extract study population size from abstract"""
        abstract = self._safe_find_text(article, './/Abstract/AbstractText')
        if abstract:
            # Look for patterns like "n=123" or "N=123" or "123 patients"
            patterns = [
                r'[nN]\s*=\s*(\d+)',
                r'(\d+)\s*patients',
                r'(\d+)\s*subjects',
                r'(\d+)\s*participants'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, abstract)
                if match:
                    try:
                        return int(match.group(1))
                    except ValueError:
                        continue
        return None
    
    def _extract_country(self, article: ET.Element) -> List[str]:
        """Extract study countries from affiliations and abstract"""
        countries = set()
        
        # Check affiliations
        for affiliation in article.findall('.//Affiliation'):
            if affiliation.text:
                # Add your country detection logic here
                # This is a simplified example
                for country in self.config.get('countries', []):
                    if country in affiliation.text:
                        countries.add(country)
        
        return list(countries)
    
    def _extract_institutions(self, article: ET.Element) -> List[str]:
        """Extract unique institutions from affiliations"""
        institutions = set()
        
        for affiliation in article.findall('.//Affiliation'):
            if affiliation.text:
                # Simple splitting by comma and semicolon
                parts = re.split('[,;]', affiliation.text)
                for part in parts:
                    if 'university' in part.lower() or 'hospital' in part.lower():
                        institutions.add(part.strip())
        
        return list(institutions)
    
    def _analyze_technology_mentions(self, article: ET.Element) -> Dict[str, int]:
        """Analyze technology mentions in title and abstract"""
        text = ' '.join([
            self._safe_find_text(article, './/ArticleTitle'),
            self._safe_find_text(article, './/Abstract/AbstractText')
        ]).lower()
        
        tech_mentions = {}
        for tech_type in self.config.get('technologies', []):
            count = sum(1 for term in tech_type['terms'] 
                       if term.lower() in text)
            if count > 0:
                tech_mentions[tech_type['name']] = count
        
        return tech_mentions
    
    def _extract_outcome_measures(self, article: ET.Element) -> List[str]:
        """Extract outcome measures from abstract"""
        abstract = self._safe_find_text(article, './/Abstract/AbstractText')
        outcomes = []
        
        # Common outcome measure patterns
        patterns = [
            r'primary (?:outcome|endpoint)(?:s)?\s+(?:was|were)\s+([^\.]+)',
            r'secondary (?:outcome|endpoint)(?:s)?\s+(?:was|were)\s+([^\.]+)',
            r'measured (?:using|with|by)\s+([^\.]+)',
            r'(?:HbA1c|glycated hemoglobin|time in range|TIR|glucose variability)\s+([^\.]+)'
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, abstract, re.IGNORECASE)
            outcomes.extend(match.group(1).strip() for match in matches)
        
        return outcomes

    def _extract_limitations(self, article: ET.Element) -> List[str]:
        """Extract study limitations from abstract"""
        abstract = self._safe_find_text(article, './/Abstract/AbstractText')
        limitations = []
        
        # Look for limitation sections
        patterns = [
            r'limitation(?:s)?\s+(?:was|were|include|included)\s+([^\.]+)',
            r'(?:study|studies) (?:was|were) limited by\s+([^\.]+)',
            r'(?:weakness|weaknesses|drawback|drawbacks)\s+(?:was|were)\s+([^\.]+)'
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, abstract, re.IGNORECASE)
            limitations.extend(match.group(1).strip() for match in matches)
        
        return limitations

    def _extract_key_findings(self, article: ET.Element) -> List[str]:
        """Extract key findings from abstract"""
        abstract = self._safe_find_text(article, './/Abstract/AbstractText')
        findings = []
        
        # Look for conclusion and results sections
        patterns = [
            r'(?:conclusion|conclusions):\s+([^\.]+)',
            r'(?:we|authors) conclude(?:d)?\s+(?:that)?\s+([^\.]+)',
            r'(?:result|results) showed that\s+([^\.]+)',
            r'(?:significant|significantly)\s+([^\.]+)'
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, abstract, re.IGNORECASE)
            findings.extend(match.group(1).strip() for match in matches)
        
        return findings

    def analyze_collection(self, articles: List[Dict]) -> Dict:
        """Perform comprehensive analysis on collected articles"""
        analysis = {
            'total_articles': len(articles),
            'publication_years': self._analyze_years(articles),
            'study_types': self._analyze_study_types(articles),
            'top_authors': self._analyze_authors(articles),
            'top_institutions': self._analyze_institutions(articles),
            'technology_trends': self._analyze_technology_trends(articles),
            'geographic_distribution': self._analyze_geography(articles),
            'outcome_measures': self._analyze_outcomes(articles),
            'research_focus': self._analyze_research_focus(articles)
        }
        
        return analysis

    def _analyze_years(self, articles: List[Dict]) -> Dict[str, int]:
        """Analyze publication year distribution"""
        years = [article['publication_date'][:4] for article in articles if article['publication_date']]
        return dict(Counter(years))

    def _analyze_study_types(self, articles: List[Dict]) -> Dict[str, int]:
        """Analyze distribution of study types"""
        types = [article['study_type'] for article in articles if article['study_type']]
        return dict(Counter(types))

    def _analyze_authors(self, articles: List[Dict]) -> List[Dict]:
        """Analyze top authors and their contributions"""
        author_stats = {}
        
        for article in articles:
            for author in article['authors']:
                key = f"{author['lastname']}, {author['firstname']}"
                if key not in author_stats:
                    author_stats[key] = {
                        'count': 0,
                        'affiliations': set(),
                        'recent_year': '',
                        'articles': []
                    }
                
                author_stats[key]['count'] += 1
                if author['affiliation']:
                    author_stats[key]['affiliations'].add(author['affiliation'])
                if article['publication_date']:
                    year = article['publication_date'][:4]
                    if not author_stats[key]['recent_year'] or year > author_stats[key]['recent_year']:
                        author_stats[key]['recent_year'] = year
                author_stats[key]['articles'].append(article['pmid'])
        
        # Convert to sorted list
        authors_list = [
            {
                'name': name,
                'publication_count': stats['count'],
                'affiliations': list(stats['affiliations']),
                'recent_year': stats['recent_year'],
                'articles': stats['articles']
            }
            for name, stats in author_stats.items()
        ]
        
        return sorted(authors_list, key=lambda x: x['publication_count'], reverse=True)

    def _analyze_institutions(self, articles: List[Dict]) -> List[Dict]:
        """Analyze institutional contributions"""
        institution_stats = {}
        
        for article in articles:
            for inst in article['institutions']:
                if inst not in institution_stats:
                    institution_stats[inst] = {
                        'count': 0,
                        'countries': set(),
                        'recent_year': '',
                        'articles': []
                    }
                
                institution_stats[inst]['count'] += 1
                for country in article['country']:
                    institution_stats[inst]['countries'].add(country)
                if article['publication_date']:
                    year = article['publication_date'][:4]
                    if not institution_stats[inst]['recent_year'] or year > institution_stats[inst]['recent_year']:
                        institution_stats[inst]['recent_year'] = year
                institution_stats[inst]['articles'].append(article['pmid'])
        
        # Convert to sorted list
        institutions_list = [
            {
                'name': name,
                'publication_count': stats['count'],
                'countries': list(stats['countries']),
                'recent_year': stats['recent_year'],
                'articles': stats['articles']
            }
            for name, stats in institution_stats.items()
        ]
        
        return sorted(institutions_list, key=lambda x: x['publication_count'], reverse=True)

    def _analyze_technology_trends(self, articles: List[Dict]) -> Dict:
        """Analyze technology trends over time"""
        trends = {
            'overall': {},
            'by_year': {}
        }
        
        for article in articles:
            year = article['publication_date'][:4] if article['publication_date'] else 'unknown'
            
            # Update overall counts
            for tech, count in article['technology_mentions'].items():
                trends['overall'][tech] = trends['overall'].get(tech, 0) + count
                
                # Update yearly counts
                if year != 'unknown':
                    if year not in trends['by_year']:
                        trends['by_year'][year] = {}
                    trends['by_year'][year][tech] = trends['by_year'][year].get(tech, 0) + count
        
        return trends

    def _analyze_geography(self, articles: List[Dict]) -> Dict:
        """Analyze geographic distribution of research"""
        geography = {
            'countries': {},
            'regions': {},
            'collaborations': []
        }
        
        for article in articles:
            # Count country appearances
            for country in article['country']:
                geography['countries'][country] = geography['countries'].get(country, 0) + 1
                
            # Identify multi-country collaborations
            if len(article['country']) > 1:
                geography['collaborations'].append({
                    'countries': article['country'],
                    'year': article['publication_date'][:4] if article['publication_date'] else 'unknown',
                    'pmid': article['pmid']
                })
        
        return geography

    def _analyze_outcomes(self, articles: List[Dict]) -> Dict:
        """Analyze reported outcomes and measures"""
        outcomes = {
            'primary_measures': {},
            'secondary_measures': {},
            'by_study_type': {}
        }
        
        for article in articles:
            study_type = article['study_type']
            if study_type not in outcomes['by_study_type']:
                outcomes['by_study_type'][study_type] = {}
                
            for outcome in article['outcome_measures']:
                # Categorize as primary or secondary
                if 'primary' in outcome.lower():
                    outcomes['primary_measures'][outcome] = outcomes['primary_measures'].get(outcome, 0) + 1
                elif 'secondary' in outcome.lower():
                    outcomes['secondary_measures'][outcome] = outcomes['secondary_measures'].get(outcome, 0) + 1
                
                # Count by study type
                outcomes['by_study_type'][study_type][outcome] = \
                    outcomes['by_study_type'][study_type].get(outcome, 0) + 1
        
        return outcomes

    def _analyze_research_focus(self, articles: List[Dict]) -> Dict:
        """Analyze research focus areas"""
        focus = {
            'keywords': {},
            'mesh_terms': {},
            'emerging_topics': {}
        }
        
        # Analyze by year to identify emerging topics
        yearly_topics = {}
        
        for article in articles:
            year = article['publication_date'][:4] if article['publication_date'] else 'unknown'
            
            # Count keywords
            for keyword in article['keywords']:
                focus['keywords'][keyword] = focus['keywords'].get(keyword, 0) + 1
                
                # Track yearly occurrence
                if year != 'unknown':
                    if year not in yearly_topics:
                        yearly_topics[year] = {}
                    yearly_topics[year][keyword] = yearly_topics[year].get(keyword, 0) + 1
            
            # Count MeSH terms
            for term in article['mesh_terms']:
                focus['mesh_terms'][term] = focus['mesh_terms'].get(term, 0) + 1
        
        # Identify emerging topics (increasing trend in recent years)
        sorted_years = sorted(yearly_topics.keys())
        if len(sorted_years) >= 2:
            recent_years = sorted_years[-2:]
            for topic, counts in yearly_topics[recent_years[-1]].items():
                if topic in yearly_topics.get(recent_years[-2], {}) and \
                   counts > yearly_topics[recent_years[-2]].get(topic, 0):
                    focus['emerging_topics'][topic] = {
                        'recent_count': counts,
                        'previous_count': yearly_topics[recent_years[-2]].get(topic, 0),
                        'growth_rate': (counts - yearly_topics[recent_years[-2]].get(topic, 0)) / \
                                     yearly_topics[recent_years[-2]].get(topic, 1)
                    }
        
        return focus

    def save_articles(self, articles: List[Dict], output_dir: str):
        """Save processed articles with enhanced analysis"""
        print(f"\nSaving {len(articles)} articles to {output_dir}")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        try:
            # Save detailed results
            timestamp = datetime.now().strftime('%Y%m%d')
            
            # Save raw article data
            articles_path = output_path / f"pubmed_articles_{timestamp}.json"
            with open(articles_path, 'w') as f:
                json.dump(articles, f, indent=2)
            print(f"Saved detailed articles to: {articles_path}")
            
            # Create summary CSV
            df = pd.DataFrame([{
                'pmid': article['pmid'],
                'title': article['title'],
                'authors': '; '.join([f"{a['lastname']}, {a['firstname']}" for a in article['authors']]),
                'publication_date': article['publication_date'],
                'journal': article['journal'],
                'study_type': article['study_type'],
                'population_size': article['population_size'],
                'countries': '; '.join(article['country']),
                'technologies': '; '.join(f"{k}({v})" for k, v in article['technology_mentions'].items()),
                'key_findings': '; '.join(article['key_findings']),
                'mesh_terms': '; '.join(article['mesh_terms']),
                'doi': article['doi']
            } for article in articles])
            
            csv_path = output_path / f"pubmed_summary_{timestamp}.csv"
            df.to_csv(csv_path, index=False)
            print(f"Saved summary CSV to: {csv_path}")
            
            # Generate and save analysis
            analysis = self.analyze_collection(articles)
            analysis_path = output_path / f"pubmed_analysis_{timestamp}.json"
            with open(analysis_path, 'w') as f:
                json.dump(analysis, f, indent=2)
            print(f"Saved analysis to: {analysis_path}")
            
            return articles_path, csv_path, analysis_path
            
        except Exception as e:
            print(f"Error saving articles: {str(e)}")
            return None

async def main():
    try:
        print("\n=== Starting Enhanced PubMed Data Collection ===\n")
        connector = EnhancedPubMedConnector()
        
        # Build and execute search
        query = connector.build_search_query(['diabetes_tech', 'clinical_focus', 'population'])
        articles = await connector.search_articles(query, max_results=1000)
        
        # Save results
        if articles:
            print(f"\nFound {len(articles)} articles to process")
            results = connector.save_articles(
                articles,
                "../../research_papers/pubmed/"
            )
            if results:
                articles_path, csv_path, analysis_path = results
                print(f"\nResults successfully saved:")
                print(f"Detailed Articles: {articles_path}")
                print(f"Summary CSV: {csv_path}")
                print(f"Analysis: {analysis_path}")
        else:
            print("\nNo articles to save")
            
        print("\n=== Enhanced PubMed Data Collection Complete ===\n")
            
    except Exception as e:
        print(f"\nERROR: Script failed with error: {str(e)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
