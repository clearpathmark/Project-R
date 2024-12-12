import os
import json
from typing import Dict, List
import requests
from pathlib import Path

class ProjectSetup:
    def __init__(self, base_path: str = "Project-R"):
        self.base_path = Path(base_path)
        self.kb_path = self.base_path / "KB"
        
    def create_directory_structure(self):
        """Create the basic directory structure"""
        directories = [
            # Epidemiology
            "KB/epidemiology/edeg_reports",
            "KB/epidemiology/who_data",
            "KB/epidemiology/easd_archives",
            "KB/epidemiology/national_registries",
            
            # Research Papers
            "KB/research_papers/pubmed",
            "KB/research_papers/scopus",
            "KB/research_papers/web_of_science",
            "KB/research_papers/metadata",
            
            # Clinical Studies
            "KB/clinical_studies/glycemic_variability",
            "KB/clinical_studies/technology_implementation",
            "KB/clinical_studies/patient_experience",
            
            # Technology
            "KB/technology/cgm_systems",
            "KB/technology/insulin_pumps",
            "KB/technology/integration_studies",
            
            # APIs
            "KB/apis/connectors",
            "KB/apis/processors",
            "KB/apis/config"
        ]
        
        for directory in directories:
            path = self.base_path / directory
            path.mkdir(parents=True, exist_ok=True)
            # Create README.md in each directory
            self._create_readme(path)
    
    def _create_readme(self, path: Path):
        """Create README.md files with appropriate content"""
        readme_content = self._get_readme_content(path)
        readme_path = path / "README.md"
        readme_path.write_text(readme_content)
    
    def _get_readme_content(self, path: Path) -> str:
        """Generate appropriate README content based on directory path"""
        path_parts = path.parts
        if "epidemiology" in path_parts:
            return self._epidemiology_readme()
        elif "research_papers" in path_parts:
            return self._research_papers_readme()
        elif "clinical_studies" in path_parts:
            return self._clinical_studies_readme()
        elif "technology" in path_parts:
            return self._technology_readme()
        elif "apis" in path_parts:
            return self._apis_readme()
        return self._default_readme()
    
    def _epidemiology_readme(self) -> str:
        return """# Epidemiology Data Collection

## Data Sources
1. European Diabetes Epidemiology Group (EDEG) Reports
2. WHO European Region Diabetes Database
3. European Association for the Study of Diabetes (EASD)
4. National Health Registries

## Data Structure
- Source Information
- Collection Date
- Geographic Coverage
- Key Metrics
- Update Frequency
"""

    def _research_papers_readme(self) -> str:
        return """# Research Papers Repository

## Database Sources
- PubMed Central
- Scopus
- Web of Science
- ScienceDirect
- Cochrane Library

## Search Criteria
- Keywords: "Type 1 diabetes", "Type 2 diabetes", "Europe", "CGM", "Insulin Pump"
- Date Range: 2015-2024
- Focus: Clinical trials, systematic reviews, meta-analyses
"""

    def _clinical_studies_readme(self) -> str:
        return """# Clinical Studies Database

## Categories
1. Glycemic Variability Studies
2. Technology Implementation Research
3. Patient Experience Analysis

## Data Collection Template
- Study Title
- Authors
- Publication Date
- Methodology
- Key Findings
- Technology Used
- Patient Demographics
"""

    def _technology_readme(self) -> str:
        return """# Technology Assessment Database

## Focus Areas
1. CGM Systems
   - Accuracy Analysis
   - User Experience
   - Integration Capabilities

2. Insulin Pumps
   - System Features
   - Clinical Outcomes
   - Patient Feedback

3. Integration Studies
   - Interoperability
   - Data Management
   - Clinical Impact
"""

    def _apis_readme(self) -> str:
        return """# API Integration Framework

## Components
1. Connectors
   - PubMed API
   - Scopus API
   - WHO Database API

2. Data Processors
   - Document Parsing
   - Data Standardization
   - Quality Control

3. Configuration
   - API Keys
   - Rate Limits
   - Data Schemas
"""

    def _default_readme(self) -> str:
        return """# Documentation

## Overview
This directory contains project documentation and data.

## Contents
- Data files
- Documentation
- Analysis results
"""

    def create_api_config(self):
        """Create API configuration files"""
        config = {
            "api_endpoints": {
                "pubmed": {
                    "base_url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
                    "search_endpoint": "/esearch.fcgi",
                    "fetch_endpoint": "/efetch.fcgi",
                    "parameters": {
                        "db": "pubmed",
                        "retmode": "json",
                        "retmax": 100
                    }
                },
                "scopus": {
                    "base_url": "https://api.elsevier.com",
                    "search_endpoint": "/content/search/scopus",
                    "parameters": {
                        "view": "COMPLETE"
                    }
                },
                "who_diabetes": {
                    "base_url": "https://gateway.euro.who.int/en/datasets",
                    "endpoint": "/diabetes-country-profiles-2016"
                }
            },
            "search_parameters": {
                "date_range": {
                    "start": "2015",
                    "end": "2024"
                },
                "keywords": [
                    "Type 1 diabetes",
                    "Type 2 diabetes",
                    "Europe",
                    "CGM",
                    "Insulin Pump",
                    "Unmet Needs"
                ]
            }
        }
        
        config_path = self.base_path / "KB/apis/config/config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(json.dumps(config, indent=2))

def main():
    setup = ProjectSetup()
    setup.create_directory_structure()
    setup.create_api_config()

if __name__ == "__main__":
    main()