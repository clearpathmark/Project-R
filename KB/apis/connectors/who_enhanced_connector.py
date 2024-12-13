import pandas as pd
import aiohttp
import asyncio
import logging
import json
from pathlib import Path
from datetime import datetime
import os

class WHOEnhancedConnector:
    def __init__(self, config_path: str = "../config/who_config.json"):
        self.base_url = "https://ghoapi.azureedge.net/api"
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        
        # Define data categories and their indicators
        self.indicators = {
            "prevalence": [
                "NCD_DIABETES_PREVALENCE_CRUDE",
                "NCD_DIABETES_PREVALENCE_AGESTD"
            ],
            "treatment": [
                "NCD_DIABETES_TREATMENT_CRUDE",
                "NCD_DIABETES_TREATMENT_AGESTD"
            ],
            "risk_factors": [
                "NCD_BMI_30A",  # Obesity
                "NCD_HYP_PREVALENCE_A",  # Hypertension
                "NCD_CCS_DIABETES_MEDICINES"  # Access to medicines
            ]
        }

    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {}

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("WHOConnector")
        handler = logging.FileHandler('who_connector.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    async def fetch_indicator_data(self, session: aiohttp.ClientSession, indicator: str, country: dict) -> list:
        """Fetch data for a specific indicator and country"""
        url = f"{self.base_url}/{indicator}"
        params = {
            "$filter": f"SpatialDim eq '{country['code']}'",
            "$select": "SpatialDim,TimeDim,Value,NumericValue,Low,High,Dim1,Dim2"
        }
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    records = data.get("value", [])
                    return [{
                        "Country": country["name"],
                        "CountryCode": country["code"],
                        "Indicator": indicator,
                        "Year": record.get("TimeDim"),
                        "Value": record.get("Value"),
                        "NumericValue": record.get("NumericValue"),
                        "LowEstimate": record.get("Low"),
                        "HighEstimate": record.get("High"),
                        "Gender": record.get("Dim1"),
                        "AgeGroup": record.get("Dim2"),
                        "CollectedDate": datetime.now().strftime("%Y-%m-%d")
                    } for record in records]
                else:
                    self.logger.error(f"Error {response.status} for {country['name']} - {indicator}")
                    return []
        except Exception as e:
            self.logger.error(f"Error fetching {indicator} for {country['name']}: {e}")
            return []

    async def fetch_all_data(self):
        """Fetch data for all indicators and countries"""
        async with aiohttp.ClientSession() as session:
            all_data = []
            total_countries = len(self.config['countries'])
            
            for idx, country in enumerate(self.config['countries'], 1):
                print(f"Processing country {idx}/{total_countries}: {country['name']}")
                
                for category, indicators in self.indicators.items():
                    for indicator in indicators:
                        try:
                            data = await self.fetch_indicator_data(session, indicator, country)
                            all_data.extend(data)
                            print(f"  - Collected {len(data)} records for {indicator}")
                        except Exception as e:
                            self.logger.error(f"Error processing {indicator} for {country['name']}: {e}")
                
                # Small delay to respect API limits
                await asyncio.sleep(0.5)
            
            return all_data

    def save_data(self, data: list, category: str = "all"):
        """Save collected data to files"""
        if not data:
            print("No data to save")
            return

        # Create timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure output directory exists
        output_dir = Path("../../epidemiology/who_data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save as CSV
        df = pd.DataFrame(data)
        csv_path = output_dir / f"who_diabetes_{category}_{timestamp}.csv"
        df.to_csv(csv_path, index=False)
        print(f"Saved CSV to: {csv_path}")

        # Save as JSON
        json_path = output_dir / f"who_diabetes_{category}_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Saved JSON to: {json_path}")

        # Generate summary statistics
        summary = {
            "total_records": len(df),
            "countries": df["Country"].nunique(),
            "indicators": df["Indicator"].nunique(),
            "year_range": {
                "start": df["Year"].min(),
                "end": df["Year"].max()
            },
            "collection_date": timestamp,
            "value_statistics": {
                "mean": df["NumericValue"].mean(),
                "median": df["NumericValue"].median(),
                "min": df["NumericValue"].min(),
                "max": df["NumericValue"].max()
            }
        }

        # Save summary
        summary_path = output_dir / f"who_diabetes_{category}_{timestamp}_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Saved summary to: {summary_path}")

        return {
            "csv_path": str(csv_path),
            "json_path": str(json_path),
            "summary_path": str(summary_path)
        }

async def main():
    print("\n=== Starting Enhanced WHO Data Collection ===\n")
    
    try:
        connector = WHOEnhancedConnector()
        data = await connector.fetch_all_data()
        
        if data:
            print(f"\nCollected {len(data)} total records")
            file_paths = connector.save_data(data)
            print("\nFiles saved successfully:")
            for key, path in file_paths.items():
                print(f"{key}: {path}")
        else:
            print("No data collected")
            
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n=== WHO Data Collection Complete ===\n")

if __name__ == "__main__":
    asyncio.run(main())