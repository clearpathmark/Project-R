import pandas as pd
import aiohttp
import asyncio
import logging

class WHODatabaseConnector:
    def __init__(self):
        self.base_url = "https://ghoapi.azureedge.net/api"
        self.entity_set = "NCD_DIABETES_PREVALENCE_CRUDE"
        self.countries = [{"name": "France", "code": "FRA"}, {"name": "Andorra", "code": "AND"}]
        self.logger = logging.getLogger("WHOConnector")
        logging.basicConfig(level=logging.INFO)

    async def fetch_data(self):
        """
        Fetch data from the WHO API and process the JSON response.
        """
        async with aiohttp.ClientSession() as session:
            all_data = []

            for country in self.countries:
                params = {"$filter": f"SpatialDim eq '{country['code']}'"}
                url = f"{self.base_url}/{self.entity_set}"
                try:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            records = data.get("value", [])
                            for record in records:
                                # Extract relevant fields
                                all_data.append({
                                    "Country": country["name"],
                                    "SpatialDim": record.get("SpatialDim"),
                                    "TimeDim": record.get("TimeDim"),
                                    "NumericValue": record.get("NumericValue"),
                                    "Low": record.get("Low"),
                                    "High": record.get("High"),
                                    "Value": record.get("Value"),
                                })
                        else:
                            self.logger.error(f"Error {response.status} for {country['name']} ({country['code']})")
                except Exception as e:
                    self.logger.error(f"Error processing data for {country['name']} ({country['code']}): {str(e)}")

            return all_data

    def save_to_csv(self, data, filename="diabetes_data.csv"):
        """
        Save processed data to a CSV file.
        """
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

async def main():
    connector = WHODatabaseConnector()
    data = await connector.fetch_data()
    if data:
        connector.save_to_csv(data)

if __name__ == "__main__":
    asyncio.run(main())

