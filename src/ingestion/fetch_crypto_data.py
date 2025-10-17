import requests
import pandas as pd
import yaml
import logging
import os
from datetime import datetime
import time
from typing import Dict, List, Optional


class CryptoDataFetcher:
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the crypto data fetcher with configuration."""
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.base_url = self.config['api']['coingecko']['base_url']
        self.timeout = self.config['api']['coingecko']['timeout']
        self.tracked_coins = self.config['cryptocurrencies']['tracked_coins']

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")

    def _setup_logging(self):
        """Set up logging configuration."""
        log_config = self.config['logging']

        # Ensure logs directory exists
        os.makedirs(os.path.dirname(log_config['file']), exist_ok=True)

        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format=log_config['format'],
            handlers=[
                logging.FileHandler(log_config['file']),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _make_api_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make a request to the CoinGecko API with error handling."""
        url = f"{self.base_url}{endpoint}"

        try:
            self.logger.info(f"Making API request to: {url}")
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            # Respect rate limits
            time.sleep(60 / self.config['api']['coingecko']['rate_limit_per_minute'])

            return response.json()

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {url}: {e}")
            return None
        except ValueError as e:
            self.logger.error(f"JSON parsing failed for {url}: {e}")
            return None

    def fetch_market_data(self) -> Optional[List[Dict]]:
        """Fetch current market data for tracked cryptocurrencies."""
        endpoint = self.config['api']['coingecko']['endpoints']['market_data']

        # Convert coin list to comma-separated string
        coins_string = ','.join(self.tracked_coins)

        params = {
            'ids': coins_string,
            'vs_currency': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true',
            'include_last_updated_at': 'true'
        }

        self.logger.info(f"Fetching market data for {len(self.tracked_coins)} cryptocurrencies")
        return self._make_api_request(endpoint, params)

    def process_market_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """Process raw market data into a structured DataFrame."""
        if not raw_data:
            self.logger.warning("No market data to process")
            return pd.DataFrame()

        processed_data = []

        for coin in raw_data:
            try:
                processed_coin = {
                    'timestamp': datetime.now().isoformat(),
                    'coin_id': coin.get('id'),
                    'symbol': coin.get('symbol'),
                    'name': coin.get('name'),
                    'current_price': coin.get('current_price'),
                    'market_cap': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'total_volume': coin.get('total_volume'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'market_cap_change_24h': coin.get('market_cap_change_24h'),
                    'market_cap_change_percentage_24h': coin.get('market_cap_change_percentage_24h'),
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    'last_updated': coin.get('last_updated')
                }
                processed_data.append(processed_coin)

            except Exception as e:
                self.logger.error(f"Error processing coin data for {coin.get('id', 'unknown')}: {e}")
                continue

        df = pd.DataFrame(processed_data)
        self.logger.info(f"Processed data for {len(df)} cryptocurrencies")
        return df

    def save_to_csv(self, df: pd.DataFrame) -> str:
        """Save DataFrame to CSV file with timestamp."""
        if df.empty:
            self.logger.warning("No data to save")
            return ""

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"crypto_market_data_{timestamp}.csv"
        filepath = os.path.join("data", "raw", filename)

        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # Save to CSV
            df.to_csv(filepath, index=False)
            self.logger.info(f"Data saved to {filepath}")
            return filepath

        except Exception as e:
            self.logger.error(f"Error saving data to CSV: {e}")
            return ""

    def fetch_and_save_data(self) -> bool:
        """Main method to fetch and save crypto data."""
        try:
            self.logger.info("Starting crypto data fetch and save process")

            # Fetch market data
            raw_data = self.fetch_market_data()
            if not raw_data:
                self.logger.error("Failed to fetch market data")
                return False

            # Process data
            df = self.process_market_data(raw_data)
            if df.empty:
                self.logger.error("No data was processed")
                return False

            # Save to CSV
            filepath = self.save_to_csv(df)
            if not filepath:
                self.logger.error("Failed to save data")
                return False

            self.logger.info(f"Successfully completed data fetch and save. File: {filepath}")
            return True

        except Exception as e:
            self.logger.error(f"Unexpected error in fetch_and_save_data: {e}")
            return False


def main():
    """Main function to run the crypto data fetcher."""
    try:
        fetcher = CryptoDataFetcher()
        success = fetcher.fetch_and_save_data()

        if success:
            print("Crypto data fetch completed successfully!")
        else:
            print("Crypto data fetch failed. Check logs for details.")

    except Exception as e:
        print(f"Error initializing crypto data fetcher: {e}")


if __name__ == "__main__":
    main()