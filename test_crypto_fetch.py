#!/usr/bin/env python3
"""
Test script for fetch_crypto_data.py
Verifies API connection, data fetching, CSV creation, and data validation.
"""

import os
import sys
import pandas as pd
import requests
import glob
from datetime import datetime
import time

# Add src to path for imports
sys.path.insert(0, 'src')

from ingestion.fetch_crypto_data import CryptoDataFetcher


class CryptoDataFetcherTest:
    def __init__(self):
        """Initialize the test class."""
        self.test_results = {
            'api_connection': False,
            'data_fetching': False,
            'csv_creation': False,
            'data_validation': False
        }
        self.expected_columns = [
            'timestamp', 'coin_id', 'symbol', 'name', 'current_price',
            'market_cap', 'market_cap_rank', 'total_volume', 'price_change_24h',
            'price_change_percentage_24h', 'market_cap_change_24h',
            'market_cap_change_percentage_24h', 'circulating_supply',
            'total_supply', 'max_supply', 'last_updated'
        ]

    def test_api_connection(self):
        """Test if CoinGecko API is accessible."""
        print("🔗 Testing API connection...")

        try:
            # Test basic API connectivity
            response = requests.get(
                "https://api.coingecko.com/api/v3/ping",
                timeout=10
            )

            if response.status_code == 200:
                print("✅ API connection successful")
                self.test_results['api_connection'] = True
                return True
            else:
                print(f"❌ API connection failed with status code: {response.status_code}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ API connection failed: {e}")
            return False

    def test_data_fetching(self):
        """Test if data can be fetched from the API."""
        print("\n📊 Testing data fetching...")

        try:
            # Initialize fetcher
            fetcher = CryptoDataFetcher()

            # Fetch market data
            raw_data = fetcher.fetch_market_data()

            if raw_data and isinstance(raw_data, list) and len(raw_data) > 0:
                print(f"✅ Data fetching successful - retrieved {len(raw_data)} cryptocurrencies")
                self.test_results['data_fetching'] = True
                return raw_data
            else:
                print("❌ Data fetching failed - no data returned")
                return None

        except Exception as e:
            print(f"❌ Data fetching failed with error: {e}")
            return None

    def test_csv_creation(self):
        """Test if CSV file is created correctly."""
        print("\n💾 Testing CSV file creation...")

        try:
            # Get initial file count
            initial_files = glob.glob("data/raw/crypto_market_data_*.csv")
            initial_count = len(initial_files)

            # Initialize fetcher and run full process
            fetcher = CryptoDataFetcher()
            success = fetcher.fetch_and_save_data()

            if not success:
                print("❌ CSV creation failed - fetch_and_save_data returned False")
                return None

            # Check if new file was created
            new_files = glob.glob("data/raw/crypto_market_data_*.csv")
            new_count = len(new_files)

            if new_count > initial_count:
                # Get the newest file
                newest_file = max(new_files, key=os.path.getctime)
                file_size = os.path.getsize(newest_file)

                print(f"✅ CSV file created successfully: {newest_file}")
                print(f"   File size: {file_size} bytes")

                self.test_results['csv_creation'] = True
                return newest_file
            else:
                print("❌ CSV creation failed - no new file found")
                return None

        except Exception as e:
            print(f"❌ CSV creation failed with error: {e}")
            return None

    def test_data_validation(self, csv_file=None):
        """Test if the data in CSV has expected structure and content."""
        print("\n🔍 Testing data validation...")

        try:
            # If no file provided, get the most recent one
            if not csv_file:
                csv_files = glob.glob("data/raw/crypto_market_data_*.csv")
                if not csv_files:
                    print("❌ Data validation failed - no CSV files found")
                    return False
                csv_file = max(csv_files, key=os.path.getctime)

            # Read the CSV file
            df = pd.read_csv(csv_file)

            # Test 1: Check if file is not empty
            if df.empty:
                print("❌ Data validation failed - CSV file is empty")
                return False

            print(f"✅ CSV file contains {len(df)} rows")

            # Test 2: Check for expected columns
            missing_columns = set(self.expected_columns) - set(df.columns)
            extra_columns = set(df.columns) - set(self.expected_columns)

            if missing_columns:
                print(f"❌ Missing expected columns: {missing_columns}")
                return False

            if extra_columns:
                print(f"ℹ️  Extra columns found: {extra_columns}")

            print("✅ All expected columns present")

            # Test 3: Check for data quality
            required_numeric_columns = ['current_price', 'market_cap', 'total_volume']
            for col in required_numeric_columns:
                if col in df.columns:
                    non_null_count = df[col].notna().sum()
                    if non_null_count == 0:
                        print(f"❌ Column {col} has no valid data")
                        return False

            print("✅ Key numeric columns have valid data")

            # Test 4: Check timestamp format
            try:
                pd.to_datetime(df['timestamp'].iloc[0])
                print("✅ Timestamp format is valid")
            except:
                print("❌ Invalid timestamp format")
                return False

            # Test 5: Check for expected cryptocurrencies
            tracked_coins = [
                'bitcoin', 'ethereum', 'tether', 'bnb', 'solana', 'xrp',
                'usd-coin', 'cardano', 'tron', 'avalanche-2'
            ]

            found_coins = df['coin_id'].tolist()
            expected_found = sum(1 for coin in tracked_coins[:10] if coin in found_coins)

            if expected_found >= 5:  # At least 5 of the top 10 should be found
                print(f"✅ Found {expected_found} of top 10 expected cryptocurrencies")
            else:
                print(f"⚠️  Only found {expected_found} of top 10 expected cryptocurrencies")

            self.test_results['data_validation'] = True
            return True

        except Exception as e:
            print(f"❌ Data validation failed with error: {e}")
            return False

    def run_all_tests(self):
        """Run all tests and provide summary."""
        print("🚀 Starting Crypto Data Fetcher Tests")
        print("=" * 50)

        start_time = time.time()

        # Run tests
        api_ok = self.test_api_connection()

        if api_ok:
            data_ok = self.test_data_fetching()
            csv_file = self.test_csv_creation()
            validation_ok = self.test_data_validation(csv_file)
        else:
            print("\n⚠️  Skipping remaining tests due to API connection failure")

        # Summary
        print("\n" + "=" * 50)
        print("📋 TEST SUMMARY")
        print("=" * 50)

        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name.replace('_', ' ').title():.<30} {status}")

        total_passed = sum(self.test_results.values())
        total_tests = len(self.test_results)

        end_time = time.time()
        duration = end_time - start_time

        print(f"\nTests passed: {total_passed}/{total_tests}")
        print(f"Test duration: {duration:.2f} seconds")

        if total_passed == total_tests:
            print("\n🎉 ALL TESTS PASSED! Crypto data fetcher is working correctly.")
            return True
        else:
            print(f"\n⚠️  {total_tests - total_passed} tests failed. Please check the issues above.")
            return False


def main():
    """Main function to run the tests."""
    tester = CryptoDataFetcherTest()
    success = tester.run_all_tests()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()