from datetime import datetime, timedelta
import time
import requests
import json
from typing import Dict, Optional, List
import logging

API_KEY = 'dd4f85c5a1381de52161632b664322cf4bdb10fe24e37da6c745144b7eb018fb'

logger = logging.getLogger(__name__)


class OpenAQFetcher:
    def __init__(self, api_key: str, base_url: str = "https://api.openaq.org/v3"):
        self.base_url = base_url
        self.headers = {
            'Accept': 'application/json',
            'X-API-Key': api_key
        }
        # Define the time threshold for recent measurements (24 hours)
        self.recent_threshold = datetime.utcnow() - timedelta(hours=24)

    def fetch_locations_page(self, page: int, limit: int = 100) -> Optional[Dict]:
        """
        Fetch locations data from OpenAQ API
        Returns the JSON response or None if the request fails
        """

        max_retries = 3  # Maximum number of retry attempts
        base_delay = 5  # Base delay in seconds

        for attempt in range(max_retries):
            try:
                # Make GET request to the locations endpoint
                response = requests.get(f"{self.base_url}/locations", headers=self.headers,
                                        params={'limit': limit, 'page': page})

                # Check if request was successful
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # rate the max requests we can get
                    wait_time = base_delay * (attempt + 1)  # Increase delay with each retry
                    print(f"Rate limit hit. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue  # Try again
                else:
                    print(f"Error: API returned status code {response.status_code}")
                    return None

            except requests.RequestException as e:
                print(f"Error making API request: {e}")
                return None

    def fetch_all_locations(self) -> List[Dict]:
        all_locations = []
        page = 1
        limit = 100
        delay_between_requests = 1

        while True:
            # print(f"fetching page {page}...")
            data = self.fetch_locations_page(page, limit)

            if not data or 'results' not in data:
                break

            locations_of_page = data['results']
            # print(f"locations_of_page: {len(locations_of_page)}")
            if not locations_of_page:
                break
            all_locations.extend(locations_of_page)

            if len(locations_of_page) < limit:  # last iteration.
                break

            page += 1
            time.sleep(delay_between_requests)  # Add delay between requests

        return all_locations

    def is_location_active(self, location: Dict) -> bool:
        """Check if location has recent data (within last 48 hours)"""

        # print(f"line 77, location: {location}")

        if not location.get('datetimeLast'):
            return False

        last_update = datetime.strptime(
            location['datetimeLast']['utc'],
            "%Y-%m-%dT%H:%M:%SZ"
        )
        time_difference = datetime.utcnow() - last_update

        return time_difference <= timedelta(hours=48)

    def is_measurement_recent(self, measurement: Dict) -> bool:
        """Check if a measurement is recent (within last 24 hours)"""
        measurement_time = datetime.strptime(
            measurement['datetime']['utc'],
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return measurement_time >= self.recent_threshold

    def fetch_latest_measurements(self, location_id: int) -> Optional[Dict]:
        """
        Fetch latest measurements for a specific location
        """
        try:
            # Add date parameter for last 24 hours
            # date_from = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

            response = requests.get(
                f"{self.base_url}/locations/{location_id}/latest",
                headers=self.headers,
                # params={'date_from': date_from}
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('meta', {}).get('found', 0) > 0:
                    # Filter to keep only recent measurements
                    recent_results = [
                        result for result in data['results']
                        if self.is_measurement_recent(result)
                    ]
                    if recent_results:
                        data['results'] = recent_results
                        return data
                return None
            elif response.status_code == 429:  # Rate limit hit
                print(f"Rate limit hit for location {location_id}. Waiting 5 seconds...")
                time.sleep(5)
                return self.fetch_latest_measurements(location_id)  # Retry
            else:
                print(f"Error fetching measurements for location {location_id}: status code {response.status_code}")
                return None

        except requests.RequestException as e:
            print(f"Error making API request for location {location_id}: {e}")
            return None

    def fetch_all_locations_with_measurements(self, max_locations: int = None) -> List[Dict]:
        """
        Fetch all active locations and their latest measurements
        """
        all_locations = self.fetch_all_locations()
        active_locations = [loc for loc in all_locations if self.is_location_active(loc)]

        print(f"\nFound {len(active_locations)} active locations out of {len(all_locations)} total")

        # Limit the number of locations if specified
        if max_locations:
            active_locations = active_locations[:max_locations]

        locations_with_measurements = []
        total = len(active_locations)
        locations_with_data = 0  # Counter for locations with measurements

        for idx, location in enumerate(active_locations, 1):
            # print(f"\nFetching measurements for location {idx}/{total}: {location['name']}")
            # print(f"Last update: {location['datetimeLast']['utc']}")

            measurements = self.fetch_latest_measurements(location['id'])
            if measurements and 'results' in measurements and measurements['results']:
                location['latest_measurements'] = measurements['results']
                locations_with_data += 1
                # print(f"Found {len(measurements['results'])} recent measurements")
            else:
                location['latest_measurements'] = None
                # print("No recent measurements found")

            locations_with_measurements.append(location)
            time.sleep(1)  # Delay to avoid rate limits

            # Print progress every 10 locations
            if idx % 10 == 0:
                print(f"\nProgress: {idx}/{total} locations processed")
                # print(f"Locations with recent data: {locations_with_data}")
                # print("------------------------\n")

        print(f"\nFinal Summary:")
        print(f"Total active locations processed: {total}")
        print(f"Locations with recent measurements: {locations_with_data}")
        print(f"Percentage with data: {(locations_with_data / total) * 100:.2f}%")

        return locations_with_measurements
