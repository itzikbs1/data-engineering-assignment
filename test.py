import requests
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
import json

API_KEY = 'dd4f85c5a1381de52161632b664322cf4bdb10fe24e37da6c745144b7eb018fb'


@dataclass
class APIResponse:
    meta: Dict
    results: List[Dict]


class OpenAQv3Client:
    def __init__(self, api_key: str, base_url: str = "https://api.openaq.org/v3"):
        self.base_url = base_url
        self.headers = {
            'Accept': 'application/json',
            'X-API-Key': api_key
        }

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[APIResponse]:
        """Make API request and parse the response"""
        url = f"{self.base_url}{endpoint}"
        try:
            print(f"Making request to: {url}")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Print raw response for debugging
            print("\nRaw API Response:")
            print(json.dumps(data, indent=2))

            return APIResponse(
                meta=data.get('meta', {}),
                results=data.get('results', [])
            )
        except requests.exceptions.RequestException as e:
            print(f"Error making request to {endpoint}: {str(e)}")
            return None

    def get_locations(self, page: int = 1, limit: int = 100) -> Optional[APIResponse]:
        """Get locations with pagination"""
        return self._make_request("/locations", params={'page': page, 'limit': limit})

    def get_location(self, location_id: int) -> Optional[APIResponse]:
        """Get a specific location by ID"""
        return self._make_request(f"/locations/{location_id}")


def safe_get(data: Optional[Dict], *keys: str, default: Any = "N/A") -> Any:
    """Safely get nested dictionary values"""
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def explore_data():
    """Function to explore the API and display data in a readable format"""
    api_key = 'dd4f85c5a1381de52161632b664322cf4bdb10fe24e37da6c745144b7eb018fb'
    client = OpenAQv3Client(api_key)

    # Get locations
    response = client.get_locations(limit=1)
    if not response:
        print("No response received from the API")
        return

    print(f"\nFound {safe_get(response.meta, 'found', default=0)} total locations")
    print(
        f"Page {safe_get(response.meta, 'page', default=1)} of {safe_get(response.meta, 'limit', default=0)} per page")

    if not response.results:
        print("No locations found in the results")
        return

    # Process each location
    for location in response.results:
        print("\nLocation Details:")
        print(f"ID: {safe_get(location, 'id', default='No ID')}")
        print(f"Name: {safe_get(location, 'name', default='No Name')}")
        print(f"Country: {safe_get(location, 'country', 'name', default='Unknown Country')}")

        # Print coordinates if available
        latitude = safe_get(location, 'coordinates', 'latitude')
        longitude = safe_get(location, 'coordinates', 'longitude')
        if latitude != "N/A" and longitude != "N/A":
            print(f"Coordinates: {latitude}, {longitude}")
        else:
            print("Coordinates: Not available")

        # Print sensors
        sensors = safe_get(location, 'sensors', default=[])
        if sensors:
            print("\nSensors:")
            for sensor in sensors:
                display_name = safe_get(sensor, 'parameter', 'displayName', default='Unknown')
                param_name = safe_get(sensor, 'parameter', 'name', default='unknown')
                units = safe_get(sensor, 'parameter', 'units', default='no units')
                print(f"- {display_name} ({param_name}): {units}")
        else:
            print("\nNo sensors found")

        # Print date range
        first_date = safe_get(location, 'datetimeFirst', 'local', default='unknown')
        last_date = safe_get(location, 'datetimeLast', 'local', default='unknown')
        print(f"\nData Range: {first_date} to {last_date}")

        # Print provider and owner
        provider = safe_get(location, 'provider', 'name', default='Unknown Provider')
        owner = safe_get(location, 'owner', 'name', default='Unknown Owner')
        print(f"Provider: {provider}")
        print(f"Owner: {owner}")


def main():
    try:
        explore_data()
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("Please check your API key and try again")


if __name__ == "__main__":
    main()