# src/api/client.py
import requests
import time
from typing import Optional, Dict
from config.settings import settings
from config.logging_config import setup_logging

logger = setup_logging(__name__)

class OpenAQClient:
    def __init__(self):
        self.base_url = settings.API_BASE_URL
        self.headers = {
            'Accept': 'application/json',
            'X-API-Key': settings.API_KEY
        }
        self.max_retries = 3
        self.base_delay = 5

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}/{endpoint}",
                    headers=self.headers,
                    params=params
                )

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit
                    wait_time = self.base_delay * (attempt + 1)
                    logger.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error: API returned status code {response.status_code}")
                    return None

            except requests.RequestException as e:
                logger.error(f"Error making API request: {e}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.base_delay)

        return None

    def get_locations(self, limit: int = 100, page: int = 1) -> Optional[Dict]:
        return self._make_request('locations', {'limit': limit, 'page': page})

    def get_location_sensors(self, location_id: int) -> Optional[Dict]:
        return self._make_request(f'locations/{location_id}/sensors')

    def get_measurements(self, location_id: int) -> Optional[Dict]:
        return self._make_request(f'locations/{location_id}/latest')
# import requests
# import time
# from typing import Optional, Dict, List
# from config.settings import settings
# from config.logging_config import setup_logging
#
# logger = setup_logging(__name__)
#
# class OpenAQClient:
#     """Handles raw API communication"""
#
#     def __init__(self):
#         self.base_url = settings.API_BASE_URL
#         self.headers = {
#             'Accept': 'application/json',
#             'X-API-Key': settings.API_KEY
#         }
#         self.max_retries = 3
#         self.base_delay = 5
#
#     def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
#         """Generic method to make API requests with retry logic"""
#         for attempt in range(self.max_retries):
#             try:
#                 response = requests.get(
#                     f"{self.base_url}/{endpoint}",
#                     headers=self.headers,
#                     params=params
#                 )
#
#                 if response.status_code == 200:
#                     return response.json()
#                 elif response.status_code == 429:  # Rate limit
#                     wait_time = self.base_delay * (attempt + 1)
#                     logger.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
#                     time.sleep(wait_time)
#                     continue
#                 else:
#                     logger.error(f"Error: API returned status code {response.status_code}")
#                     return None
#
#             except requests.RequestException as e:
#                 logger.error(f"Error making API request: {e}")
#                 if attempt == self.max_retries - 1:
#                     return None
#                 time.sleep(self.base_delay)
#
#         return None
#
#     def get_locations(self, limit: int = 100, page: int = 1) -> Optional[Dict]:
#         """Get locations from API"""
#         return self._make_request('locations', {'limit': limit, 'page': page})
#         # return self._make_request(limit, page)
#
#     def get_location_sensors(self, location_id: int) -> Optional[Dict]:
#         """Get sensors for a location"""
#         return self._make_request(f'locations/{location_id}/sensors')
#
#     def get_measurements(self, location_id: int) -> Optional[Dict]:
#         """Get measurements for a location"""
#         return self._make_request(f'locations/{location_id}/latest')
#
#     def fetch_all_locations(self, limit: int = 100, page: int = 1) -> List[Dict]:
#         all_locations = []
#         # page = 1
#         # limit = 100
#         delay_between_requests = 1
#
#         while True:
#             # print(f"fetching page {page}...")
#             data = self.get_locations(limit, page)
#
#             if not data or 'results' not in data:
#                 break
#
#             locations_of_page = data['results']
#             # print(f"locations_of_page: {len(locations_of_page)}")
#             if not locations_of_page:
#                 break
#             all_locations.extend(locations_of_page)
#
#             if len(locations_of_page) < limit:  # last iteration.
#                 break
#
#             page += 1
#             time.sleep(delay_between_requests)  # Add delay between requests
#
#         return all_locations