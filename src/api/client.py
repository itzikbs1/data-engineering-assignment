import requests
import time
from typing import List, Dict
import logging
from requests.exceptions import RequestException


class OpenAQClient:
    def __init__(self, base_url: str, api_key: str, limit: int = 100, delay: int = 1,
                 max_retries: int = 5, initial_retry_delay: int = 5):
        self.base_url = base_url
        self.headers = {
            'X-API-Key': api_key,
            'Accept': 'application/json'
        }
        self.limit = limit
        self.delay = delay
        self.max_retries = max_retries
        self.initial_retry_delay = initial_retry_delay

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _make_request(self, url: str, params: Dict = None) -> Dict:
        """Make HTTP request with retry logic for rate limits"""
        current_retry = 0
        while current_retry <= self.max_retries:
            try:
                response = requests.get(url, params=params, headers=self.headers)

                if response.status_code == 200:
                    return response.json()

                elif response.status_code == 429:  # Rate limit exceeded
                    if current_retry == self.max_retries:
                        raise Exception(f"Rate limit exceeded after {self.max_retries} retries")

                    retry_delay = self.initial_retry_delay * (2 ** current_retry)
                    self.logger.warning(
                        f"Rate limit hit. Waiting {retry_delay} seconds before retry {current_retry + 1}/{self.max_retries}"
                    )
                    time.sleep(retry_delay)
                    current_retry += 1
                    continue

                else:
                    raise Exception(f"Request failed with status code: {response.status_code}")

            except RequestException as e:
                if current_retry == self.max_retries:
                    raise Exception(f"Request failed after {self.max_retries} retries: {str(e)}")

                retry_delay = self.initial_retry_delay * (2 ** current_retry)
                self.logger.warning(
                    f"Request failed. Retrying in {retry_delay} seconds. Retry {current_retry + 1}/{self.max_retries}"
                )
                time.sleep(retry_delay)
                current_retry += 1
                continue

        raise Exception("Maximum retries exceeded")

    def generic_get(self, endpoint: str, params: Dict = None) -> List[Dict]:
        """Generic method for handling all API requests with pagination"""
        all_results = []
        page = 1

        while True:
            request_params = {
                'page': page,
                'limit': self.limit,
                **(params or {})
            }

            try:
                data = self._make_request(f"{self.base_url}/{endpoint}", request_params)
                results = data['results']
                all_results.extend(results)

                if len(results) < self.limit or page > 100:  # or page * self.limit >= int(
                    # str(data['meta'].get('found', '0')).replace('>', '')):
                    break

                page += 1
                time.sleep(self.delay)

            except Exception as e:
                raise Exception(f"Failed to fetch {endpoint}: {str(e)}")

        return all_results
