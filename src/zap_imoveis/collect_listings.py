import logging
import json
import sys
import time
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from functools import cached_property
from urllib.parse import urlencode

from curl_cffi import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_result,
)

from zap_imoveis.account_fields import ACCOUNT_FIELDS
from zap_imoveis.listing_fields import LISTING_FIELDS
from zap_imoveis.query_builder import build_query_fields, build_search_query
from zap_imoveis.search_params import (
    NeighborhoodSearchParams,
    get_address_search_params,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

FIREFOX_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"
)


@dataclass
class APIParams:
    """Container for API request parameters."""

    device_id: str
    page: int


menino_deus = NeighborhoodSearchParams(
    name="Menino Deus",
    latitude="-30.05444",
    longitude="-51.222427",
)

cidade_baixa = NeighborhoodSearchParams(
    name="Cidade Baixa", latitude="-30.040167", longitude="-51.222861"
)

centro_historico = NeighborhoodSearchParams(
    name="Centro Historico",
    alt_name="Centro Histórico",
    latitude="-30.030804",
    longitude="-51.227824",
)

sarandi = NeighborhoodSearchParams(
    name="Sarandi", latitude="-29.986181", longitude="-51.129206"
)


def is_retriable_status(response: requests.Response) -> bool:
    """Check if the response status code indicates we should retry."""
    return response.status_code == 424 or response.status_code >= 500


def log_retry(retry_state):
    """Log retry attempts."""
    if retry_state.outcome.failed:
        exception = retry_state.outcome.exception()
        logger.warning(
            f"Retry attempt {retry_state.attempt_number} failed: {str(exception)}. "
            f"Retrying in {retry_state.next_action.sleep} seconds"
        )


class ZapImoveisDataRetriever:
    items_per_page = 110

    # API configuration
    api_base_url = "https://glue-api.zapimoveis.com.br/v2/listings"

    query_structure = build_search_query(
        listing_fields=LISTING_FIELDS, account_fields=ACCOUNT_FIELDS
    )
    include_fields = f"{build_query_fields(query_structure)})"

    @cached_property
    def session(self) -> requests.Session:
        """Create and configure a curl_cffi session."""
        return requests.Session()

    def _build_url(
        self, api_params: APIParams, neighorhood_params: NeighborhoodSearchParams
    ) -> str:
        """Build API URL with query parameters."""

        search_params = {
            "user": api_params.device_id,
            "portal": "ZAP",
            "categoryPage": "RESULT",
            "business": "SALE",
            "listingType": "USED",
            "size": self.items_per_page,
            "topoFixoSize": "0",
            "superPremiumSize": "0",
            "developmentsSize": "4",
            "from": str((api_params.page - 1) * self.items_per_page),
            "page": str(api_params.page),
            "viewport": "null",
            "images": "webp",
            "__zt": "mtc:deduplication2023",
        }
        address_search_params = get_address_search_params(neighorhood_params)
        params = {
            **search_params,
            **address_search_params,
            "viewport": "null",
            "includeFields": self.include_fields,
        }
        return f"{self.api_base_url}?{urlencode(params)}"

    def _get_headers(self, device_id: str) -> dict[str, str]:
        """Get headers for the API request."""
        return {
            "User-Agent": FIREFOX_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,pt-BR;q=0.8,pt;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "X-Domain": ".zapimoveis.com.br",
            "X-DeviceId": device_id,
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_result(is_retriable_status),
        before_sleep=log_retry,
    )
    def get_listings(self, neighborhood: NeighborhoodSearchParams, page: int):
        """Get listings from the zapimoveis API."""
        device_id = str(uuid.uuid4())
        api_params = APIParams(device_id=device_id, page=page)
        url = self._build_url(api_params, neighborhood)
        headers = self._get_headers(device_id)
        response = self.session.get(url, headers=headers)
        return response


if __name__ == "__main__":
    retriever = ZapImoveisDataRetriever()
    neighborhoods = [menino_deus, cidade_baixa, centro_historico, sarandi]
    with ExitStack() as stack:
        files = {}
        for neighborhood in neighborhoods:
            file_name = f"{neighborhood.name}.json"
            files[neighborhood.name] = stack.enter_context(open(file_name, "a"))
        for neighborhood in neighborhoods:
            ids = set()
            exhausted = False
            for i in range(1, 501):
                response = retriever.get_listings(neighborhood, page=i)
                time.sleep(2)
                search_results = response.json()
                if "search" not in search_results:
                    print(
                        f"Search in neighborhood {neighborhood.name} "
                        f"exhausted at page {i}. Response: {search_results}"
                    )
                    exhausted = True
                    break
                listings = search_results["search"]["result"]["listings"]
                listings_file = files[neighborhood.name]
                for listing in listings:
                    if id_ := listing["listing"]["id"] in ids:
                        raise ValueError(f"{id_} is repeated!")
                    json.dump(listing, listings_file)
                    listings_file.write("\n")
            if not exhausted:
                print(
                    f"Search in neighborhood {neighborhood.name} "
                    f"finished at page {i}."
                )
