from enum import StrEnum
from typing import TypedDict
from datetime import datetime

import requests
import json
import urllib
import pandas as pd

SEMO_MARKET_URL = 'https://reports.sem-o.com/api/v1/dynamic'

class SemoMarketReport(StrEnum):
    Meter = 'BM-086'
    Price = 'BM-026'
    Exchange = 'BM-084'
    AggregatedContractedGeneration = 'BM-098'
    AggregatedContractedDemand = 'BM-099'
    AggregatedContractedWind = 'BM-100'

class SemoMarketQuery(TypedDict):
    Jurisdiction: str
    sort_by: str
    order_by: str
    page_size: int
    page: int
    StartTime: str

def fetch_market_report(
    report: SemoMarketReport,
    start_date: datetime,
    end_date: datetime,
    jurisdiction: str = 'ROI',
    order_by: str = 'ASC',
    sort_by: str = 'StartTime',
):
    """
    Returns a data frame of the results from the SEMO dynamic market report.
    Will loop through until all the pages have been fetched.
    """
    build_query = lambda page: {
        'Jurisdiction': jurisdiction,
        'sort_by': sort_by,
        'order_by': order_by,
        'page_size': 5000,
        'page': page,
        'StartTime': f'>={start_date.isoformat(timespec="seconds")}<={end_date.isoformat(timespec="seconds")}'
    }
    has_next_page = True
    current_page  = 0
    total_pages   = 0

    data = []

    while has_next_page:
        current_page = current_page + 1
        query = build_query(current_page)
        if jurisdiction.lower() == 'all':
            del query['Jurisdiction']
        url = f'{SEMO_MARKET_URL}/{report.value}?{urllib.parse.urlencode(query)}'
        print(f'Page #{current_page}: Fetching {url}')
        print(query)
        response = requests.get(url)
        response = json.loads(response.text)
        pagination = response['pagination']
        data = data + response['items']

        if 'totalPages' in pagination:
            total_pages = pagination['totalPages']
            print(f'There are {total_pages} page(s)')

        page_delta = max(total_pages - current_page, 0)
        print(f'There are {page_delta} page(s) left to fetch\n')
        has_next_page = page_delta > 0
            
    return pd.DataFrame(data)
