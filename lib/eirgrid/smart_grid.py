from lib.common import constants as k
from lib.common import dates
from .constants import Area, DASHBOARD_ENDPOINT
from typing import TypedDict
from urllib.parse import urlencode

import pandas as pd
import datetime
import requests
import json

class SmartGridQuery(TypedDict):
    region: str
    area: Area
    datefrom: str
    dateto: str

class SmartGridRow(TypedDict):
    EffectiveTime: str
    Region: str
    FieldName: str
    Value: str

class SmartGridResponse(TypedDict):
    Rows: list[SmartGridRow]
    LastUpdated: str
    Status: str
    ErrorMessage: str | None

def response_to_dataframe(response: dict) -> pd.DataFrame:
    """Parses a smart grid response to a data frame"""
    df = pd.DataFrame(response['Rows'])
    df['EffectiveTime'] = pd.to_datetime(df['EffectiveTime'])
    df = df.sort_values(by='EffectiveTime', ascending=False).reset_index(drop=True)
    return df

def query_smartgrid(query: SmartGridQuery = None):
    """Sends a query to the EirGrid smart grid dasboard"""
    url = f'{DASHBOARD_ENDPOINT}?{urlencode(query)}'
    print(f'Fetching {url}')
    response = requests.get(url)
    response = json.loads(response.text)
    if response['ErrorMessage'] is not None:
        raise Exception('Failed to fetch endpoint')
    return response_to_dataframe(response)

def collect_smartgrid(
    area: Area | str,
    start_date: datetime.datetime,
    end_date = datetime.datetime.now(),
    region: str = 'ROI',
) -> pd.DataFrame:
    """Runs a query to the smart grid dashboard across a given date range.
       The returned output is a pandas data frame"""
    df = pd.DataFrame()

    if area is Area:
        area = area.name
    
    for date in dates.monthrange(start_date, end_date):
        from_date = date
        to_date = dates.next_month(from_date)

        print('=====================')
        print(f'Latest {area}/{region}: {from_date} to {to_date}')
        print('=====================')

        response = query_smartgrid({
            'region': region,
            'area': area,
            'datefrom': dates.format_datettime(from_date),
            'dateto': dates.format_datettime(to_date)
        })

        df = pd.concat([df, response])

    df['EffectiveTime'] = pd.to_datetime(df['EffectiveTime'])
    return df.drop_duplicates(subset='EffectiveTime') \
             .sort_values(by='EffectiveTime', ascending=True) \
             .reset_index(drop=True)