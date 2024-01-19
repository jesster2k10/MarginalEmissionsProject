import click
import sys
import os
from math import ceil
from datetime import datetime
from dateutil.relativedelta import relativedelta
from enum import StrEnum
sys.path.insert(0, os.path.abspath('../')) 

import lib.semo.market_data as market_data
from lib.common import constants as k

OUT_DIR = k.RAW_DATA_DIR / 'semo'

@click.group()
def cli_collect():
    pass

@click.group()
def cli_sync():
    pass

@cli_collect.command()
@click.option('-s', '--start', required=False, help='The start date for the collection. Must be maximum 3 months ago. Defaults to 3 months ago')
@click.option('-e', '--end', help='The end date for collection. Defaults to today')
@click.option('-j', '--jurisdiction', default='ROI')
@click.argument('reports', nargs=-1, required=True)
def collect(start: str, end: str, reports: tuple[str], jurisdiction: str):
    for report in reports:
        if not report in market_data.SemoMarketReport._member_names_:
            raise Exception(f'Report "{report}" is not valid')
    reports = list(map(lambda x: market_data.SemoMarketReport[x], reports))
    three_months_ago = datetime.now() - relativedelta(months=3)

    if start:
        start_date = datetime.strptime(start, '%d/%m/%Y')
    else:
        start_date = three_months_ago
    start_date_diff = ceil((datetime.now() - start_date).days / 30)
    if start_date_diff > 4:
        raise Exception("The date difference has to be less than three months")
    
    if end:
        end_date = datetime.strptime(end, '%d/%m/%Y')
    else:
        end_date = datetime.now()
    click.clear()

    for report in reports:
        filename = f'{report.name}_{jurisdiction}.csv'.lower()
        df = market_data.fetch_market_report(
            report,
            start_date=start_date,
            end_date=end_date,
            jurisdiction=jurisdiction,
        ) 
        filename = OUT_DIR / filename  
        print(f'\nWriting output for report: {report.name} to file: {filename}\n')
        df.to_csv(filename)

@cli_sync.command()
def sync():
    pass

cli = click.CommandCollection(sources=[cli_collect, cli_sync])

if __name__ == '__main__':
    OUT_DIR.mkdir(parents=False,exist_ok=True)
    cli()