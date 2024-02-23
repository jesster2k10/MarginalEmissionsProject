import click
import sys
import os
import datetime
import pandas as pd
from tqdm import tqdm
sys.path.insert(0, os.path.abspath('../')) 

from lib.eirgrid.constants import Area
from lib.eirgrid.smart_grid import collect_smartgrid, query_smartgrid
from lib.common import constants as k
from lib.common.dates import format_datettime

OUT_DIR = k.RAW_DATA_DIR / 'eirgrid'

@click.group()
def cli_collect():
    pass

@click.group()
def cli_sync():
    pass

@cli_collect.command()
@click.option('-s', '--start', required=True, help='The start date in DD/MM/YYYY')
@click.option('-e', '--end', help='The end date in DD/MM/YYYY')
@click.option('-r', '--region', default='ROI')
@click.option('-ud', '--usedates', default=False, help='Should use dates in the filename instead')
@click.argument('areas', nargs=-1, required=True)
def collect(start: str, end: str, areas: tuple[str], region: str, usedates: bool):
    """
    Collects the historic data from the eirgrid smart grid dashbiard
    """    
    if len(areas) == 1 and areas[0] == 'all':
        areas = list(map(lambda x: x.name, [
            Area.co2emission,
            Area.co2intensity,
            Area.demandactual,
            Area.interconnection,
            Area.generationactual,
            Area.frequency,
            Area.windactual
        ]))
    for area in areas:
        if not area in Area._member_names_:
            raise Exception(f'Area "{area}" is not valid')
        
    if end:
        end_date = datetime.datetime.strptime(end, '%d/%m/%Y')
    else:
        end_date = datetime.datetime.now()    
    start_date = datetime.datetime.strptime(start, '%d/%m/%Y')

    click.clear()
    print(areas)
    for area in areas:
        if usedates:
            dateformat = '%d_%m_%y'
            filename = f'{area}--{start_date.strftime(dateformat)}--{end_date.strftime(dateformat)}.csv'
        else:
            filename = f'{area}_{region}.csv'

        df = collect_smartgrid(
            area=area,
            start_date=start_date,
            end_date=end_date,
            region=region
        )

        filename = OUT_DIR / filename
        print(f'\nWriting output for area: {area} to file: {filename}\n')
        df.to_csv(filename)

@cli_sync.command()
@click.option('-r', '--region', required=True)
@click.argument('areas', nargs=-1, required=True)
def sync(areas: tuple[str], region: str):
    """
    Fetches the most up-to-date data and syncs it
    """
    for area in areas:
        filename = OUT_DIR / f'{area}_{region}.csv'
        df = pd.read_csv(filename, index_col=0, parse_dates=True).dropna()
        df['EffectiveTime'] = pd.to_datetime(df['EffectiveTime'])
        last_updated = df.tail(1)['EffectiveTime'].item()
        print(last_updated)

        response = query_smartgrid({
            'area': area,
            'datefrom': format_datettime(last_updated),
            'dateto': format_datettime(datetime.datetime.now() - datetime.timedelta(30)),
            'region': region
        })
        df = pd.concat([df, response], ignore_index=True)
        df = df.drop_duplicates(keep='first', subset=['EffectiveTime'])
        df.to_csv(OUT_DIR / f'{area}_{region}_latest.csv')

cli = click.CommandCollection(sources=[cli_collect, cli_sync])

if __name__ == '__main__':
    OUT_DIR.mkdir(parents=False,exist_ok=True)
    cli()