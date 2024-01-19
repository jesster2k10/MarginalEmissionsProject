import sys
import click
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../')) 

from tqdm import tqdm
from lib.common import constants as k
from lib.semo import units as semo_units

OUT_DIR = k.PROCESSED_DATA_DIR / 'semo'

def process_meter_data():
    """
    Processes the semo market data files
    """

    file = k.RAW_DATA_DIR / 'semo' / 'meter_all.csv'
    bar = tqdm(pd.read_csv(file, index_col=0, parse_dates=True, chunksize=600_000))
    bar.set_description(f'Processing {file.stem}')
    df = pd.DataFrame()
    
    def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
        df = chunk[['StartTime', 'ResourceName', 'ResourceType', 'MeteredMW']]
        df = df.loc[df['ResourceType'] == 'GEN']
        df['StartTime'] = pd.to_datetime(df['StartTime'])
        df = pd.merge(df, semo_units.generators(), on='ResourceName', how='left')
        df = df.drop(columns=['FuelType', 'ResourceName', 'ResourceType'])
        return df.pivot_table(
            index='StartTime',
            columns='FuelKind',
            values='MeteredMW',
            aggfunc='sum'
        ).reset_index().rename_axis(None, axis=1)

    with ThreadPoolExecutor() as executor:
        for chunk_pivot in executor.map(process_chunk, bar):
            df = pd.concat([df, chunk_pivot])
    df = df.sort_values(by='StartTime', ascending=False).reset_index(drop=True)
    df = df.rename(columns={
        'COAL_GAS': 'Fuel_CoalGas',
        'OTHER': 'Fuel_Other',
        'RENEWABLES': 'Fuel_Renewables',
        'MIXED': 'Fuel_Mixed'
    })
    print(df)
    print('🗂️ Writing complete output...')
    df.to_csv(k.PROCESSED_DATA_DIR / f'fuel_mix.csv')
    print(f'✅ Wrote complete output\n')

def process_price_data():
    """
    Processes the semo price data information
    """
    pass

if __name__ == '__main__':
    process_meter_data()
    process_price_data()