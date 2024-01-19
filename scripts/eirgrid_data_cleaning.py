import sys
import click
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import pandas as pd
import numpy as np
import humps
sys.path.insert(0, os.path.abspath('../')) 

from tqdm import tqdm
from lib.common import constants as k

OUT_DIR = k.RAW_DATA_DIR / 'eirgrid'
# REGION = 'all'

def clean_interconnection():
    """
    We are only concerned with net interconnection
    """
    df = pd.read_csv(OUT_DIR / 'interconnection_all.csv', index_col=0)
    df = df.pivot_table(index='EffectiveTime', columns='FieldName', values='Value')
    df['INTER_EWIC'] = df['INTER_EWIC'].fillna(0)
    df['INTER_MOYLE'] = df['INTER_MOYLE'].fillna(0)
    mask = pd.isna(df['INTER_NET'])
    net_ic = df['INTER_EWIC'] - df['INTER_MOYLE']
    df.loc[mask, 'INTER_NET'] = net_ic[mask]
    df = df.reset_index()
    df = df.melt(id_vars='EffectiveTime', var_name='FieldName', value_name='Value')
    df = df.loc[df['FieldName'] == 'INTER_NET']
    df.to_csv(OUT_DIR / 'interconnection_net.csv')

def clean_frequency():
    """
    The frequency data is at five-minte intervals.
    Resample this to 15 minute intervals.
    """
    df = pd.read_csv(OUT_DIR / 'frequency_all.csv', index_col=0)
    df['EffectiveTime'] = pd.to_datetime(df['EffectiveTime'])
    df = df.set_index('EffectiveTime')
    df = df.resample('15T').asfreq()
    df = df.reset_index()
    df.dropna(inplace=True)
    df.to_csv(OUT_DIR / 'frequency_all_resampled.csv')

def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk['EffectiveTime'] = pd.to_datetime(chunk['EffectiveTime'])
    if 'Region' in chunk:
        unique_regions = len(chunk['Region'].unique())
        if unique_regions > 1:
            raise Exception('There are more than one regions in this file.')
        chunk = chunk.drop(columns='Region')

    df = chunk.pivot_table(
        index='EffectiveTime',
        columns='FieldName',
        values='Value',
        aggfunc='first'
    )
    df.columns = df.columns.to_series().apply(lambda x: humps.pascalize(x.lower()))
    return df

def process_file(file: Path) -> pd.DataFrame:
    df = pd.DataFrame()

    bar = tqdm(pd.read_csv(file, index_col=0, parse_dates=True, chunksize=600_000))
    bar.set_description(f'Processing {file.stem}')

    with ThreadPoolExecutor(max_workers=100) as executor:
        for chunk_pivot in executor.map(process_chunk, bar):
            df = pd.concat([df, chunk_pivot])
    return df

def clean():
    """
    Cleans the raw data files downlaoded using the `eirgrid_data_collection` script
    """

    files = list(map(
        lambda x: OUT_DIR / f'{x}.csv',
        [
            'frequency_all_resampled',
            'co2emission_all',
            'co2intensity_all',
            'demandactual_all',
            'generationactual_all',
            'interconnection_net',
            'windactual_all'
        ]
    ))

    click.clear()
    print(f'🔎 Reading {len(files)} files')

    df = pd.DataFrame()

    # Process files in parallel
    with ProcessPoolExecutor() as executor:
        for i, result in enumerate(list(tqdm(executor.map(process_file, files), total=len(files)))):
            if i == 0:
                df = result
            else:
                print(f'Applying join for {files[i].stem}')
                df = df.merge(result, on='EffectiveTime', how='outer')
    print(f'✅ Processed all frames')

    # Group by year extracted from 'EffectiveTime'
    df = df.reset_index()
    df = df.sort_values(by='EffectiveTime', ascending=False)
    print(df)
    print('🗂️ Writing complete output...')
    df.to_csv(k.CLEANED_DATA_DIR / f'eirgrid.csv')
    print(f'✅ Wrote complete output\n')

    df['Year'] = df['EffectiveTime'].dt.year
    year_groups = df.groupby('Year')

    # Write each year's data to a separate file
    for year, group in year_groups:
        group.drop(colrumns='Year', inplace=True)
        outdir = k.CLEANED_DATA_DIR / f'eirgrid_{year}.csv'
        
        ixs = np.array_split(group.index, 1000)  
        bar = tqdm(enumerate(ixs), total=len(ixs))
        for ix, subset in bar:
            bar.set_description(f'🗂️ Writing output for {year} to {outdir.stem}')
            if ix == 0:
                group.loc[subset].to_csv(outdir, mode='w', index=False)
            else:
                group.loc[subset].to_csv(outdir, header=None, mode='a', index=False)
    
if __name__ == '__main__':
    clean_interconnection()
    # clean_frequency()
    clean()