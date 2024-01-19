import pandas as pd
from lib.common import constants as k
from .common import process_raw_dataframe

def system_data(year: int) -> pd.DataFrame:
    """Returns the system profile data for a given year"""
    filename = k.CLEANED_DATA_DIR / f'eirgrid_{year}.csv'
    df = pd.read_csv(filename, index_col=None, parse_dates=True)
    return process_raw_dataframe(df)

def system_profile(day: int, month: int, year: int) -> pd.DataFrame:
    """Returns the system profile data for a given day"""
    df = system_data(year)
    return df.loc[(df['EffectiveTime'].dt.month == month) & (df['EffectiveTime'].dt.day == day)]

def system() -> pd.DataFrame:
    """Returns the entire system profile data available"""
    filename = k.CLEANED_DATA_DIR / f'eirgrid.csv'
    df = pd.read_csv(filename, index_col=0, parse_dates=True)
    return process_raw_dataframe(df)