import pandas as pd

def process_raw_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    """Used to process the .csv data frame. Ensures correct data structures"""
    df = frame.copy()
    df['EffectiveTime'] = pd.to_datetime(df['EffectiveTime'])
    df = df.sort_values(by='EffectiveTime', ascending=False).reset_index(drop=True)
    return df