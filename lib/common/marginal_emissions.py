import pandas as pd
import numpy as np

def compute_mef(frame: pd.DataFrame, clean=True) -> pd.DataFrame:
    """
    Calculates the marginal emissions for a given data frame
    """
    df = frame.copy()
    df['MarginalEmissions'] = (
        ((df['Co2Intensity'].shift(-1)*df['GenExp'].shift(-1)) - 
        (df['Co2Intensity']*df['GenExp'])) / (df['GenExp'].shift(-1) - df['GenExp'])
    )

    if clean:
        df = df.dropna() \
            .replace([np.inf, -np.inf], np.nan) \
            .ffill()
    return df