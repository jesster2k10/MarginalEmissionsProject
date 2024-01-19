import pandas as pd

def compute_mef(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the marginal emissions for a given data frame
    """
    df = frame.copy()
    df['MarginalEmissions'] = (
        ((df['Co2Intensity'].shift(-1)*df['GenExp'].shift(-1)) - 
        (df['Co2Intensity']*df['GenExp'])) / (df['GenExp'].shift(-1) - df['GenExp'])
    )
    return df