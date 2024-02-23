import pandas as pd
import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler

class GreyRelationalGrade():
    def __init__(self, grc_target, columns):
        self.grc_target = grc_target
        self.columns = columns
    
    def df(self) -> pd.DataFrame:
        return pd.DataFrame(self.grc_target, columns=self.columns)
    
    def mean(self):
        return self.df().mean().sort_values(ascending=False)

def dt_encode(df: pd.DataFrame, column: str) -> pd.DataFrame:
  """Encodes a date time series to a sine/cosine representation"""
  timestamp_s = df[column].map(datetime.datetime.timestamp)
  day_s = 24 * 60 * 60
  features = df.drop(columns=[column])
  features[f'{column}_sin'] = (np.sin(timestamp_s * (2*np.pi/day_s))).values
  features[f'{column}_cos'] = (np.cos(timestamp_s * (2*np.pi/day_s))).values
  return features.dropna()

def grg(features: pd.DataFrame, target: pd.Series, zeta=0.5, norm=False) -> pd.DataFrame:
    """
    Returns the grey relational grade for a feature set and target data frame.
    Data should be scaled before processing.
    """
    # Convert the DataFrame and Series to numpy arrays
    feature_data = features.values
    target_data  = target.values.reshape(-1, 1)  # Reshape target to be a 2D array for concatenation
    
    # Combine the target and features into one array with the target as the first column
    data = np.concatenate([target_data, feature_data], axis=1)
    
    # Normalize the data using MinMaxScaler
    if norm:
        scaler = MinMaxScaler()
        data = scaler.fit_transform(data)
    
    # Calculate the absolute differences
    abs_diff = np.abs(data - data[:, [0]])
    
    # Find the minimum and maximum of the absolute differences
    min_diff = np.nanmin(abs_diff)
    max_diff = np.nanmax(abs_diff)
    
    # Calculation of the grey relational coefficient matrix
    grc = (min_diff + (zeta * max_diff)) / (abs_diff + (zeta * max_diff))
    
    # Since the first column is the target, we ignore it in the result
    grc_target = grc[:, 1:]

    # Return as data frame
    return GreyRelationalGrade(grc_target, features.columns)
 
def relevant_features(grg: pd.DataFrame, threshold = 0.7):
    return grg.columns[grg.mean().ge(threshold)].tolist()