import pandas as pd
from matplotlib import pyplot as plt

def plot_system_profile(
    df: pd.DataFrame,
    figsize=(10,7)
) -> pd.DataFrame:
    """
    Creates a plot of the system profile, and marginal emissions (if present)
    """
    fig = plt.figure(figsize=figsize)
    ax = plt.gca()

    if 'MarginalEmissions' in df:
        sx = ax.twinx()