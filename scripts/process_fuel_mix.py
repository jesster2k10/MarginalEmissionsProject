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

def process_fuel_mix():
    pass