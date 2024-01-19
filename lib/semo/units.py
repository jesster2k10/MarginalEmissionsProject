import pandas as pd
from enum import Enum
import os

registered_units_file = os.path.join(os.path.dirname(__file__), './List-of-Registered-Units.xlsx')
class SemoFuelType(str, Enum):
    wind       = 'WIND'
    solar      = 'SOLAR'
    multi_fuel = 'MULTI_FUEL'
    coal       = 'COAL'
    gas        = 'GAS'
    hydro      = 'HYDRO'
    peat       = 'PEAT'
    pump_storage = 'PUMP_STORAGE'
    biomass    = 'BIOMASS'
    distillate = 'DISTILLATE'
    battery    = 'BATTERY'
    sync_comp  = 'SYNC_COMP'
    oil        = 'OIL'

RENEWABLES = [
    SemoFuelType.wind.value,
    SemoFuelType.solar.value,
    SemoFuelType.hydro.value,
    SemoFuelType.biomass.value,
    SemoFuelType.sync_comp.value,
    SemoFuelType.battery.value,
    SemoFuelType.pump_storage.value,
]
COAL_GAS = [
    SemoFuelType.coal.value,
    SemoFuelType.gas.value,
    SemoFuelType.peat.value,
    SemoFuelType.oil.value,
]
OTHER = [
    SemoFuelType.multi_fuel.value,
]

def fuelmix_kind(fuel_type: str | SemoFuelType) -> str:
    """
    Returns the type of fuel mix for a given fuel type
    """
    if fuel_type is SemoFuelType:
        fuel_type = fuel_type.value

    if fuel_type.upper() in RENEWABLES:
        return 'RENEWABLES'
    elif fuel_type.upper() in COAL_GAS:
        return 'COAL_GAS'
    elif fuel_type == SemoFuelType.multi_fuel.value:
        return 'MIXED'
    elif fuel_type.upper() in OTHER:
        return 'OTHER'

def units() -> pd.DataFrame:
    """
    Returns a Pandas DataFrame of all the SEMO Registered units
    """
    df = pd.read_excel(
        registered_units_file,
        sheet_name="Registered Units TSC",
        skiprows=[0,1],
    )[['Resource Name', 'Resource Type', 'Fuel Type']].rename(columns={
        'Resource Name': 'ResourceName',
        'Resource Type': 'ResourceType',
        'Fuel Type': 'FuelType'
    }).set_index('ResourceName', drop=True)
    df['FuelType'] = df['FuelType'].fillna('None').astype(str)
    df['FuelKind'] = df['FuelType'].apply(fuelmix_kind)
    return df

def generators() -> pd.DataFrame:
    """
    Returns a Pandas DataFrame of the SEMO registered unit generators
    """
    df = units()
    df = df.loc[df['ResourceType'] == 'GENERATOR']
    return df.drop(columns='ResourceType')