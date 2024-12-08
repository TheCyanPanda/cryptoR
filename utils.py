"""
Collection of useful functions
"""
from typing import Any
import pandas as pd


class TestError(Exception):
    pass


def add_prefix_to_df(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """ Function to add prefix to columns of a dataframe """
    df = df.rename(columns={col: f"{prefix}_{col}" for col in df.columns})
    return df


def defn(
    value: Any,
    default: Any
) -> Any:
    """
    Returns default value if value is None else value
    """
    if value is None:
        return default
    return value

