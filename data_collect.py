"""

"""

import requests
import pandas as pd
from utils import TestError
from typing import Self, Iterable, Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class CryptoSymbol:
    """
    Representation of a Crypto currency trading symbol
        symbol: Name of currency, e.g. 'BTC'
        pair: Trading pair, e.g 'USD'
    """
    symbol: str
    pair: str

    @classmethod
    def from_string(cls, symbol_pair_str: str) -> Self:
        """Create a CryptoSymbol instance from a string like 'BTC/USD'"""
        try:
            symbol, pair = symbol_pair_str.split("/")
            return cls(symbol, pair)
        except ValueError as e:
            raise e("String format must be 'SYMBOL/PAIR', e.g. 'BTC/USD'")

    def to_string(self) -> str:
        """ Return trading symbol string, e.g. 'BTC/USD' """
        return f"{self.symbol}/{self.pair}"


def symbol_price_hist(
    symbol: CryptoSymbol | Iterable[CryptoSymbol],
    limit: int = 2000,
    exchange: str | None = None,
    unit: str = "day",
    merge: bool = False,
    index: str = "time"
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """
    Fetch historical data for a multiple crypto currency trading pair
    (API Info: https://min-api.cryptocompare.com/documentation?key=Historical&cat=dataHistoday)
    Args:
        symbol: Crypto currency/currencies to get history from
        limit: Number of data points to retrieve (e.g. 14 = two weeks)
        exchange: Specific exchange to fetch data from
        unit: Specifies unit of 'limit' parameter. Select from: ['minute', 'hour', 'day']
        merge: Merge data for all symbols into one common dataframe
        index: Set index of dataframe
    Returns:
        DataFrame or a dictionary of dataframes containing the historical data of a crypto
        trading pair(s)
    """
    def _single_symbol_price_hist(symbol: CryptoSymbol) -> pd.DataFrame:
        # Get API URL
        url: str = {
            "minute": "https://min-api.cryptocompare.com/data/v2/histominute",
            "hour": "https://min-api.cryptocompare.com/data/v2/histohour",
            "day": "https://min-api.cryptocompare.com/data/v2/histoday"
        }.get(unit, None)
        if url is None:
            raise ValueError("Invalid unit provided. Select from 'minute', 'hour' or 'day'.")

        # Get params to use with API
        params = {
            "fsym": symbol.symbol,
            "tsym": symbol.pair,
            "limit": limit,
        }
        if exchange:
            params["e"] = exchange

        # Send params to API
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses

        # Collect HTTP response
        data: dict = response.json()
        if data.get("Response") != "Success":
            s: str = f"API Error: {data.get('Message')}"
            raise ValueError(s)

        # Set up dataframe to be returned
        df = pd.DataFrame(data["Data"]["Data"])
        df["time"] = pd.to_datetime(df["time"], unit="s")

        return df

    # Get dataframe with results
    df_res: pd.DataFrame
    data: dict[str, pd.DataFrame] = {}
    if isinstance(symbol, Iterable):
        for sym in symbol:
            sym_str = sym.to_string()  # Store the string version of the symbol once
            data[sym_str] = _single_symbol_price_hist(sym)
            data[sym_str]['time'] = pd.to_datetime(
                data[sym_str]['time'],
                unit='s'
            )
            if index != "":
                data[sym_str].set_index(index, inplace=True)
            # Add prefix to each column if dataset is to be merged
            if merge:
                data[sym_str].rename(
                    columns={col: f"{sym_str}_{col}" for col in data[sym_str].columns},
                    inplace=True
                )

        # Merge dataframes into one
        if merge:
            df_res = data[symbol[0].to_string()]  # Get the first symbol's data
            for sym in symbol[1:]:
                sym_str = sym.to_string()
                df_res = pd.merge(
                    df_res, data[sym_str], left_index=True, right_index=True, how='outer'
                )
        else:
            df_res = data

    else:
        df_res = _single_symbol_price_hist(symbol)
        df_res['time'] = pd.to_datetime(
            df_res['time'],
            unit='s'
        )
        if index != "":
            df_res.set_index(index, inplace=True)
    return df_res


if __name__ == "__main__":
    verbose: bool = True

    """ Utility functions """

    def _vprint(s: str, end: str = '\n') -> None:
        if verbose:
            print(s, end=end)

    def test(fn: Callable[..., bool], *args, **kwargs) -> None:
        _vprint(f"Testing '{fn.__name__}' ...'", end="")
        ret: bool = fn(*args, **kwargs)
        if not ret:
            s: str = f"Test failed: {fn}: {fn.__name__}"
            raise TestError(s)
        _vprint("PASSED!")

    """ Test functions """

    def _utest_symbol_price_hist(columns_per_df: int = 8) -> bool:
        df: pd.DataFrame
        # Test with single symbol
        symbol: CryptoSymbol = CryptoSymbol.from_string("BTC/USD")
        df = symbol_price_hist(symbol, unit='minute')
        if not df.shape[1] == columns_per_df:
            return False

        # Test multiple symbols
        symbols: list[CryptoSymbol] = [
            CryptoSymbol.from_string("BTC/USD"),
            CryptoSymbol.from_string("ETH/BTC")
        ]
        df = symbol_price_hist(symbols, merge=True)
        if not df.shape[1] == columns_per_df * len(symbols):
            return False

        return True

    """ Run function tests """

    test(_utest_symbol_price_hist)
