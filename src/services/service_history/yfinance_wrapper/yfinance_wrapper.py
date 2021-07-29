from typing import Any
from datetime import date
from decimal import Decimal
import requests_cache
from pandas import DataFrame
import yfinance as yf


from src.utils.asyncio_utils import AsyncioUtils

'''
def auto_adjust(data):
    df = data.copy()
    ratio = df["Close"] / df["Adj Close"]
    df["Adj Open"] = df["Open"] / ratio
    df["Adj High"] = df["High"] / ratio
    df["Adj Low"] = df["Low"] / ratio

    df.drop(
        ["Open", "High", "Low", "Close"],
        axis=1, inplace=True)

    df.rename(columns={
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low", "Adj Close": "Close"
    }, inplace=True)

    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df[["Open", "High", "Low", "Close", "Volume"]]
'''


class YfinanceWrapper:
    def __init__(self) -> None:
        self.__session = requests_cache.CachedSession('yfinance.cache')
        self.__session.headers['User-agent'] = 'technical-analysis-julian/1.0'

    async def history(self, symbol: str, start: date = None, end: date = None) -> DataFrame:
        if not start is None and end is None:
            end = date.today()

        ticker = yf.Ticker(symbol, self.__session)
        df = await AsyncioUtils.asyncize(ticker.history, period='max', start=start, end=end)
        df.dropna(inplace=True)
        df = df[~df.index.duplicated(keep='first')]

        df.reset_index(inplace=True)
        df = DataFrame({c: map(self.__converter(c), df[c]) for c in df.columns})
        df.insert(0, 'Symbol', symbol, allow_duplicates=True)
        df.set_index(["Symbol", "Date"], drop=False, inplace=True)
        return df

    def __converter(self, col: str):
        if col == "Date":
            return self.__cast_date

        if col == "Volume":
            return int

        return self.__cast_decimal

    def __cast_date(self, x: Any):
        return x.date()

    def __cast_decimal(self, x: Any):
        return Decimal("%.6f" % x)

    def __cast_int(self, x: Any):
        return int(x)

if __name__ == "__main__":
    async def main():
        y = YfinanceWrapper()
        df = await y.history('MSFT', '2021-05-15', '2021-05-19')
        print(df)

    AsyncioUtils.run_async_main(main)
