from datetime import datetime, timedelta, timezone

import requests_cache
import yfinance as yf
from pandas import DataFrame
from src.logger import WithLogger
from src.utils.asyncio_utils import asyncize, run_async_main
from src.utils.date_utiles import TOMORROW

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


class YfinanceWrapper(WithLogger):
    def __init__(self) -> None:
        WithLogger.__init__(self)
        self.__session = requests_cache.CachedSession('yfinance.cache')
        self.__session.headers['User-agent'] = 'technical-analysis-julian/1.0'

    async def history(self, symbol: str, start: str = "2000-01-01", end: str = str(TOMORROW)) -> DataFrame:
        self.logger.debug("%s downloading with period %s to %s." % (symbol, start, end))
        ticker = yf.Ticker(symbol, self.__session)
        df = await asyncize(ticker.history, start=start, end=end, actions=False, debug=False)
        df.dropna(inplace=True)
        df = df[~df.index.duplicated(keep='first')]
        self.logger.debug("%s downloaded with %d rows." % (symbol, len(df)))
        return df


if __name__ == "__main__":
    async def main():
        y = YfinanceWrapper()
        df = await y.history('MSFT', '2021-01-01')
        # df.to_csv("msft.csv", header=True, index=True)
        print(df)

    run_async_main(main)
