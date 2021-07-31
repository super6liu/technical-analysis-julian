from datetime import date
import pandas as pd

from src.data.database import Database
from src.data.histories import Histories
from src.data.symbols import Symbols
from src.utils.asyncio_utils import AsyncioUtils
from src.utils.date_utiles import DateUtils
from src.constants import Env

class Datastore():
    def __init__(self, databaseService: Database, historyService: Histories, symbolService: Symbols) -> None:
        self.__ds = databaseService
        self.__hs = historyService
        self.__ss = symbolService

    async def read(self, symbol: str):
        return await self.__ds.history.read(symbol)

    async def update(self, symbol: str):
        ticker = await self.__ds.ticker.read(symbol)
        if ticker.empty:
            ticker = pd.DataFrame({'Symbol': pd.Series([symbol], dtype='str'),
                                   'Dividended':  pd.Series([None], dtype='datetime64[D]'),
                                   'Splitted':  pd.Series([None], dtype='datetime64[D]'),
                                   'Updated':  pd.Series([None], dtype='datetime64[D]')})
            ticker.set_index('Symbol', inplace=True)
            await self.__ds.ticker.create(ticker)

            history = await self.__hs.history(symbol)

            lastDividendRow = history.query('Dividends > 0').last('1d')
            if not lastDividendRow.empty:
                ticker.at[symbol, 'Dividended'] = lastDividendRow.index[0]
            lastSplitRow = history.query('`Stock Splits` > 0').last('1d')
            if not lastSplitRow.empty:
                ticker.at[symbol, 'Splitted'] = lastSplitRow.index[0]
            ticker.at[symbol, 'Updated'] = date.today()

            history.drop(['Dividends', 'Stock Splits'], axis='columns', inplace=True)
            await self.__ds.history.create(history)
            await self.__ds.ticker.update(ticker)
        else:
            updated = ticker['Updated'][0]
            if (updated >= DateUtils.latest_weekday()):
                return

            history = await self.__hs.history(symbol, start = updated)
            if history.empty:
                return

            # dividend & split
            newDividendRow = history.query('Dividends > 0').first('1d')
            if not newDividendRow.empty:
                dividend = newDividendRow['Dividends'][0]
                await self.__ds.history.update_dividend(symbol, dividend, ticker['Dividended'][0])

            newSplitRows = history.query('`Stock Splits` > 0')
            if not newSplitRows.empty:
                split = newSplitRows['Stock Splits'].product()
                if split != 1:
                    await self.__ds.history.update_split(symbol, split)

            lastDividendRow = history.query('Dividends > 0').last('1d')
            if not lastDividendRow.empty:
                ticker.at[symbol, 'Dividended'] = lastDividendRow.index[0]
            lastSplitRow = history.query('`Stock Splits` > 0').last('1d')
            if not lastSplitRow.empty:
                ticker.at[symbol, 'Splitted'] = lastSplitRow.index[0]
            ticker.at[symbol, 'Updated'] = date.today()

            history.drop(['Dividends', 'Stock Splits'], axis='columns', inplace=True)
            await self.__ds.history.create(history)
            await self.__ds.ticker.update(ticker)

    async def delete(self, symbol: str):
        # FK CASCADE to History table
        await self.__ds.ticker.delete(symbol)

    async def backfill(self, debug=False):
        symbols = self.__ss.symbols()
        for s in symbols:
            await self.update(s)
            if debug:
                break


if __name__ == '__main__':
    async def main():
        ss = await Database().init(Env.TEST)
        ds = Datastore(
            ss, Histories(), Symbols())
        await ds.delete('MSFT')
        await ds.update('MSFT')
        print(await ds.read('MSFT'))

        await ds.update('MSFT')
        print(await ds.read('MSFT'))

    AsyncioUtils.run_async_main(main)
