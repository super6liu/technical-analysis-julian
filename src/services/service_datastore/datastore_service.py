from datetime import date
import pandas as pd
import asyncio

from src.services.service_database import DatabaseService
from src.services.service_history import HistoryService
from src.services.service_symbol import SymbolService


class DatastoreService():
    def __init__(self, databaseService: DatabaseService, historyService: HistoryService, symbolService: SymbolService) -> None:
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

            ticker = await self.__ds.ticker.read(symbol)
            history = self.__hs.history(symbol)

            lastDividendRow = history.query('Dividends > 0').last('1d')
            if not lastDividendRow.empty:
                ticker.at[symbol, 'Dividended'] = lastDividendRow.index[0]
            lastSplitRow = history.query('`Stock Splits` > 0').last('1d')
            if not lastSplitRow.empty:
                ticker.at[symbol, 'Splitted'] = lastSplitRow.index[0]
            ticker.at[symbol, 'Updated'] = date.today()

            history.drop(['Dividends', 'Stock Splits'],
                         axis='columns', inplace=True)
            history.insert(0, 'Symbol', symbol, allow_duplicates=True)
            await self.__ds.history.create(history)
            await self.__ds.ticker.update(ticker)
        else:
            updated = ticker['Updated'][0]
            history = self.__hs.history(symbol, updated)

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
            history.insert(0, 'Symbol', symbol, allow_duplicates=True)
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
        ss = await DatabaseService().init()
        ds = DatastoreService(
            ss, HistoryService(), SymbolService())
        await ds.delete('MSFT')
        await ds.update('MSFT')
        print(await ds.read('MSFT'))

        await ds.update('MSFT')
        print(await ds.read('MSFT'))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
