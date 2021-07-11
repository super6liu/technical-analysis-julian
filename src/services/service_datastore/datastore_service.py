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
        print(ticker)
        if ticker.empty:
            ticker = pd.DataFrame({'Symbol': pd.Series([symbol], dtype='str'),
                                   'Dividended':  pd.Series([None], dtype='datetime64[D]'),
                                   'Splitted':  pd.Series([None], dtype='datetime64[D]'),
                                   'Updated':  pd.Series([None], dtype='datetime64[D]')})
            ticker.set_index('Symbol', inplace=True)
            await self.__ds.ticker.create(ticker)

            ticker = await self.__ds.ticker.read(symbol)
            history = self.__hs.history(symbol)

            ticker.at[symbol, 'Dividended'] = history.query(
                'Dividends > 0').last('1d').index[0]
            ticker.at[symbol, 'Splitted'] = history.query(
                '`Stock Splits` > 0').last('1d').index[0]
            ticker.at[symbol, 'Updated'] = date.today()

            history.drop(['Dividends', 'Stock Splits'],
                         axis='columns', inplace=True)
            await self.__ds.history.create(history)
            await self.__ds.ticker.update(ticker)
        else:
            history = self.__hs.history(symbol, ticker.loc[0, 'Updated'])

            # dividend & split
            dividend = history.query('Dividends > 0').first('1d')[
                'Dividends'].iloc[0]
            await self.__ds.history.update_dividend(symbol, dividend, ticker.loc[0, 'Dividended'])
            split = history.query('`Stock Splits` > 0')[
                'Stock Splits'].product()
            await self.__ds.history.update_split(symbol, split)

            ticker.loc[0, 'Dividended'] = history.query(
                'Dividends > 0').last('1d')['Date'].iat[0]
            ticker.loc[0, 'Splitted'] = history.query(
                '`Stock Splits` > 0').last('1d')['Date'].iat[0]
            ticker.loc[0, 'Updated'] = date.today()

            history.drop(['`Dividends', 'Stock Splits'], axis='columns')
            await self.__ds.history.update(history)
            await self.__ds.ticker.update(ticker)

    async def delete(self, symbol: str):
        await self.__ds.history.delete(symbol)
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
        # print(await ds.read('MSFT'))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
