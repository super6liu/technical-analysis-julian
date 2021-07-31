from datetime import date

from pandas import Series
from src.constants import Env
from src.data.database import Database
from src.data.histories import Histories
from src.data.symbols import Symbols
from src.utils.asyncio_utils import AsyncioUtils
from src.utils.date_utiles import DateUtils


class Datastore():
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        self.__env = env
        self.__database = Database(env)
        self.__histories = Histories()
        self.__symbols = Symbols()

    async def init(self):
        await self.__database.init()

    async def read(self, symbol: str):
        return await self.__database.history.read(symbol)

    async def backfill(self):
        symbols = self.__symbols.symbols()
        for s in symbols:
            await self.__update(s)
            if self.__env == Env.TEST:
                break

    async def delete(self, symbol: str):
        # FK CASCADE to History table
        if (self.__env == Env.TEST):
            await self.__database.history.delete(symbol)

        await self.__database.ticker.delete(symbol)

    async def update(self, symbol: str):
        ticker = await self.__database.ticker.read(symbol)
        if ticker.empty:
            ticker.loc[0] = Series()
            history = await self.__histories.history(symbol)

            lastDividendRow = history.query('Dividends > 0').last('1d')
            if not lastDividendRow.empty:
                ticker['Dividended'][0] = lastDividendRow.index[0]
            lastSplitRow = history.query('`Stock Splits` > 0').last('1d')
            if not lastSplitRow.empty:
                ticker['Splitted'][0] = lastSplitRow.index[0]
            ticker['Updated'][0] = date.today()

            await self.__database.ticker.insert(symbol, ticker)
            await self.__database.history.insert(symbol, history)
        else:
            updated = ticker['Updated'][0]
            if (updated >= DateUtils.latest_weekday()):
                return

            history = await self.__histories.history(symbol, start=updated)
            if history.empty:
                return

            # dividend & split
            newDividendRow = history.query('Dividends > 0').first('1d')
            if not newDividendRow.empty:
                dividend = newDividendRow['Dividends'][0]
                await self.__database.history.update_dividend(symbol, dividend, ticker['Dividended'][0])

            newSplitRows = history.query('`Stock Splits` > 0')
            if not newSplitRows.empty:
                split = newSplitRows['Stock Splits'].product()
                if split != 1:
                    await self.__database.history.update_split(symbol, split)

            lastDividendRow = history.query('Dividends > 0').last('1d')
            if not lastDividendRow.empty:
                ticker['Dividended'][0] = lastDividendRow.index[0]
            lastSplitRow = history.query('`Stock Splits` > 0').last('1d')
            if not lastSplitRow.empty:
                ticker['Splitted'][0] = lastSplitRow.index[0]
            ticker['Updated'][0] = date.today()

            await self.__database.history.insert(symbol, history)
            await self.__database.ticker.update(symbol, ticker)


if __name__ == '__main__':
    async def main():
        ds = Datastore(Env.TEST)
        await ds.init()
        await ds.delete('MSFT')
        await ds.update('MSFT')
        print(await ds.read('MSFT'))

        await ds.update('MSFT')
        print(await ds.read('MSFT'))

    AsyncioUtils.run_async_main(main)
