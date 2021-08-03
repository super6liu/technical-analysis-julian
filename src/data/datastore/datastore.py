import sys
from asyncio import create_task, gather
from datetime import date, datetime
from random import Random
from typing import Tuple

from numpy import isclose
from pandas import DataFrame, Series
from src.constants import Env
from src.data.database import Database
from src.data.histories import Histories
from src.data.symbols import Symbols
from src.utils.asyncio_utils import AsyncioUtils
from src.utils.date_utiles import DateUtils
from src.logger import WithLogger

class Datastore(WithLogger):
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        WithLogger.__init__(self)
        self.__env = env
        self.__database = Database(env)
        self.__histories = Histories()
        self.__symbols = Symbols()

    async def init(self):
        await self.__database.init()

    async def read_symbols(self):
        return await self.__database.ticker.read_symbols()

    async def read_history(self, symbol: str):
        return await self.__database.history.read(symbol)

    async def backfill(self, *symbols: Tuple[str]):
        begin = datetime.now()
        if not symbols:
            symbols = await self.__symbols.symbols()
        if (self.__env == Env.TEST):
            symbols = symbols[:100]

        trimmed = map(lambda x: x.replace("/", "-"), symbols)
        tasks = map(lambda s: create_task(self.update(s), name=s), trimmed)

        async for i in AsyncioUtils.task_queue(tasks):
            progress = i * 100 // len(symbols)
            sys.stdout.write('\r{0}: [{1}{2}] {3}% - {4}/{5}'.format("Backfill", '#'*(
                progress//2), '-'*(50-progress//2), progress, i, len(symbols)))

        print("\nTime consumed:", datetime.now() - begin)

        r = Random(datetime.now().timestamp())
        symbol = symbols[r.randint(0, len(symbols) - 1)]
        await self.validate(symbol)

    async def validate(self, *symbols: Tuple[str]):
        begin = datetime.now()
        if not symbols:
            symbols = await self.__symbols.symbols()

        for i, symbol in enumerate(symbols):
            self.logger.warning(symbol)
            # progress = i * 100 // len(symbols)
            # sys.stdout.write('\r{0}: [{1}{2}] {3}% - {4}/{5}'.format("Validate", '#'*(
            #     progress//2), '-'*(50-progress//2), progress, i, len(symbols)))


            freshCoroutine = self.__histories.history(symbol)
            tickerCoroutine = self.__database.ticker.read(symbol)
            historyCoroutine = self.__database.history.read(symbol)
            fresh, ticker, history = await gather(freshCoroutine, tickerCoroutine, historyCoroutine)
            # print(fresh, ticker, history)
            self.logger.warning("\n %s" % fresh)
            flag = len(fresh) == len(history) and all(
                isclose(history["Close"], fresh["Close"])) and fresh.index.equals(history.index)
            lastDividendRow = fresh.query('Dividends > 0').last('1d')
            lastSplitRow = fresh.query('`Stock Splits` > 0').last('1d')
            flag1 = (lastDividendRow.empty or ticker['Dividended'][0] == lastDividendRow.index[0]) and (
                lastSplitRow.empty or ticker['Splitted'][0] == lastSplitRow.index[0]) and not ticker['Updated'][0] is None and ticker['Updated'][0] >= history.last('1d').index[0]
            if not flag or not flag1:
                sys.stderr.write(f"Invalid: {symbol}\n")
        
        print("\nTime consumed:", datetime.now() - begin)

    async def delete(self, symbol: str):
        # FK CASCADE to History table
        if (self.__env == Env.TEST):
            await self.__database.history.delete(symbol)

        await self.__database.ticker.delete(symbol)

    async def update(self, symbol: str):
        ticker = await self.__database.ticker.read(symbol)
        if ticker.empty:
            ticker = DataFrame({'Dividended':  Series([None], dtype='datetime64[D]'),
                                'Splitted':  Series([None], dtype='datetime64[D]'),
                                'Updated':  Series([None], dtype='datetime64[D]')})
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
        ds = Datastore(Env.PRODUCETION)
        await ds.init()
        # await ds.delete('PUK')

        # await ds.update('MSFT')
        # print(await ds.read('MSFT'))

        # await ds.update('MSFT')
        # print(await ds.read('MSFT'))

        # await ds.backfill('PUK')
        await ds.validate("ALOT")

    AsyncioUtils.run_async_main(main)
