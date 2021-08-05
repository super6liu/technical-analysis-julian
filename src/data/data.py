import sys
from asyncio import create_task, gather
from typing import Tuple

from numpy import isclose
from src.constants import Env
from src.data.database import Database
from src.data.histories import Histories
from src.data.symbols import Symbols
from src.logger import WithLogger
from src.utils.asyncio_utils import AsyncioUtils
from src.utils.date_utiles import DateUtils


class Data(WithLogger):
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
        if not symbols:
            symbols = await self.__database.history.read_symbols()

        self.logger.info(f"{len(symbols)} symbols.")
        tasks = map(lambda s: create_task(self.update(s), name=s), symbols)

        async for i in AsyncioUtils.task_queue(tasks):
            progress = i * 100 // len(symbols)
            sys.stdout.write('\r{0}: [{1}{2}] {3}% - {4}/{5}'.format("Backfill", '#'*(
                progress//2), '-'*(50-progress//2), progress, i, len(symbols)))

        self.logger.info("done.")

    async def validate(self, *symbols: Tuple[str]):
        if not symbols:
            symbols = await self.__database.history.read_symbols()

        for i, symbol in enumerate(symbols):
            progress = i * 100 // len(symbols)
            sys.stdout.write('\r{0}: [{1}{2}] {3}% - {4}/{5}'.format("Validate", '#'*(
                progress//2), '-'*(50-progress//2), progress, i, len(symbols)))

            webCoroutine = self.__histories.history(symbol)
            dbCoroutine = self.__database.history.read(symbol)
            web, db = await gather(webCoroutine, dbCoroutine)

            candidate = lambda d: d[['Open', 'High', 'Low', 'Close']].head(len(d) - 1)
            try:
                if not isclose(candidate(web), candidate(db)).all():
                    self.logger.error(symbol)
                    await self.__database.history.delete(symbol)
                    await self.__database.history.insert(symbol, web)
            except Exception as e:
                self.logger.exception(e)
                await self.__database.history.delete(symbol)

            

    async def delete(self, symbol: str):
        # FK CASCADE to History table
        if (self.__env == Env.TEST):
            await self.__database.history.delete(symbol)

        await self.__database.ticker.delete(symbol)

    async def update(self, symbol: str):
        last = await self.__database.history.read_last(symbol)
        if last.empty:
            history = await self.__histories.history(symbol)
            if not history.empty:
                await self.__database.history.insert(symbol, history)
        else:
            updated = last.index[0]
            if (updated >= DateUtils.latest_weekday()):
                return

            history = await self.__histories.history(symbol, start=updated.date())
            ln = len(history)
            if ln < 2 or history.index[0] != updated:
                return

            factor = history['Close'][0] / last['Close'][0]
            if not isclose(factor, 1):
                await self.__database.history.update(symbol, factor)

            await self.__database.history.insert(symbol, history.tail(ln-1))


if __name__ == '__main__':
    async def main():
        ds = Data(Env.PRODUCETION)
        await ds.init()
        # await ds.delete('PUK')

        # await ds.update('MSFT')
        # print(await ds.read('MSFT'))

        # await ds.update('MSFT')
        # print(await ds.read('MSFT'))

        await ds.backfill('ACBA')
        await ds.validate('ACBA')

    AsyncioUtils.run_async_main(main)
