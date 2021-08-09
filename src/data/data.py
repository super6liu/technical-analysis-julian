from asyncio import create_task
from datetime import datetime, timedelta
from typing import Tuple

from numpy import isclose
from src.data.database import Database
from src.data.histories import Histories
from src.data.symbols import Symbols
from src.logger import WithLogger
from src.utils.asyncio_utils import run_async_main, task_queue
from src.utils.console_utils import progress
from src.utils.date_utiles import LATEST_WEEKDAY


class Data(WithLogger):
    def __init__(self) -> None:
        WithLogger.__init__(self)
        self.__database = Database()
        self.__histories = Histories()
        self.__symbols = Symbols()

    async def init(self):
        await self.__database.init()

    async def read_symbols(self):
        return await self.__database.history.read_symbols()

    async def read_history(self, symbol: str):
        return await self.__database.history.read(symbol)

    async def backfill(self, *symbols: Tuple[str]):
        if not symbols:
            symbols = await self.__symbols.symbols()

        self.logger.info(f"{len(symbols)} symbols.")
        tasks = map(lambda s: create_task(self.__update(s), name=s), symbols)

        async for i in task_queue(tasks):
            progress(i, len(symbols), "Backfill")

        self.logger.info("done.")

    async def validate(self, resume: str = None, symbols: Tuple[str] = None):
        """
        :Parameter
            resume: str
                Last known valid symbol to resume the validation from.
                Default is None.
        """
        if not symbols:
            symbols = await self.__database.history.read_symbols()
            if resume:
                symbols = symbols[symbols.index(resume) + 1:]

        self.logger.info(f"{len(symbols)} symbols.")
        start = datetime.now()
        tasks = map(lambda s: create_task(self.__test(s), name=s), symbols)

        async for i in task_queue(tasks):
            progress(i, len(symbols), "Validate")

        self.logger.info(datetime.now() - start)

    async def delete(self, symbol: str, start: str = '2000-01-01'):
        pass

    async def __test(self, symbol: str):
        db = await self.__database.history.read_first(symbol)
        web = await self.__histories.history(symbol, start=str(db.index[0].date()), end=str(db.index[0].date() + timedelta(days=1)))

        def candidate(d):
            return d[['Open', 'High', 'Low', 'Close']]

        async def handle():
            self.logger.error("Invalid data for\n%s.\nData in db:\n%s.\nData from web:\n%s" % (symbol, db, web))            
            await self.__database.history.delete(symbol)

        try:
            if not isclose(candidate(web), candidate(db)).all():
                await handle()
        except Exception as e:
            await handle()
            self.logger.exception(e)

    async def __update(self, symbol: str):
        last = await self.__database.history.read_last(symbol)
        if last.empty:
            history = await self.__histories.history(symbol)
            if not history.empty:
                await self.__database.history.insert(symbol, history)
        else:
            updated = last.index[0]
            if (updated >= LATEST_WEEKDAY):
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
        ds = Data()
        await ds.init()

        # await ds.backfill('GIM')
        await ds.validate()

    run_async_main(main)
