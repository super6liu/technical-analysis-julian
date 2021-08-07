from src.data import Data


class Watchdog():
    def __init__(self) -> None:
        self.__data = Data()

    async def init(self):
        await self.__data.init()

    async def run(self):
        await self.__data.backfill()
        await self.__data.validate()
