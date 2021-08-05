from src.data import Data
from src.constants import Env


class Watchdog():
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        self.__env = env
        self.__data = Data(env)

    async def init(self):
        await self.__data.init()

    async def run(self):
        await self.__data.backfill()
