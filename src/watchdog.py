from data.database.database import Database
from src.data import Datastore
from src.constants import Env

class Watchdog():
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        self.__env = env
        self.__datastore = Datastore(env)

    async def init(self):
        await self.__datastore.init()

    async def run(self):
        await self.__datastore.backfill()
        
        
    