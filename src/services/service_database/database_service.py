from src.services.service_database.history import History
from src.services.service_database.ticker import Ticker
from src.constants import Env

class DatabaseService():
    def __init__(self) -> None:
        self.ticker = Ticker()
        self.history = History()

    async def init(self, env: Env = Env.PRODUCETION):
        await self.ticker.init(env)
        await self.history.init(env)
        return self