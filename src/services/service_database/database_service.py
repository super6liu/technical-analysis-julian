from src.services.service_database.history import History
from src.services.service_database.ticker import Ticker
from src.constants import Env

class DatabaseService():
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        self.ticker = Ticker(env)
        self.history = History(env)

    async def init(self):
        await self.ticker.init()
        await self.history.init()
        return self