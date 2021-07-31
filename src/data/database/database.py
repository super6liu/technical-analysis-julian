from src.constants import Env
from src.data.database.history_table import HistoryTable
from src.data.database.ticker_table import TickerTable


class Database():
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        self.ticker = TickerTable(env)
        self.history = HistoryTable(env)

    async def init(self):
        await self.ticker.init()
        await self.history.init()
