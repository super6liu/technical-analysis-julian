from src.constants import Env
from src.data.database.history_table import HistoryTable


class Database():
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        self.history = HistoryTable(env)

    async def init(self):
        await self.history.init()
