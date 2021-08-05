from src.constants import Env
from src.data.database.history_table import HistoryTable
from src.data.database.mysql_wrapper import Wrapper


class Database():
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        self.executor = Wrapper(env)
        self.history = HistoryTable(self.executor, env)

    async def init(self):
        await self.executor.setUp()
        await self.history.init()

    async def tearDown(self):
        await self.executor.tearDown()
