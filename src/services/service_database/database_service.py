from src.services.service_database.mysql_wrapper import History, Ticker

class DatabaseService():
    def __init__(self) -> None:
        self.ticker = Ticker()
        self.history = History()

    async def init(self):
        await self.ticker.init()
        await self.history.init()
        return self