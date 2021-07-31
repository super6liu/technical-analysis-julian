from abc import ABC, abstractmethod
from typing import List

from src.constants import Env
from src.data.database.mysql_wrapper.wrapper import Wrapper

"""
pandas.DataFrame:

        Symbol   Column
Symbol                                                             
MSFT      MSFT    Value
"""


class BaseTable(ABC):
    def __init__(self, index: str, columns: List[str], env: Env = Env.PRODUCETION) -> None:
        super().__init__()
        self.index = index
        self.columns = columns
        self.env = env

    async def init(self) -> None:
        self.executor = await Wrapper.init(self.env)

    @abstractmethod
    async def insert(self, *args, **kwargs):
        pass

    @abstractmethod
    async def read(self, *args, **kwargs):
        pass

    @abstractmethod
    async def update(self, *args, **kwargs):
        pass

    @abstractmethod
    async def delete(self, *args, **kwargs):
        pass
