from abc import ABC, abstractmethod
from typing import Iterable


class ExecutorInterface(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    async def setUp(self):
        pass

    @abstractmethod
    async def tearDown(self):
        pass

    @abstractmethod
    async def execute(self, sql: str):
        pass

    @abstractmethod
    async def read(self, sql: str, params: Iterable):
        pass

    @abstractmethod
    async def write(self, sql: str, params: Iterable):
        pass

    @abstractmethod
    async def writemany(self, sql: str, params: Iterable[Iterable]):
        pass
