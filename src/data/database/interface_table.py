from abc import ABC, abstractmethod

from src.constants import Env
from src.data.database.interface_executor import ExecutorInterface


class TableInterface(ABC):
    @abstractmethod
    def __init__(self, executor: ExecutorInterface, env: Env = Env.PRODUCETION) -> None:
        pass
