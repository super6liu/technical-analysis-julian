from abc import ABC, abstractmethod

from src.data.database.interface_executor import ExecutorInterface


class TableInterface(ABC):
    @abstractmethod
    def __init__(self, executor: ExecutorInterface) -> None:
        pass
