from asyncio import get_event_loop
from concurrent.futures import ThreadPoolExecutor
from types import CoroutineType
from typing import Any, Callable, TypeVar

T = TypeVar('T')


class AsyncioUtils:
    __thread_pool_executor: ThreadPoolExecutor = ThreadPoolExecutor()

    @staticmethod
    def run_async_main(func: CoroutineType):
        loop = get_event_loop()
        loop.run_until_complete(func())

    @staticmethod
    def asyncize(func: Callable[[Any], T], *args, **kwargs):
        loop = get_event_loop()
        return loop.run_in_executor(AsyncioUtils.__thread_pool_executor, lambda : func(*args, **kwargs))
