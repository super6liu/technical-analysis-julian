from asyncio import get_event_loop
from os import environ
from types import CoroutineType
from concurrent.futures import ThreadPoolExecutor

from typing import Any, TypeVar, Callable


T = TypeVar('T')


class AsyncioUtils:
    __thread_pool_executor: ThreadPoolExecutor = ThreadPoolExecutor()

    @staticmethod
    def run_async_main(func: CoroutineType, debug: bool = True):
        if debug:
            environ['DEBUG'] = 'True'
        loop = get_event_loop()
        loop.run_until_complete(func())

    @staticmethod
    def asyncify(func: Callable[[Any], T], *args, **kwargs):
        loop = get_event_loop()
        return loop.run_in_executor(AsyncioUtils.__thread_pool_executor, lambda : func(*args, **kwargs))
