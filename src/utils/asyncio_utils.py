from asyncio import get_event_loop
from os import environ
from types import CoroutineType


def run_async_main(func: CoroutineType, debug: bool = True):
    if debug:
        environ['DEBUG'] = 'True'
    loop = get_event_loop()
    loop.run_until_complete(func())
