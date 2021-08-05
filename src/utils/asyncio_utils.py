from asyncio import Queue, get_event_loop, sleep
from asyncio.tasks import Task
from concurrent.futures import ThreadPoolExecutor
from types import CoroutineType
from typing import Any, AsyncGenerator, Callable, Iterable, TypeVar

T = TypeVar('T')

thread_pool_executor: ThreadPoolExecutor = ThreadPoolExecutor()

def run_async_main(func: CoroutineType):
    loop = get_event_loop()
    loop.run_until_complete(func())

def asyncize(func: Callable[[Any], T], *args, **kwargs):
    loop = get_event_loop()
    return loop.run_in_executor(thread_pool_executor, lambda: func(*args, **kwargs))

async def task_queue(tasks: Iterable[Task], maxsize: int = 10) -> AsyncGenerator[int, None]:
    queue = Queue(maxsize)
    i = 0
    yield i
    while True:
        if not queue.full() and (task := next(tasks, None)):
            await queue.put(task)

        if (queue.empty()):
            break

        item = await queue.get()
        if (item._state == "PENDING"):
            await queue.put(item)
            await sleep(0.1 * queue.qsize())
        else:
            queue.task_done()
            i += 1
            yield i
