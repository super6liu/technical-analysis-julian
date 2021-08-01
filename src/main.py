from src.utils.asyncio_utils import AsyncioUtils
from src.watchdog import Watchdog

if __name__ == '__main__':
    async def main():
        wd = Watchdog()
        await wd.init()
        await wd.run()

    AsyncioUtils.run_async_main(main)
