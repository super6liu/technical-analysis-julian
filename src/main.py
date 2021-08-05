from src.utils.asyncio_utils import run_async_main
from src.watchdog import Watchdog

if __name__ == '__main__':
    async def main():
        wd = Watchdog()
        await wd.init()
        await wd.run()

    run_async_main(main)
