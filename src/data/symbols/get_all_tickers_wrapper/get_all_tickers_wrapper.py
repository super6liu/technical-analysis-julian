from typing import List

from src.data.symbols.get_all_tickers_wrapper.get_tickers import get_tickers
from src.logger import WithLogger
from src.utils.asyncio_utils import asyncize, run_async_main

# from get_all_tickers import get_tickers
# fix get_all_tickers
# https://github.com/shilewenuw/get_all_tickers/issues/12#issuecomment-851007438
# https://stackoverflow.com/questions/65888368/python-get-all-stock-tickers
# Note: possible deprecation:
# https://github.com/shilewenuw/get_all_tickers/pull/17#issuecomment-866093788


class GetAllTickersWrapper(WithLogger):
    def __init__(self) -> None:
        WithLogger.__init__(self)

    async def symbols(self) -> List[str]:
        self.logger.debug("downloading")
        tickers = await asyncize(get_tickers)
        tickers = list(set(map(lambda x: x.strip().upper().replace("/", "-"), tickers)))
        tickers.sort()
        self.logger.debug("downloaded %s" % len(tickers))
        return tickers


if __name__ == "__main__":
    async def main():
        g = GetAllTickersWrapper()
        l = await g.symbols()
        print(l[:10])

    run_async_main(main)
