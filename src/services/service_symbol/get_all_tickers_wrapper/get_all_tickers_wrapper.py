from typing import Generator

from src.utils.asyncio_utils import AsyncioUtils

from src.services.service_symbol.get_all_tickers_wrapper.get_tickers import get_tickers
# from get_all_tickers import get_tickers
# fix get_all_tickers
# https://github.com/shilewenuw/get_all_tickers/issues/12#issuecomment-851007438
# https://stackoverflow.com/questions/65888368/python-get-all-stock-tickers
# Note: possible deprecation:
# https://github.com/shilewenuw/get_all_tickers/pull/17#issuecomment-866093788


class GetAllTickersWrapper:
    async def symbols(self) -> Generator[str, None, None]:
        tickers = await AsyncioUtils.asyncize(get_tickers)
        return map(lambda x: x.strip().upper(), tickers)


if __name__ == "__main__":
    async def main():
        g = GetAllTickersWrapper()
        l = list(await g.symbols())
        l.sort(reverse=True, key=len)
        print(l[:10])

    AsyncioUtils.run_async_main(main)
