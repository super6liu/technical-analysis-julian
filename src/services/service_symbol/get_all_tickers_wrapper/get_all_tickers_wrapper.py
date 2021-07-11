from typing import Generator
from get_all_tickers import get_tickers as gt

# fix get_all_tickers
# https://github.com/shilewenuw/get_all_tickers/issues/12#issuecomment-851007438
# https://stackoverflow.com/questions/65888368/python-get-all-stock-tickers
# Note: possible deprecation:
# https://github.com/shilewenuw/get_all_tickers/pull/17#issuecomment-866093788


class GetAllTickersWrapper:
    def symbols(self) -> Generator[str, None, None]:
        tickers = gt.get_tickers()
        return map(lambda x: x.strip().upper(), tickers)


if __name__ == "__main__":
    g = GetAllTickersWrapper()
    l = list(g.symbols())
    l.sort(reverse=True, key=len)
    print(l[:10])
