from typing import Tuple, Type

from backtrader import Cerebro, analyzers, feeds
from src.analyzer.backtrader_wrapper.algos.simple_moving_average_strategy import *
from src.analyzer.backtrader_wrapper.base_screener import BaseScreener
from src.analyzer.backtrader_wrapper.base_strategy import BaseStrategy
from src.data import Data
from src.logger import WithLogger
from src.utils.asyncio_utils import run_async_main

# matplotlib==3.2.2 https://stackoverflow.com/a/63974376


class BacktraderWrapper(WithLogger):
    def __init__(self) -> None:
        super().__init__()

        self.data_store = Data()

    async def asyncSetUp(self):
        await self.data_store.init()

    async def backtest(self, strategy: Type[BaseStrategy], *symbols: Tuple[str], **kwargs):
        cerebro = await self.__create_cerebro(*symbols)
        cerebro.addstrategy(strategy, **kwargs)
        cerebro.addanalyzer(analyzers.SharpeRatio, _name='mysharpe')
        cerebro.addanalyzer(analyzers.AnnualReturn, _name='annualreturn')

        thestrats = cerebro.run(runonce=True, preload=True)
        thestrat = thestrats[0]

        print('Annual Return:', thestrat.analyzers.annualreturn.get_analysis())
        print('Sharpe Ratio:', thestrat.analyzers.mysharpe.get_analysis())
        cerebro.plot(style='candle')

    async def optimize(self, strategy: Type[BaseStrategy], *symbols: Tuple[str], **kwargs):
        cerebro = await self.__create_cerebro(*symbols)
        cerebro.optstrategy(strategy, **kwargs)
        cerebro.run(maxcpus=4)

    async def screen(self, screener: Type[BaseScreener], *symbols: Tuple[str], **kwargs):
        cerebro = await self.__create_cerebro(*symbols)
        cerebro.addanalyzer(screener)
        cerebro.run(runonce=False, stdstats=False, writer=True)

    async def __create_cerebro(self, *symbols: Tuple[str]):
        cerebro = Cerebro()
        for symbol in symbols:
            df = await self.data_store.read_history(symbol)
            data = feeds.PandasData(dataname=df[-365:])
            cerebro.adddata(data, name=symbol)

        COST = 100000.0
        cerebro.broker.setcash(COST)
        cerebro.broker.setcommission(commission=0.0)
        return cerebro


if __name__ == '__main__':
    async def main():
        bw = BacktraderWrapper()
        await bw.asyncSetUp()
        symbols = ("MSFT",)
        # await  bw.backtest(SimpleMovingAverageStrategy, *symbols)
        await bw.optimize(SimpleMovingAverageStrategy, *symbols,  maperiod=range(1, 30))
        # await bw.screen(SimpleMovingAverageScreener, *symbols)

    run_async_main(main)
