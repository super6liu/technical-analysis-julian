from datetime import datetime
from typing import Type

import backtrader as bt
from backtrader import Strategy
from pandas import DataFrame
from pandas._libs.tslibs.timedeltas import Timedelta

from src.data import Data
from src.utils.asyncio_utils import run_async_main
from src.analyzer.backtrader_wrapper.strategies.test_strategy import TestStrategy
from src.logger import WithLogger

# matplotlib==3.2.2 https://stackoverflow.com/a/63974376

class BacktraderWrapper(WithLogger):
    def __init__(self) -> None:
        super().__init__()

    def backtest(self, history: DataFrame, strategy: Type[Strategy]):
        data = bt.feeds.PandasData(dataname=history)
        cerebro = bt.Cerebro()
        cerebro.addstrategy(strategy)
        cerebro.adddata(data)

        COST = 100000.0
        cerebro.broker.setcash(COST)

        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

        cerebro.run()

        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        rate = cerebro.broker.getvalue() / COST
        years = (history.last('1d').index[0] -
                 history.first('1d').index[0]).days / 365.0
        print('Total Gain: %.2f%%, Annualized Gain: %.2f%%' %
              (rate * 100 - 100, pow(rate, 1/years) * 100 - 100))

        cerebro.plot(style='bar')


if __name__ == '__main__':
    async def main():
        ds = Data()
        await ds.init()
        df = await ds.read_history("AMZN")

        bw = BacktraderWrapper()
        bw.backtest(df, TestStrategy)

    run_async_main(main)
