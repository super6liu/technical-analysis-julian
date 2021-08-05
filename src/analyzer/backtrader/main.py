from datetime import datetime

import backtrader as bt
from pandas import DataFrame

from src.constants import Env
from src.data import Data
from src.utils.asyncio_utils import AsyncioUtils
from src.analyzer.backtrader.strategies.test_strategy import TestStrategy



if __name__ == '__main__':
    async def main():
        ds = Data()
        await ds.init()
        symbols = await ds.read_symbols()
        df = await ds.read_history("MSFT")

        data = bt.feeds.PandasData(dataname=df)


        cerebro = bt.Cerebro()
        cerebro.addstrategy(TestStrategy)
        cerebro.adddata(data)


        cerebro.broker.setcash(100000.0)

        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

        cerebro.run()

        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        # cerebro.plot(style='bar')


    AsyncioUtils.run_async_main(main)
