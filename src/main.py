from datetime import datetime
import backtrader as bt
from pandas import DataFrame

from src.data.database import Database
from src.data.histories import Histories
from src.utils.asyncio_utils import AsyncioUtils
from src.constants import Env
import yfinance as yf

class TestStrategy(bt.Strategy):
    
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

        # To keep track of pending orders
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            if self.dataclose[0] < self.dataclose[-1]:
                    # current close less than previous close

                    if self.dataclose[-1] < self.dataclose[-2]:
                        # previous close less than the previous close

                        # BUY, BUY, BUY!!! (with default parameters)
                        self.log('BUY CREATE, %.2f' % self.dataclose[0])

                        # Keep track of the created order to avoid a 2nd order
                        self.order = self.buy()

        else:

            # Already in the market ... we might sell
            if len(self) >= (self.bar_executed + 5):
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()



if __name__ == '__main__':
    async def main():
        start = datetime.now()
        hs = Histories()
        df = await hs.history("MSFT")
        print("Download", datetime.now() - start)
        ds = await Database(Env.TEST).init()
        await ds.ticker.create("MSFT", DataFrame({"Dividended": [], "Splitted": [], "Updated": []}))
        print("Ticker", datetime.now() - start)
        await ds.history.create("MSFT", df)
        print("History", datetime.now() - start)
        df = await ds.history.read("MSFT")
        print("Read", datetime.now() - start)
        return

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