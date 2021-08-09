from backtrader import indicators
from src.analyzer.backtrader_wrapper.base_screener import BaseScreener
from src.analyzer.backtrader_wrapper.base_strategy import BaseStrategy
from src.analyzer.backtrader_wrapper.interface_algo import AlgoInterface


class SimpleMovingAverageAlgo(AlgoInterface):
    params = dict(maperiod=20)

    def qualifier(self, data, *args, **kwargs) -> dict:
        return {
            "sma": indicators.MovingAverageSimple(data, period=self.params.maperiod)
        }

    def shouldBuy(self, data, qualifiers: dict) -> bool:
        return data > qualifiers["sma"].lines.sma

    def shouldSell(self, data, qualifiers: dict) -> bool:
        return data < qualifiers["sma"].lines.sma


class SimpleMovingAverageStrategy(BaseStrategy, SimpleMovingAverageAlgo):
    pass


class SimpleMovingAverageScreener(BaseScreener, SimpleMovingAverageAlgo):
    pass
