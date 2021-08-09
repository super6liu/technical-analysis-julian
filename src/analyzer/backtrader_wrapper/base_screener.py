from backtrader import Analyzer, indicators


class BaseScreener(Analyzer):
    def start(self):
        self.bband = {data: self.qualifier(data) for data in self.datas}

    def stop(self):
        self.rets['buy'] = list()
        self.rets['sell'] = list()

        for data, band in self.bband.items():
            node = data._name, data.close[0]
            if self.shouldBuy(data, band):
                self.rets['buy'].append(node)
            elif self.shouldSell(data, band):
                self.rets['sell'].append(node)
