class AlgoInterface():
    def qualifiers(self) -> dict:
        pass

    def shouldBuy(self, data, qualifiers: dict) -> bool:
        pass

    def shouldSell(self, data, qualifiers: dict) -> bool:
        pass
