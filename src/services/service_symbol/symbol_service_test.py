from unittest import IsolatedAsyncioTestCase, main

from src.services.service_symbol import SymbolService

class TestSymbolService(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.__ss = SymbolService()
        return super().setUpClass()

    async def test_symbols(self):
        symbols = list(await self.__ss.symbols())
        self.assertGreaterEqual(len(symbols), 6000)
        self.assertFalse(any(" " in s for s in symbols))
        self.assertFalse(any(s.upper() != s for s in symbols))
        self.assertFalse(any(len(s) > 5 for s in symbols))             
        self.assertIn('MSFT', symbols)
        self.assertIn('DIDI', symbols)


if __name__ == "main":
    main()
