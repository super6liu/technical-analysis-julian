from datetime import date
from unittest.case import skip
from pandas import DataFrame
from unittest import IsolatedAsyncioTestCase, main
from decimal import Decimal

from .history import History
from .ticker import Ticker
from src.constants import Env


class TestHistory(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.fk = Ticker(Env.TEST)
        cls.instance = History(Env.TEST)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.fk.init()
        ticker = DataFrame({'Dividended': [date(2021, 5, 3)], 'Splitted': [date(2021, 5, 3)],
                           'Updated': [date(2021, 5, 3)], 'Symbol': ['MSFT'], 'Something': ['Test']}).set_index('Symbol', drop=False)
        await self.fk.create(ticker)

        await self.instance.init()

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        sql = f"""
            DROP TABLE IF EXISTS {self.instance.__class__.__name__};
            DROP TABLE IF EXISTS {Ticker.__name__};
        """
        await self.instance.executor.execute(sql)

    async def test_init(self):
        self.assertEqual(self.instance.indexes, ["Symbol", "Date"])
        self.assertListEqual(self.instance.columns, [
                             "Symbol", "Date", "Open", "High", "Low", "Close", "Volume"])

        sql = f"""
            SELECT 1 FROM {self.instance.__class__.__name__};
        """
        raw = await self.instance.executor.read(sql, None)
        self.assertIsNotNone(raw)

    async def test_read(self):
        df = await self.instance.read('MSFT')
        self.assertTrue(df.empty)
        self.assertListEqual(df.columns.tolist(), self.instance.columns)

    #Series(map(lambda x: Decimal(x), df["Open"])).values == input.Open.values
    async def test_create(self):
        input = DataFrame({'Date': [date(2021, 5, 10)],
                           'Symbol': ['MSFT'],
                           'Open': [Decimal("250.87")],
                           'High': [Decimal("251.73")],
                           'Low': [Decimal("247.12")],
                           'Close': [Decimal("247.18")],
                           'Adj Close': [Decimal("246.61")],
                           'Volume': [29299900],
                           'Dividends': [Decimal("0")],
                           'Stock Splits': [Decimal("0")]
                           })
        input.set_index(["Symbol", "Date"], drop=False, inplace=True)
        await self.instance.create(input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.equals(input[self.instance.columns]))

    async def test_update_dividend(self):
        input = DataFrame({'Date': [date(2021, 5, 9), date(2021, 5, 10), date(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [Decimal("250.87"), Decimal("250.87"), Decimal("250.87")],
                           'High': [Decimal("251.73"), Decimal("251.73"), Decimal("251.73")],
                           'Low': [Decimal("247.12"), Decimal("247.12"), Decimal("247.12")],
                           'Close': [Decimal("247.18"), Decimal("247.18"), Decimal("247.18")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [Decimal("0"), Decimal("0"), Decimal("0")],
                           'Stock Splits': [Decimal("0"), Decimal("0"), Decimal("0")]
                           })
        input.set_index(["Symbol", "Date"], drop=False, inplace=True)
        await self.instance.create(input)

        await self.instance.update_dividend("MSFT", 0.5, date(2021, 5, 10))

        df = await self.instance.read('MSFT')
        input = DataFrame({'Date': [date(2021, 5, 9), date(2021, 5, 10), date(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [Decimal("250.87"), Decimal("250.36"), Decimal("250.36")],
                           'High': [Decimal("251.73"), Decimal("251.22"), Decimal("251.22")],
                           'Low': [Decimal("247.12"), Decimal("246.62"), Decimal("246.62")],
                           'Close': [Decimal("247.18"), Decimal("246.68"), Decimal("246.68")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [Decimal("0"), Decimal("0"), Decimal("0")],
                           'Stock Splits': [Decimal("0"), Decimal("0"), Decimal("0")]
                           })
        input.set_index(["Symbol", "Date"], drop=False, inplace=True)
        self.assertTrue(df.equals(input[self.instance.columns]))

    async def test_update_split(self):
        input = DataFrame({'Date': [date(2021, 5, 9), date(2021, 5, 10), date(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [Decimal("250.87"), Decimal("250.87"), Decimal("250.87")],
                           'High': [Decimal("251.73"), Decimal("251.73"), Decimal("251.73")],
                           'Low': [Decimal("247.12"), Decimal("247.12"), Decimal("247.12")],
                           'Close': [Decimal("247.18"), Decimal("247.18"), Decimal("247.18")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [Decimal("0"), Decimal("0"), Decimal("0")],
                           'Stock Splits': [Decimal("0"), Decimal("0"), Decimal("0")]
                           })
        input.set_index(["Symbol", "Date"], drop=False, inplace=True)
        await self.instance.create(input)

        await self.instance.update_split("MSFT", 2)

        df = await self.instance.read('MSFT')
        input = DataFrame({'Date': [date(2021, 5, 9), date(2021, 5, 10), date(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [Decimal("125.44"), Decimal("125.44"), Decimal("125.44")],
                           'High': [Decimal("125.87"), Decimal("125.87"), Decimal("125.87")],
                           'Low': [Decimal("123.56"), Decimal("123.56"), Decimal("123.56")],
                           'Close': [Decimal("123.59"), Decimal("123.59"), Decimal("123.59")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [Decimal("0"), Decimal("0"), Decimal("0")],
                           'Stock Splits': [Decimal("0"), Decimal("0"), Decimal("0")]
                           })
        input.set_index(["Symbol", "Date"], drop=False, inplace=True)
        self.assertTrue(df.equals(input[self.instance.columns]))

    @skip("mysql MEMORY ENGINE in Env.Test doesn't support Foreign Key constaint.")
    async def test_delete(self):
        input = DataFrame({'Date': [date(2021, 5, 10)],
                           'Symbol': ['MSFT'],
                           'Open': [Decimal("250.87")],
                           'High': [Decimal("251.73")],
                           'Low': [Decimal("247.12")],
                           'Close': [Decimal("247.18")],
                           'Adj Close': [Decimal("246.61")],
                           'Volume': [29299900],
                           'Dividends': [Decimal("0")],
                           'Stock Splits': [Decimal("0")]
                           })
        input.set_index(["Symbol", "Date"], drop=False, inplace=True)
        await self.instance.create(input)

        await self.fk.delete("MSFT")

        df = await self.instance.read('MSFT')
        self.assertTrue(df.empty)


if __name__ == "__main__":
    main()
