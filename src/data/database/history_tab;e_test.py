from unittest.case import skip
from datetime import date
from numpy import float64, isclose
from pandas import DataFrame, Timestamp
from unittest import IsolatedAsyncioTestCase, main

from .history_table import HistoryTable
from .ticker_table import TickerTable
from src.constants import Env


class TestHistoryTable(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.fk = TickerTable(Env.TEST)
        cls.instance = HistoryTable(Env.TEST)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.fk.init()
        ticker = DataFrame({'Dividended': [Timestamp(2021, 5, 3)], 'Splitted': [Timestamp(2021, 5, 3)],
                           'Updated': [Timestamp(2021, 5, 3)], 'Symbol': ['MSFT'], 'Something': ['Test']}).set_index('Symbol', drop=False)
        await self.fk.create('MSFT', ticker)

        await self.instance.init()
        sql = f"""
            DELETE IGNORE FROM {self.instance.__class__.__name__};
        """
        await self.instance.executor.execute(sql)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        sql = f"""
            DROP TABLE IF EXISTS {self.instance.__class__.__name__};
            DROP TABLE IF EXISTS {TickerTable.__name__};
        """
        await self.instance.executor.execute(sql)

    async def test_init(self):
        self.assertEqual(self.instance.index, "Date")
        self.assertListEqual(self.instance.columns, ["Open", "High", "Low", "Close", "Volume"])

        sql = f"""
            SELECT 1 FROM {self.instance.__class__.__name__};
        """
        raw = await self.instance.executor.read(sql, None)
        self.assertIsNotNone(raw)

    async def test_read(self):
        df = await self.instance.read('MSFT')
        self.assertTrue(df.empty)
        self.assertListEqual(df.columns.tolist(), self.instance.columns)

    async def test_create(self):
        input = DataFrame({'Date': [Timestamp(2021, 5, 10)],
                           'Symbol': ['MSFT'],
                           'Open': [float64("250.87")],
                           'High': [float64("251.73")],
                           'Low': [float64("247.12")],
                           'Close': [float64("247.18")],
                           'Adj Close': [float64("246.61")],
                           'Volume': [29299900],
                           'Dividends': [float64("0")],
                           'Stock Splits': [float64("0")]
                           })
        input.set_index(["Date"], drop=False, inplace=True)
        await self.instance.create('MSFT', input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.index.equals(input.index))
        for c in ['Open', 'High', 'Low', 'Close']:
            self.assertTrue(all(isclose(df[c], input[c])))

    async def test_update_dividend(self):
        input = DataFrame({'Date': [Timestamp(2021, 5, 9), Timestamp(2021, 5, 10), Timestamp(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [float64("250.87"), float64("250.87"), float64("250.87")],
                           'High': [float64("251.73"), float64("251.73"), float64("251.73")],
                           'Low': [float64("247.12"), float64("247.12"), float64("247.12")],
                           'Close': [float64("247.18"), float64("247.18"), float64("247.18")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [float64("0"), float64("0"), float64("0")],
                           'Stock Splits': [float64("0"), float64("0"), float64("0")]
                           })
        input.set_index(["Date"], drop=False, inplace=True)
        await self.instance.create("MSFT", input)

        await self.instance.update_dividend("MSFT", 0.5, date(2021, 5, 10))

        df = await self.instance.read('MSFT')
        input = DataFrame({'Date': [Timestamp(2021, 5, 9), Timestamp(2021, 5, 10), Timestamp(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [float64("250.87"), float64("250.362536"), float64("250.362536")],
                           'High': [float64("251.73"), float64("251.220796"), float64("251.220796")],
                           'Low': [float64("247.12"), float64("246.620121"), float64("246.620121")],
                           'Close': [float64("247.18"), float64("246.68"), float64("246.68")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [float64("0"), float64("0"), float64("0")],
                           'Stock Splits': [float64("0"), float64("0"), float64("0")]
                           })
        input.set_index(["Date"], drop=False, inplace=True)
        self.assertTrue(df.index.equals(input.index))
        for c in ['Open', 'High', 'Low', 'Close']:
            self.assertTrue(all(isclose(df[c], input[c])))

    async def test_update_split(self):
        input = DataFrame({'Date': [Timestamp(2021, 5, 9), Timestamp(2021, 5, 10), Timestamp(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [float64("250.87"), float64("250.87"), float64("250.87")],
                           'High': [float64("251.73"), float64("251.73"), float64("251.73")],
                           'Low': [float64("247.12"), float64("247.12"), float64("247.12")],
                           'Close': [float64("247.18"), float64("247.18"), float64("247.18")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [float64("0"), float64("0"), float64("0")],
                           'Stock Splits': [float64("0"), float64("0"), float64("0")]
                           })
        input.set_index(["Date"], drop=False, inplace=True)
        await self.instance.create("MSFT", input)

        await self.instance.update_split("MSFT", 2)

        df = await self.instance.read('MSFT')
        input = DataFrame({'Date': [Timestamp(2021, 5, 9), Timestamp(2021, 5, 10), Timestamp(2021, 5, 11)],
                           'Symbol': ['MSFT', 'MSFT', 'MSFT'],
                           'Open': [float64("125.435"), float64("125.435"), float64("125.435")],
                           'High': [float64("125.865"), float64("125.865"), float64("125.865")],
                           'Low': [float64("123.56"), float64("123.56"), float64("123.56")],
                           'Close': [float64("123.59"), float64("123.59"), float64("123.59")],
                           'Volume': [29299900, 29299900, 29299900],
                           'Dividends': [float64("0"), float64("0"), float64("0")],
                           'Stock Splits': [float64("0"), float64("0"), float64("0")]
                           })
        input.set_index(["Date"], drop=False, inplace=True)
        self.assertTrue(df.index.equals(input.index))
        for c in ['Open', 'High', 'Low', 'Close']:
            self.assertTrue(all(isclose(df[c], input[c])))

    @skip("mysql MEMORY ENGINE in Env.Test doesn't support Foreign Key constaint.")
    async def test_delete(self):
        input = DataFrame({'Date': [Timestamp(2021, 5, 10)],
                           'Symbol': ['MSFT'],
                           'Open': [float64("250.87")],
                           'High': [float64("251.73")],
                           'Low': [float64("247.12")],
                           'Close': [float64("247.18")],
                           'Adj Close': [float64("246.61")],
                           'Volume': [29299900],
                           'Dividends': [float64("0")],
                           'Stock Splits': [float64("0")]
                           })
        input.set_index(["Date"], drop=False, inplace=True)
        await self.instance.create(input)

        await self.fk.delete("MSFT")

        df = await self.instance.read('MSFT')
        self.assertTrue(df.empty)


if __name__ == "__main__":
    main()
