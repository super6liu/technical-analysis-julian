from datetime import date
from pandas import DataFrame, Timestamp
from unittest import IsolatedAsyncioTestCase, main

from utils.asyncio_utils import T

from .history_table import HistoryTable
from .ticker_table import TickerTable
from src.constants import Env


class TestTickerTable(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.instance = TickerTable(Env.TEST)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.instance.init()
        sql = f"""
            DELETE IGNORE FROM {self.instance.__class__.__name__};
        """
        await self.instance.executor.execute(sql)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        sql = f"""
            DROP TABLE IF EXISTS {HistoryTable.__name__};
            DROP TABLE IF EXISTS {self.instance.__class__.__name__};
        """
        await self.instance.executor.execute(sql)

    async def test_init(self):
        self.assertIsNone(self.instance.index)
        self.assertListEqual(self.instance.columns, ['Dividended', 'Splitted', 'Updated'])

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
        input = DataFrame({'Dividended': [Timestamp(2021, 5, 3)], 'Splitted': [Timestamp(2021, 5, 3)],
                           'Updated': [Timestamp(2021, 5, 3)], 'Symbol': ['MSFT'], 'Something': ['Test']})
        await self.instance.create('MSFT', input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.equals(input[self.instance.columns]))

    async def test_update(self):
        input = DataFrame({'Dividended': [Timestamp(2021, 5, 3)], 'Splitted': [Timestamp(2021, 5, 3)],
                           'Updated': [Timestamp(2021, 5, 3)], 'Symbol': ['MSFT'], 'Something': ['Test']})
        await self.instance.create("MSFT", input)
        
        input.loc[0, 'Updated'] = Timestamp(2021, 6, 3)
        await self.instance.update("MSFT", input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.equals(input[self.instance.columns]))

    async def test_delete(self):
        input = DataFrame({'Dividended': [Timestamp(2021, 5, 3)], 'Splitted': [Timestamp(2021, 5, 3)],
                           'Updated': [Timestamp(2021, 5, 3)], 'Symbol': ['MSFT']})
        await self.instance.create('MSFT', input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.equals(input[self.instance.columns]))

if __name__ == "__main__":
    main()
