from datetime import date
from pandas import DataFrame
from unittest import IsolatedAsyncioTestCase, main

from utils.asyncio_utils import T

from .history import History
from .ticker import Ticker
from src.constants import Env


class TestTicker(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.instance = Ticker(Env.TEST)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.instance.init()

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        sql = f"""
            DROP TABLE IF EXISTS {History.__name__};
            DROP TABLE IF EXISTS {self.instance.__class__.__name__};
        """
        await self.instance.executor.execute(sql)

    async def test_init(self):
        self.assertListEqual(self.instance.indexes, ["Symbol"])
        self.assertListEqual(self.instance.columns, ['Symbol', 'Dividended', 'Splitted', 'Updated'])

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
        input = DataFrame({'Dividended': [date(2021,5,3)], 'Splitted': [date(2021,5,3)],
                           'Updated': [date(2021,5,3)], 'Symbol': ['MSFT'], 'Something': ['Test']})
        input.set_index('Symbol', drop=False, inplace=True)
        await self.instance.create(input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.equals(input[self.instance.columns]))

    async def test_update(self):
        input = DataFrame({'Dividended': [date(2021,5,3)], 'Splitted': [date(2021,5,3)],
                           'Updated': [date(2021,5,3)], 'Symbol': ['MSFT'], 'Something': ['Test']})
        input.set_index('Symbol', drop=False, inplace=True)
        await self.instance.create(input)
        
        input.loc['MSFT', 'Updated'] = date(2021, 6 ,3)
        await self.instance.update(input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.equals(input[self.instance.columns]))

    async def test_delete(self):
        input = DataFrame({'Dividended': [date(2021,5,3)], 'Splitted': [date(2021,5,3)],
                           'Updated': [date(2021,5,3)], 'Symbol': ['MSFT']})
        input.set_index('Symbol', drop=False, inplace=True)
        await self.instance.create(input)

        df = await self.instance.read('MSFT')
        self.assertTrue(df.equals(input[self.instance.columns]))

if __name__ == "__main__":
    main()
