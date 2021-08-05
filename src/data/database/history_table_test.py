from unittest import IsolatedAsyncioTestCase, main

from numpy import isclose
from pandas import read_csv
from src.constants import Env
from src.data.database.history_table import HistoryTable
from src.data.database.mysql_wrapper.wrapper import Wrapper
from src.utils.env_utils import set_env


class TestHistoryTable(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        set_env(Env.TEST)
        cls.executor = Wrapper()
        cls.instance = HistoryTable(cls.executor)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.executor.setUp()
        await self.instance.init()

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        sql = f"""
            DROP TABLE IF EXISTS {self.instance.__class__.__name__};
        """
        await self.instance.executor.execute(sql)
        await self.executor.tearDown()

    async def test_init(self):
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
        input = read_csv(__file__.replace(".py", ".csv"), index_col="Date")
        await self.instance.insert('MSFT', input)

        output = await self.instance.read('MSFT')
        self.assertTrue(isclose(input, output).all())

    async def test_read_symbols(self):
        input = read_csv(__file__.replace(".py", ".csv"), index_col="Date")
        await self.instance.insert('MSFT', input)

        output = await self.instance.read_symbols()
        self.assertTupleEqual(output, ('MSFT',))

    async def test_read_last(self):
        input = read_csv(__file__.replace(".py", ".csv"), index_col="Date")
        await self.instance.insert('MSFT', input)

        output = await self.instance.read_last('MSFT')
        self.assertTrue(isclose(input.tail(1), output).all())

    async def test_update(self):
        input = read_csv(__file__.replace(".py", ".csv"), index_col="Date")
        await self.instance.insert('MSFT', input)

        await self.instance.update('MSFT', 0.5)

        output = await self.instance.read('MSFT')
        input[["Open", "High", "Low", "Close"]] = input[["Open", "High", "Low", "Close"]] * 0.5
        self.assertTrue(isclose(input, output).all())


    async def test_delete(self):
        input = read_csv(__file__.replace(".py", ".csv"), index_col="Date")
        await self.instance.insert('MSFT', input)

        await self.instance.delete('MSFT')

        output = await self.instance.read('MSFT')
        self.assertTrue(output.empty)


if __name__ == "__main__":
    main()
