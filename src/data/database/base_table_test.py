from unittest import IsolatedAsyncioTestCase, main

from src.data.database.base_table import BaseTable
from src.constants import Env


class TestSymbolTable(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        class TestChildTable(BaseTable):
            def __init__(self, index, columns) -> None:
                super().__init__(index, columns, Env.TEST)

            async def create(self):
                pass

            async def read(self):
                pass

            async def update(self):
                pass

            async def delete(self):
                pass

        cls.instance = TestChildTable("id", ["id", "value"])

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.instance.init()
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self.instance.__class__.__name__} (
                id INT NOT NULL,
                value INT,
                PRIMARY KEY (id)
            ) ENGINE=MEMORY;
        """
        await self.instance.executor.execute(sql)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        sql = f"""
            DROP TABLE IF EXISTS {self.instance.__class__.__name__};
        """
        await self.instance.executor.execute(sql)

    async def test_init(self):
        sql = f"""
            SELECT 1 FROM {self.instance.__class__.__name__};
        """
        raw = await self.instance.executor.read(sql, None)
        self.assertIsNotNone(raw)


if __name__ == "__main__":
    main()
