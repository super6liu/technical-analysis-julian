import asyncio
import pandas as pd
from aiomysql import create_pool, Pool
from os import environ

from src.configs import Configs
from src.utils.asyncio_utils import run_async_main


class Base():
    pool: Pool = None

    async def _execute(self, sql):
        await self.__setup()

        async with Base.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

            await conn.commit()

    async def _read(self, sql, params, index, columns):
        await self.__setup()
        async with Base.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
                return pd.DataFrame.from_records(rows, index=index, columns=columns)

    async def _write(self, sql, params: list):
        await self.__setup()
        async with Base.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)

            await conn.commit()

    async def _writemany(self, sql, params: pd.DataFrame):
        await self.__setup()
        async with Base.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, params.to_records(index=False).tolist())

            await conn.commit()

    async def __setup(self):
        if Base.pool is None:
            if environ["DEBUG"]:
                configs = Configs.configs("credentials", "mysql", "test")
            else:
                configs = Configs.configs("credentials", "mysql", "production")
            Base.pool = await create_pool(user=configs('user'), db=configs('db'), host='127.0.0.1', password=configs('password'))


if __name__ == '__main__':
    async def main():
        b = Base()
        # x = pd.DataFrame({'Date': ['2021-07-07', '2021-07-06']})
        # print(x)
        # s = await b._read('SELECT * FROM History WHERE Date=%s', x)
        # print(s)
        # s = await b._write('UPDATE History SET Volume = 1 WHERE Date = "2021-07-07"', [None])
        # print(s)

    run_async_main(main)
