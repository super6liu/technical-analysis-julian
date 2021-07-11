import os
import json
import pandas as pd
from aiomysql import create_pool, Pool

# todo: get credential from config

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
PATH = os.path.join(ROOT_DIR, 'credentials/mysql.json')
with open(PATH) as file:
    CREDENTIAL = json.load(file)


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
            Base.pool = await create_pool(user=CREDENTIAL['user'], db=CREDENTIAL['database'], host='127.0.0.1', password=CREDENTIAL['password'])


if __name__ == '__main__':
    pass
    # async def main():
    #     b = Base()
    #     print(await b.create('table', ['Date', 'Symbol'], pd.DataFrame({'Date': ['2021-07-07', '2021-07-06'], 'Symbol': ['MSFT', 'MSFT']})))
    #     return
    #     await b.__setup()
    #     x = pd.DataFrame({'Date': ['2021-07-07', '2021-07-06']})
    #     print(x)
    #     s = await b._read('SELECT * FROM History WHERE Date=%s', x)
    #     print(s)
    #     s = await b._write('UPDATE History SET Volume = 1 WHERE Date = "2021-07-07"', [None])
    #     print(s)

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
