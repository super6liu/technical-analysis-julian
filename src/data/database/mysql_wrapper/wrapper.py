from typing import Iterable
from aiomysql import create_pool, Pool

from src.configs import Configs
from src.constants import Env


class Wrapper():
    __pool: Pool = None

    @staticmethod
    async def init(env: Env = Env.PRODUCETION):
        configs = Configs.configs("credentials", "mysql", env.value)
        Wrapper.__pool = await create_pool(user=configs('user'), db=configs('db'), host='127.0.0.1', password=configs('password'))
        return Wrapper()

    async def execute(self, sql: str):
        async with Wrapper.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

            await conn.commit()

    async def read(self, sql: str, params: Iterable):
        async with Wrapper.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                return await cur.fetchall()

    async def write(self, sql: str, params: Iterable):
        async with Wrapper.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)

            await conn.commit()

    async def writemany(self, sql: str, params: Iterable[Iterable]):
        async with Wrapper.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, params)

            await conn.commit()