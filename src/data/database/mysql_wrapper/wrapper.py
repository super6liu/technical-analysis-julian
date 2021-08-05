from typing import Iterable

from aiomysql import create_pool
from numpy import float64
from pandas import Timestamp
from pymysql import converters
from src.configs import CREDENTIALS
from src.data.database.interface_executor import ExecutorInterface
from src.utils.env_utils import get_env

converters.encoders[float64] = converters.escape_float
converters.encoders[Timestamp] = converters.escape_datetime
converters.conversions = converters.encoders.copy()
converters.conversions.update(converters.decoders)


class Wrapper(ExecutorInterface):
    def __init__(self) -> None:
        self.__pool = None

    async def setUp(self):
        if not self.__pool:
            credential = CREDENTIALS["mysql"][get_env().value]
            self.__pool = await create_pool(user=credential['user'], db=credential['db'], host='127.0.0.1', password=credential['password'], conv=converters.decoders)

    async def tearDown(self):
        if self.__pool:
            self.__pool.close()
            await self.__pool.wait_closed()
            self.__pool = None

    async def execute(self, sql: str):
        async with self.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

            await conn.commit()

    async def read(self, sql: str, params: Iterable):
        async with self.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                return await cur.fetchall()

    async def write(self, sql: str, params: Iterable):
        async with self.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)

            await conn.commit()

    async def writemany(self, sql: str, params: Iterable[Iterable]):
        async with self.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, params)

            await conn.commit()
