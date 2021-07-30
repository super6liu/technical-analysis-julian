from typing import List, Tuple
from numpy import dtype
from pandas import DataFrame, Timestamp

from src.utils.dataframe_utils import DataFrameUtils
from src.services.service_database.base_table import BaseTable
from src.constants import Env


class Ticker(BaseTable):
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        super().__init__(None, ["Dividended", "Splitted", "Updated"], env)

    async def init(self):
        await super().init()

        sql = f"""
            CREATE TABLE IF NOT EXISTS {__class__.__name__} (
                Symbol VARCHAR(5) NOT NULL,
                Dividended TIMESTAMP NULL,
                Splitted TIMESTAMP NULL,
                Updated TIMESTAMP NULL,
                PRIMARY KEY (Symbol)
            ){" ENGINE=MEMORY" if self.env != Env.PRODUCETION else ""};
        """
        await self.executor.execute(sql)     

    async def create(self, symbol:str, df: DataFrame):
        if any([not df.columns.__contains__(c) for c in self.columns]):
            raise ValueError()

        labels = ", ".join(self.columns)
        values = ", ".join(["%s" for _c in self.columns])
        sql = f"""
            INSERT IGNORE INTO {__class__.__name__} (Symbol, {labels})
            VALUES ('{symbol}', {values});
        """
        await self.executor.writemany(sql, df[self.columns].to_records(index=False, column_dtypes="datetime64[D]").tolist())

    async def read(self, symbol: str):
        sql = f"""
            SELECT {", ".join(self.columns)} FROM {__class__.__name__}
            Where Symbol = '{symbol}';
        """
        rows = await self.executor.read(sql, [])
        df = DataFrame.from_records(rows, columns=self.columns)        
        return df

    async def update(self, symbol: str, df: DataFrame):
        set = ", ".join([f"{c} = %s" for c in self.columns])
        sql = f"""
            UPDATE {__class__.__name__}
            SET {set}
            WHERE Symbol = '{symbol}';
        """
        await self.executor.writemany(sql, df[self.columns].to_records(index=False, column_dtypes="datetime64[D]").tolist())

    async def delete(self, symbol: str):
        sql = f"""
            DELETE FROM {__class__.__name__}
            WHERE Symbol = '{symbol}';
        """
        await self.executor.execute(sql)