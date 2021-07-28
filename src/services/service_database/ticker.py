from typing import List, Tuple
from pandas import DataFrame

from src.utils.dataframe_utils import DataFrameUtils
from src.services.service_database.base_table import BaseTable
from src.constants import Env


class Ticker(BaseTable):
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        super().__init__(["Symbol"], ["Symbol", "Dividended", "Splitted", "Updated"], env)
        self.__sql_init = f"""
            CREATE TABLE IF NOT EXISTS {__class__.__name__} (
                Symbol VARCHAR(5) NOT NULL,
                Dividended DATE NULL,
                Splitted DATE NULL,
                Updated DATE NULL,
                PRIMARY KEY (Symbol)
            ){" ENGINE=MEMORY" if env != Env.PRODUCETION else ""};
        """

        labels = ", ".join(self.columns)
        values = ", ".join(["%s" for _c in self.columns])
        self.__sql_create = f"""
            INSERT IGNORE INTO {__class__.__name__} ({labels})
            VALUES ({values});
        """

        self.__sql_read = f"""
            SELECT * FROM {__class__.__name__}
            Where Symbol =  %s;
        """

        set = ", ".join([f"{c} = %s" for c in ("Dividended", "Splitted", "Updated")])
        where = " AND ".join([f"{c} = %s" for c in ("Symbol",)])
        self.__sql_update = f"""
            UPDATE {__class__.__name__}
            SET {set}
            WHERE {where};
        """

        self.__sql_delete = f"""
            DELETE FROM {__class__.__name__}
            WHERE Symbol = %s;
        """

    async def init(self):
        await super().init()
        await self.executor.execute(self.__sql_init)     

    async def create(self, df: DataFrame):
        if not df.index.names.__eq__(self.indexes) or any([not df.columns.__contains__(c) for c in self.columns]):
            raise ValueError()

        await self.executor.writemany(self.__sql_create, df[self.columns].to_records(index=False).tolist())

    async def read(self, symbol):
        rows = await self.executor.read(self.__sql_read, [symbol])
        df = DataFrame.from_records(rows, columns=self.columns)        
        df.set_index(self.indexes, drop=False, inplace=True)
        return df[self.columns]

    async def update(self, df: DataFrame):
        df = df.copy()
        for c in ("Dividended", "Splitted", "Updated"):
            df[c] = df[c].astype(str).str[:10]

        await self.executor.writemany(self.__sql_update, df[["Dividended", "Splitted", "Updated", "Symbol"]].to_records(index=False).tolist())

    async def delete(self, *symbols: Tuple[str]):
        await self.executor.writemany(self.__sql_delete, [symbols])