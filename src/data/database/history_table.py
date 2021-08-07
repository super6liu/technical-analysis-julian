from datetime import date
from typing import Tuple

from pandas import DataFrame
from src.data.database.interface_executor import ExecutorInterface
from src.data.database.interface_table import TableInterface
from src.utils.asyncio_utils import run_async_main
from src.utils.env_utils import is_test


class HistoryTable(TableInterface):
    def __init__(self, executor: ExecutorInterface) -> None:
        self.executor = executor
        self.index = "Date"
        self.columns = ["Open", "High", "Low", "Close", "Volume"]

    async def init(self):
        sql = f"""
            CREATE TABLE IF NOT EXISTS {__class__.__name__} (
                Symbol VARCHAR(5) NOT NULL,
                Date TIMESTAMP NOT NULL,
                Open DOUBLE NOT NULL,
                High DOUBLE NOT NULL,
                Low DOUBLE NOT NULL,
                Close DOUBLE NOT NULL,
                Volume BIGINT NOT NULL,
                PRIMARY KEY (Symbol, Date)
            ){" ENGINE=MEMORY" if is_test() else ""};
        """
        await self.executor.execute(sql)

    async def insert(self, symbol: str, df: DataFrame):
        if not df.index.names.__eq__(self.index) or any([not df.columns.__contains__(c) for c in self.columns]):
            raise ValueError()

        labels = ", ".join(self.columns)
        values = ", ".join(["%s" for _c in self.columns])
        sql = f"""
            INSERT IGNORE INTO {__class__.__name__} (Symbol, Date, {labels})
            VALUES ('{symbol}', %s, {values});
        """

        await self.executor.writemany(sql, df[self.columns].to_records(index=True, index_dtypes="datetime64[D]").tolist())

    async def read(self, symbol: str, start: date = date(1000, 1, 1), end: date = date.today()):
        sql = f"""
            SELECT Date, {", ".join(self.columns)}  FROM {__class__.__name__}
            Where Symbol = '{symbol}' AND Date >= %s AND Date <= %s;
        """
        rows = await self.executor.read(sql, [start, end])
        df = DataFrame.from_records(rows, index=self.index, columns=[
                                    self.index] + self.columns)
        return df

    async def read_first(self, symbol: str):
        sql = f"""
            SELECT Date, {", ".join(self.columns)} FROM {__class__.__name__}
            Where Symbol = %s 
            LIMIT 1;
        """
        rows = await self.executor.read(sql, [symbol])
        df = DataFrame.from_records(rows, index=self.index, columns=[
                                    self.index] + self.columns)
        return df

    async def read_last(self, symbol: str):
        sql = f"""
            SELECT Date, {", ".join(self.columns)} FROM {__class__.__name__} AS T1
            INNER JOIN (
                SELECT MAX(DATE) as maxDate, Symbol FROM {__class__.__name__}
                Where Symbol = %s
            ) AS T2 
            ON T1.Symbol = T2.Symbol AND T1.Date = T2.maxDate;
        """
        rows = await self.executor.read(sql, [symbol])
        df = DataFrame.from_records(rows, index=self.index, columns=[
                                    self.index] + self.columns)
        return df

    async def read_symbols(self) -> Tuple[str]:
        sql = f"""
            SELECT DISTINCT(Symbol) AS Symbol FROM {__class__.__name__}
        """
        rows = await self.executor.read(sql, [])
        return tuple(map(lambda x: x[0], rows))

    async def update(self, symbol: str, factor: float):
        sql = f"""
            UPDATE {__class__.__name__}
            SET Open = Open * %s,
                High = High * %s,
                Low = Low * %s,
                Close = Close * %s
            WHERE Symbol = '{symbol}';
        """
        await self.executor.write(sql, [factor, factor, factor, factor])

    async def delete(self, symbol: str):
        sql = f"""
            DELETE FROM {__class__.__name__}
            WHERE Symbol = '{symbol}';
        """
        await self.executor.execute(sql)


if __name__ == "__main__":
    async def main():
        h = HistoryTable()
        await h.init()
        df = await h.read_last('BANR')
        print(df)

    run_async_main(main)
