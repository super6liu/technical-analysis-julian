from decimal import Decimal
from numpy import datetime64
from pandas import DataFrame
from datetime import date, datetime

from pandas._libs.tslibs.timestamps import Timestamp

from src.services.service_database.base_table import BaseTable
from src.constants import Env


class History(BaseTable):
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        super().__init__("Date", ["Open", "High", "Low", "Close", "Volume"], env)  


    async def init(self):
        await super().init()

        sql = f"""
            CREATE TABLE IF NOT EXISTS {__class__.__name__} (
                Symbol VARCHAR(5) NOT NULL,
                Date TIMESTAMP NOT NULL,
                Open DOUBLE NOT NULL,
                High DOUBLE NOT NULL,
                Low DOUBLE NOT NULL,
                Close DOUBLE NOT NULL,
                Volume BIGINT NOT NULL,
                PRIMARY KEY (Symbol, Date),
                FOREIGN KEY (Symbol)
                    REFERENCES Ticker(Symbol)
                    ON DELETE CASCADE
                    ON UPDATE RESTRICT
            ){" ENGINE=MEMORY" if self.env != Env.PRODUCETION else ""};
        """
        await self.executor.execute(sql)

    async def create(self, symbol: str, df: DataFrame):
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
        df = DataFrame.from_records(rows, index=self.index, columns=[self.index] + self.columns)
        return df

    async def update(self, *args, **kwargs):
        raise NotImplementedError()

    async def update_dividend(self, symbol: str, dividend: Decimal, start: date):
        sql = f"""
            UPDATE {__class__.__name__}
            SET Open = Open * (@factor := (Close - %s) / Close),
                High = High * @factor,
                Low = Low * @factor,
                Close = Close - %s
            WHERE Symbol = '{symbol}' AND Date >= %s;
        """
        await self.executor.write(sql, [dividend, dividend, start])

    async def update_split(self, symbol: str, split: Decimal):
        sql = f"""
            UPDATE {__class__.__name__}
            SET Open = Open / (@split := %s),
                High = High / @split,
                Low = Low / @split,
                Close = Close / @split
            WHERE Symbol = '{symbol}';
        """
        await self.executor.write(sql, [split])

    async def delete(self, *args, **kwargs):
        raise NotImplementedError("Should delete from Ticker on FK.")
