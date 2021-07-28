from decimal import Decimal
from pandas import DataFrame
from datetime import date

from src.services.service_database.base_table import BaseTable
from src.constants import Env


class History(BaseTable):
    def __init__(self, env: Env = Env.PRODUCETION) -> None:
        super().__init__(["Symbol", "Date"], ["Symbol", "Date",
                                              "Open", "High", "Low", "Close", "Volume"], env)
        self.__sql_init = f"""
            CREATE TABLE IF NOT EXISTS {__class__.__name__} (
                Symbol VARCHAR(5) NOT NULL,
                Date DATE NOT NULL,
                Open DECIMAL(9, 2) UNSIGNED NOT NULL,
                High DECIMAL(9, 2) UNSIGNED NOT NULL,
                Low DECIMAL(9, 2) UNSIGNED NOT NULL,
                Close DECIMAL(9, 2) UNSIGNED NOT NULL,
                Volume BIGINT UNSIGNED NOT NULL,
                PRIMARY KEY (Symbol, Date),
                FOREIGN KEY (Symbol)
                    REFERENCES Ticker(Symbol)
                    ON DELETE CASCADE
                    ON UPDATE RESTRICT
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
            Where Symbol = %s AND Date >= %s AND Date <= %s;
        """

        self.__sql_update_dividend = f"""
            UPDATE {__class__.__name__}
            SET Open = Open * (@factor := (Close - %s) / Close),
                High = High * @factor,
                Low = Low * @factor,
                Close = Close - %s
            WHERE Symbol = %s AND Date >= %s;
        """

        self.__sql_update_split = f"""
            UPDATE {__class__.__name__}
            SET Open = Open / (@split := %s),
                High = High / @split,
                Low = Low / @split,
                Close = Close / @split
            WHERE Symbol = %s;
        """

    async def init(self):
        await super().init()
        await self.executor.execute(self.__sql_init)

    async def create(self, df: DataFrame):
        if not df.index.names.__eq__(self.indexes) or any([not df.columns.__contains__(c) for c in self.columns]):
            raise ValueError()

        df = df.copy()
        df['Date'] = df['Date'].astype(str).str[:10]

        await self.executor.writemany(self.__sql_create, df[self.columns].to_records(index=False).tolist())

    async def read(self, symbol, start='1000-01-01', end=date.today()):
        rows = await self.executor.read(self.__sql_read, [symbol, start, end])
        df = DataFrame.from_records(rows, columns=self.columns)
        df.set_index(self.indexes, drop=False, inplace=True)
        return df[self.columns]

    async def update(self, *args, **kwargs):
        raise NotImplementedError()

    async def update_dividend(self, symbol: str, dividend: Decimal, start: date):
        await self.executor.write(self.__sql_update_dividend, [dividend, dividend, symbol, start])

    async def update_split(self, symbol: str, split: Decimal):
        await self.executor.write(self.__sql_update_split, [split, symbol])

    async def delete(self, *args, **kwargs):
        raise NotImplementedError("Should delete from Ticker on FK.")
