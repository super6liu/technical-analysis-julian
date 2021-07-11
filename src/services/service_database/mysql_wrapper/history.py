from numpy import number
import pandas as pd
import datetime
import asyncio

from src.services.service_database.mysql_wrapper.base import Base


class History(Base):
    def __init__(self) -> None:
        super().__init__()

    async def init(self):
        sql = f"""
            CREATE TABLE IF NOT EXISTS {__class__.__name__} (
                Date DATE NOT NULL,
                Symbol VARCHAR(5) NOT NULL,
                Open DECIMAL(9, 2) UNSIGNED NOT NULL,
                High DECIMAL(9, 2) UNSIGNED NOT NULL,
                Low DECIMAL(9, 2) UNSIGNED NOT NULL,
                Close DECIMAL(9, 2) UNSIGNED NOT NULL,
                Volume INT UNSIGNED NOT NULL,
                PRIMARY KEY (Date, Symbol)
            );
        """
        await self._execute(sql)

    async def create(self, df: pd.DataFrame):
        columns = df.columns.tolist()
        labels = ",".join(df.index.names + columns)
        values = ",".join(["%s" for c in (df.index.names + columns)])
        sql = f"""
            INSERT IGNORE INTO {__class__.__name__} ({labels})
            VALUES ({values});
        """
        await self._writemany(sql, df)

    async def read(self, symbol, start='2000-01-01', end=datetime.date.today()):
        sql = f"""
            SELECT * FROM {__class__.__name__}
            Where Symbol = %s AND Date >= %s AND Date <= %s;
        """
        return await self._read(sql, [symbol, start, end], index="Date", columns=("Date", 'Symbol', 'Open', "High", 'Low', "Close", 'Volume'), types=[])

    # async def update(self, df):
    #     sql = f"""
    #         UPDATE {__class__.__name__} AS h, temp AS t
    #         WHERE h.Date = t.Date AND h.Symbol = t.Symbol
    #         SET h.Open = t.Open, h.High = t.High, h.Low = t.Low, h.Close = t.Close;
    #     """
    #     await self._read(sql)

    async def update_dividend(self, symbol: str, dividend: number, start):
        sql = f"""
            UPDATE {__class__.__name__}
            SET Open = Open * (@factor := (Close - %s) / Close),
                High = High * @factor,
                Low = Low * @factor,
                Close = Close - %s
            WHERE Symbol = %s AND Date > %s;
        """
        await self._write(sql, [dividend, dividend, symbol, start])

    async def update_split(self, symbol: str, split: number):
        sql = f"""
            UPDATE {__class__.__name__}
            SET Open = Open / (@split := %s),
                High = High / @split,
                Low = Low / @split,
                Close = Close / @split
            WHERE Symbol = %s;
        """
        await self._write(sql, [split, symbol])

    async def delete(self, symbol):
        sql = f"""
            DELETE FROM {__class__.__name__}
            WHERE Symbol = %s;
        """
        await self._write(sql, [symbol])


if __name__ == '__main__':
    async def main():
        df = pd.DataFrame({'Date': ['2021-05-10'],
                       'Symbol': ['MSFT'],
                       'Open': [250.87],
                       'High': [251.73],
                       'Low': [247.12],
                       'Close': [247.18],
                       'Adj Close': [246.61],
                       'Volume': [29299900],
                       'Dividends': [0],
                       'Stock Splits': [0]
                       }).set_index('Date')
        df.drop(['Adj Close', 'Dividends', 'Stock Splits'],
                axis='columns', inplace=True)
        print(df)

        c = History()
        await c.init()
        await c.create(df)
        print(await c.read('MSFT'))

        await c.update_dividend('MSFT', 0.56, '2020-01-01')
        print(await c.read('MSFT'))

        await c.update_split('MSFT', 2)
        print(await c.read('MSFT'))

        await c.delete('MSFT')
        print(await c.read('MSFT'))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
