import pandas as pd
import asyncio

from src.services.service_database.mysql_wrapper.base import Base

# todo: async


class Ticker(Base):
    def __init__(self) -> None:
        super().__init__()

    async def init(self):
        sql = f"""
            CREATE TABLE IF NOT EXISTS {__class__.__name__} (
                Symbol VARCHAR(5) NOT NULL,
                Dividended DATE NULL,
                Splitted DATE NULL,
                Updated DATE NULL,
                PRIMARY KEY (Symbol)
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

    async def read(self, symbol):
        sql = f"""
            SELECT * FROM {__class__.__name__}
            Where Symbol =  %s;
        """
        return await self._read(sql, [symbol], index='Symbol', columns=('Symbol', 'Dividended', 'Splitted', 'Updated'), types=('str', 'datetime64[D]', 'datetime64[D]', 'datetime64[D]'))

    async def update(self, df: pd.DataFrame):
        sql = f"""
            UPDATE {__class__.__name__}
            SET Dividended = %s, Splitted = %s, Updated = %s
            WHERE Symbol = %s;
        """

        for c in df.columns:
            df[c] = df[c].astype(str).str[:10]
        await self._writemany(sql, df)

    async def delete(self, symbol):
        sql = f"""
            DELETE FROM {__class__.__name__}
            WHERE Symbol = %s;
        """
        await self._write(sql, [symbol])


if __name__ == '__main__':
    async def main():
        df = pd.DataFrame({'Dividended': ['2021-05-03'], 'Splitted': ['2021-05-03'],
                        'Updated': ['2021-05-03'], 'Symbol': ['MSFT'], 'Something': ['Test']}).set_index('Symbol')
        df.drop('Something', axis='columns', inplace=True)
        print(df)

        t = Ticker()
        await t.init()
        await t.create(df)
        print(await t.read('MSFT'))

        df = pd.DataFrame({'Symbol': ['MSFT'], 'Dividended': [
                        '2021-05-03'], 'Splitted': ['2021-05-04'], 'Updated': ['2021-06-03']}).set_index('Symbol')
        print(df)
        await t.update(df)
        print(await t.read('MSFT'))

        await t.delete('MSFT')
        print(await t.read('MSFT'))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
