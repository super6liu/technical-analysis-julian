from datetime import date, timedelta
from unittest import IsolatedAsyncioTestCase, main
from unittest.case import skip

from numpy import float64
from pandas import Timestamp
from src.data.histories import Histories

'''
auto_adjust

False
            Open    High     Low   Close  Adj Close    Volume  Dividends  Stock Splits
Date
2021-05-10  250.87  251.73  247.12  247.18     246.61  29299900       0.00             0
2021-05-11  244.55  246.60  242.57  246.23     245.66  33641600       0.00             0
2021-05-12  242.17  244.38  238.07  239.00     238.45  36684400       0.00             0
2021-05-13  241.80  245.60  241.42  243.03     242.47  29624300       0.00             0
2021-05-14  245.58  249.18  245.49  248.15     247.58  23901100       0.00             0
2021-05-17  246.55  246.59  243.52  245.18     244.62  24970200       0.00             0
2021-05-18  246.27  246.41  242.90  243.08     242.52  20168000       0.00             0
2021-05-19  239.31  243.23  238.60  243.12     243.12  25739800       0.56             0

True
            Open    High     Low   Close    Volume  Dividends  Stock Splits
Date
2021-05-10  250.29  251.15  246.55  246.61  29299900       0.00             0
2021-05-11  243.99  246.03  242.01  245.66  33641600       0.00             0
2021-05-12  241.61  243.82  237.52  238.45  36684400       0.00             0
2021-05-13  241.24  245.03  240.86  242.47  29624300       0.00             0
2021-05-14  245.01  248.61  244.92  247.58  23901100       0.00             0
2021-05-17  245.98  246.02  242.96  244.62  24970200       0.00             0
2021-05-18  245.70  245.84  242.34  242.52  20168000       0.00             0
2021-05-19  239.31  243.23  238.60  243.12  25739800       0.56             0
'''


class TestHistoryService(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.__hs = Histories()

    async def test_schema(self):
        df = await self.__hs.history('MSFT', '2021-05-17', '2021-05-20')
        self.assertEqual(len(df), 3)
        self.assertEqual(df.index.name, 'Date')
        self.assertListEqual(df.columns.tolist(), [
                             'Open', 'High', 'Low', 'Close', 'Volume'])
        self.assertTrue(isinstance(df.index[0], Timestamp))
        self.assertTrue(isinstance(df['Open'][0], float64))

    '''
                    Symbol    Open    High     Low   Close      Volume  Dividends  Stock Splits
    Date                                                                                  
    2021-05-17   MSFT  245.98  246.02  242.96  244.62  24970200.0        0.0             0
    2021-05-17   MSFT  245.98  246.02  242.96  244.62  24970201.0        0.0             0
    '''
    @skip
    async def test_drop_duplicates(self):
        start = date.today()
        day_of_week = start.isoweekday()
        if day_of_week > 5:
            start += timedelta(5 - day_of_week)
        df = await self.__hs.history('MSFT', start)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)

    '''
                Symbol    Open    High     Low   Close      Volume  Dividends  Stock Splits
    Date                                                                                  
    2021-05-17   MSFT  245.98  246.02  242.96  244.62  24970200.0        0.0             0
    2021-05-18   MSFT  245.70  245.84  242.34  242.52  20168000.0        0.0             0
    2021-05-19   MSFT  NA      NA      NA      NA      NA                0.56            0
    '''
    async def test_drop_na(self):
        df = await self.__hs.history('MSFT', '2021-05-17', '2021-05-19')
        self.assertEqual(len(df), 2)


if __name__ == "main":
    main()
