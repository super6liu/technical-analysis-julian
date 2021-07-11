import yfinance as yf
from datetime import date
import requests_cache
import pandas as pd


'''
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

def auto_adjust(data):
    df = data.copy()
    ratio = df["Close"] / df["Adj Close"]
    df["Adj Open"] = df["Open"] / ratio
    df["Adj High"] = df["High"] / ratio
    df["Adj Low"] = df["Low"] / ratio

    df.drop(
        ["Open", "High", "Low", "Close"],
        axis=1, inplace=True)

    df.rename(columns={
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low", "Adj Close": "Close"
    }, inplace=True)

    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df[["Open", "High", "Low", "Close", "Volume"]]
'''


class YfinanceWrapper:
    def __init__(self) -> None:
        self.__session = requests_cache.CachedSession('yfinance.cache')
        self.__session.headers['User-agent'] = 'technical-analysis-julian/1.0'

    def history(self, symbol: str, start: date = None, end: date = None):
        if not start is None and end is None:
            end = date.today()

        ticker = yf.Ticker(symbol, self.__session)
        df = ticker.history(period='max', start=start,
                            end=end, rounding=True)

        # df.reset_index(inplace=True)
        # print(df, df.columns)
        # df.index.freq = 'd'
        print(df)
        # df.set_index('Date', inplace=True)
        print(df, df.index.is_type_compatible('datetime64[D]'))
        # df.insert(loc=0, column='Symbol', value=symbol)
        # df['Date'] = df.index
        return df


if __name__ == "__main__":
    y = YfinanceWrapper()
    df = y.history('MSFT', '2021-05-15', '2021-05-19')  # '2021-05-21'
    # print(df)
    # print(pd.isna(df.iloc[-1]['Close']) == True)
    # print(df.index)
