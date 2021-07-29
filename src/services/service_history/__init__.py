from .yfinance_wrapper import YfinanceWrapper as HistoryService

"""
panda.DataFrame:

                  Symbol        Date        Open        High         Low       Close            Volume Dividends Stock Splits
Symbol Date 
str  datetime.date str datetime.date        Decimal     Decimal      Decimal    Decimal         Decimal Decimal  Decimal

Note: 
    - Decimal of 6 digit precision.
"""