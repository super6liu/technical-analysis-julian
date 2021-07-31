import matplotlib.pyplot as plt
import talib as ta
import yfinance as yf

power = yf.Ticker("POWERGRID.NS")
df = power.history(start="2020-01-01", end='2020-09-04')
df.head()

df['MA'] = ta.SMA(df['Close'], 20)
df[['Close', 'MA']].plot(figsize=(12, 12))
plt.show()
