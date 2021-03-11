import datetime as dt

import cbpro
import mplfinance as mpf
import pandas as pd

cb_client = cbpro.PublicClient()

product = 'XLM-USD'
candle_seconds = 300
num_candles = 100

# calculate candle begin and end time
ten_pm_utc = dt.datetime(year=2021, month=2, day=27, hour=22)
now = dt.datetime.utcnow()
time_span = dt.timedelta(seconds=candle_seconds * num_candles)  # the time candle_seconds ago
start_time = now - time_span
end_time = now

candles = cb_client.get_product_historic_rates(product, granularity=candle_seconds,
                                               start=start_time, end=end_time)

# print("\n********************************\nNumber of candles: ", len(candles))
# print()
# for candle in candles:
#     candle_timestamp, low, high, open_price, close_price, volume = candle
#     candle_time = dt.datetime.utcfromtimestamp(candle_timestamp)
#     print(candle_time, ":", volume)

candle_df = pd.DataFrame(candles)
candle_df.columns = ['Time', 'Low', 'High', 'Open', 'Close', 'Volume']
candle_df.index = pd.to_datetime(candle_df['Time'], unit='s')
candle_df.drop(labels='Time', axis=1, inplace=True)
candle_df.sort_index(inplace=True)

mpf.plot(candle_df, type='candle', volume=True)