import datetime as dt

import pandas as pd
import matplotlib.pyplot as plt

import sql_client as db

pd.options.display.max_columns = 20
pd.options.display.width = 180

# start = '2021-03-13 23:33:00.000000'
# end = '2021-03-13 23:33:35.000000'
end = dt.datetime.utcnow()
timedelta = dt.timedelta(minutes=10)
start = end - timedelta

qry = "SELECT trade_id, time, type, size, price, taker_order_id from xlm_matches "
qry += f"WHERE time BETWEEN '{start}' AND '{end}' "
qry += "ORDER BY trade_id asc"
print(qry)

result = db.do_query(qry)

df = pd.DataFrame(result)
df.index = pd.DatetimeIndex(df['time'])
df.drop('time', axis=1, inplace=True)
cols = ['trade_id', 'type', 'size', 'price', 'taker_order_id']
df = df[cols]
df['buys'] = df['size'].where(df.type == 'buy')
df['sells'] = df['size'].where(df.type == 'sell')

all_trades = df[['buys', 'sells', 'price', 'size']]

fig, axes = plt.subplots(nrows=2, ncols=1, gridspec_kw={'height_ratios': [3, 1]})

big_trades = all_trades
big_trades.sells.plot(color='red', grid=True, label='Sell Size', style='.', ax=axes[0])
big_trades.buys.plot(color='green', grid=True, label='Buy Size', style='.', ax=axes[0])
big_trades.price.plot(color='black', grid=True, label='Price', ax=axes[1])

# df1.plot(ax=axes[0,0])
# df2.plot(ax=axes[0,1])
# plt.figure(figsize=(12,5))
# plt.xlabel('Number of requests every 10 minutes')

# ax3 = trades.price.plot(color='black', grid=True, secondary_y=True, label='Price', style='.', marker='o', alpha=0.2)
# h1, l1 = ax1.get_legend_handles_labels()
# h2, l2 = ax2.get_legend_handles_labels()


# plt.legend(h1+h2, l1+l2, loc=2)
plt.show()