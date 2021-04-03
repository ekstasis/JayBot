import datetime as dt

import pandas as pd
import matplotlib.pyplot as plt

import sql_client as db

pd.options.display.max_columns = 20
pd.options.display.width = 180

start_date = '2021-04-03'
end_date = start_date
# end_date = '2021-04-02'

start_time = '17:00:00.000000'
end_time =   '19:00:00.000000'

start = f'{start_date} {start_time}'
# end = f'{end_date} {end_time}'
end = dt.datetime.utcnow()
# timedelta = dt.timedelta(minutes=10)
# start = end - timedelta

qry = "SELECT * from matches_xlm "
qry += f"WHERE time BETWEEN '{start}' AND '{end}' "
qry += "ORDER BY trade_id asc"
print(qry)

conn = db.connection('debian_from_mac')

result = db.do_query_with(conn, qry)

df = pd.DataFrame(result)
df.index = pd.DatetimeIndex(df['time'])
df.drop('time', axis=1, inplace=True)
cols = ['trade_id', 'type', 'size', 'price', 'maker_order_id', 'taker_order_id']
df = df[cols]
df[["price", "size"]] = df[["price", "size"]].apply(pd.to_numeric)
df['maker_buys'] = df['size'].where(df.type == 'sell')
df['maker_sells'] = df['size'].where(df.type == 'buy')
df['cum_sum_maker_buy'] = df['maker_buys'].cumsum().fillna(method='ffill')
df['cum_sum_maker_sell'] = df['maker_sells'].cumsum().fillna(method='ffill')
# df['pct'] = df.price.pct_change()

# df_secs = df.resample('H')

# sums = df_secs.sum()[['buys', 'sells']]
# counts = df_secs.count()[['buys', 'sells']]
# price_plot = df_secs.mean()['price']
# counts['price'] = price_plot

# fig, axes = plt.subplots(nrows=3, ncols=1, gridspec_kw={'height_ratios': [1, 1, 1]})
fig, axes = plt.subplots(nrows=2, ncols=1, gridspec_kw={'height_ratios': [1, 1]})

diff = True
if diff:
    (df.cum_sum_maker_buy - df.cum_sum_maker_sell).plot(color='green', grid=True, style='.', ax=axes[0])
else:
    df['cum_sum_maker_sell'].plot(color='red', grid=True, style='.', ax=axes[0])
    df['cum_sum_maker_buy'].plot(color='green', grid=True, style='.', ax=axes[0])

# df['maker_buys'].plot(color='green', grid=True, style='.', ax=axes[0])
# df['maker_sells'].plot(color='red', grid=True, style='.', ax=axes[1])
df['price'].plot(grid=True, ax=axes[1])
# df[['buys', 'sells']].plot(grid=True, style='.', ax=axes[0])
# count_diffs.plot(kind='bar', color='red', grid=True, label='Count difference', ax=axes[0], secondary_y=True)
# df_secs.count().buys.plot(color='green', grid=True, label='Buy Size', style='.', ax=axes[0])
# counts['price'].plot(color='black', grid=True, label='Price', ax=axes[0], secondary_y=True)
plt.show()

takers_who_sold = df[df['type'] == 'sell'].groupby('taker_order_id')
takers_who_bought = df[df['type'] == 'buy'].groupby('taker_order_id')
makers_who_bought = df[df['type'] == 'sell'].groupby('maker_order_id')
makers_who_sold = df[df['type'] == 'buy'].groupby('maker_order_id')

