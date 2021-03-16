import datetime as dt
import pandas as pd


class Results:
    def __init__(self):
        self.product = 'XLM-USD'
        self.start_time: dt.datetime = None
        self.end_time: dt.datetime = None
        self.largest_order_price = 0.0
        self.total_buys = 0
        self.total_sells = 0
        self.largest_buy_trade = 0
        self.largest_sell_trade = 0
        self.largest_buy_order = 0
        self.largest_sell_order = 0

    def over_threshold(self, threshold):
        return self.total_buys > threshold or self.total_sells > threshold

    def report(self):
        start_time = self.start_time.time().strftime('%H:%M:%S')
        end_time = self.end_time.time().strftime('%H:%M:%S')

        total_buys = f'{int(self.total_buys / 1000):,}K'
        total_sells = f'{int(self.total_sells / 1000):,}K'
        largest_buy_trade = f'{int(self.largest_buy_trade / 1000):,}K'
        largest_sell_trade = f'{int(self.largest_sell_trade / 1000):,}K'
        largest_buy_order = f'{int(self.largest_buy_order / 1000):,}K'
        largest_sell_order = f'{int(self.largest_sell_order / 1000):,}K'

        lines = [
            f'{start_time} -> {end_time}',
            f'Coins bought: {total_buys}\nCoins sold: {total_sells}',
            f'Largest trades:\n{largest_buy_trade} (buy)\n{largest_sell_trade} (sell)',
            f'Largest orders:\n{largest_buy_order} (buy)\n{largest_sell_order} (sell)',
            f'Largest order price: ${self.largest_order_price:,.6}'
        ]
        return '\n'.join(lines)

    def sql_query(self):
        return f"SELECT * FROM xlm_matches WHERE time BETWEEN '{self.start_time}' AND '{self.end_time}'"


class PeriodAnalyzer:
    """ Calculate various statistics for a given period of trades """

    def __init__(self):
        self.raw_trades = None
        self.trades_df = None
        self.results = Results()

    def set_trades(self, raw_trades):
        self.raw_trades = raw_trades
        self.create_dataframe()

    def analyze(self):
        self.create_dataframe()
        # self.get_price()
        self.get_time_range()
        self.calculate_totals()
        self.calculate_largest_trades()
        self.calc_largest_orders()

    def get_time_range(self):
        self.results.start_time = self.trades_df.index[0]
        self.results.end_time = self.trades_df.index[-1]

    def create_dataframe(self):
        self.trades_df = pd.DataFrame(self.raw_trades)
        self.trades_df.index = pd.DatetimeIndex(self.trades_df['time'])
        self.trades_df.drop('time', axis=1, inplace=True)
        self.trades_df.sort_index(inplace=True)
        self.trades_df['size'] = self.trades_df['size'].astype(float)

    def calculate_totals(self):
        df = self.trades_df
        self.results.total_buys = df[df.type == 'buy']['size'].sum()
        self.results.total_sells = df[df.type == 'sell']['size'].sum()

    def calculate_largest_trades(self):
        df = self.trades_df
        buys = df[df.type == 'buy']
        sells = df[df.type == 'sell']
        self.results.largest_buy_trade = buys['size'].max()
        self.results.largest_sell_trade = sells['size'].max()

    def calc_largest_orders(self):
        df = self.trades_df
        sell_takers = df[df['type'] == 'sell'].groupby('taker_order_id')
        buy_takers = df[df['type'] == 'buy'].groupby('taker_order_id')
        self.results.largest_sell_order = sell_takers.sum()['size'].max()
        self.results.largest_buy_order = buy_takers.sum()['size'].max()

        largest_order_id = df.groupby('taker_order_id').sum()['size'].idxmax()
        self.results.largest_order_price = df[df.taker_order_id == largest_order_id].iloc[0]['price']

# end = dt.datetime.utcnow()
# timedelta = dt.timedelta(minutes=1)
# start = end - timedelta
#
# qry = "SELECT * from xlm_matches "
# qry += f"WHERE time BETWEEN '{start}' AND '{end}' "
# qry += "ORDER BY trade_id asc"
#
# trades = db.do_query(qry)
#
# analyzer = PeriodAnalyzer(raw_trades=trades)
# analyzer.analyze()
# results = analyzer.results
# df = analyzer.trades_df
# pd.options.display.max_columns = 20
# pd.options.display.width = 180
