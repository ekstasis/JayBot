import datetime as dt
import pandas as pd

import sql_client as db


class Results:
    def __init__(self):
        self.start_time: dt.datetime = None
        self.end_time: dt.datetime = None
        self.open= 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
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
        lines = [
            f'{start_time} -> {end_time}',
            f'Coins bought: {self.total_buys}\nCoins sold: {self.total_sells}',
            f'Largest trades:\n{self.largest_buy_trade} (buy)\n{self.largest_sell_trade} (sell)',
            f'Largest orders:\n{self.largest_buy_order} (buy)\n{self.largest_sell_order} (sell)',
            f'O: {self.open}\nH: {self.high}\nL: {self.low}\nC: {self.close}']
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
        self.get_ohlc()
        self.get_time_range()
        self.calculate_totals()
        self.calculate_largest_trades()
        self.calc_largest_orders()

    def get_ohlc(self):
        df = self.trades_df
        self.results.open = df.iloc[0].price
        self.results.high = df.price.max()
        self.results.low = df.price.min()
        self.results.close = df.iloc[-1].price

    def get_time_range(self):
        self.results.start_time = self.trades_df.index[0]
        self.results.end_time = self.trades_df.index[-1]

    def create_dataframe(self):
        self.trades_df = pd.DataFrame(self.raw_trades)
        self.trades_df.index = pd.DatetimeIndex(self.trades_df['time'])
        self.trades_df.drop('time', axis=1, inplace=True)
        self.trades_df.sort_index(inplace=True)

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