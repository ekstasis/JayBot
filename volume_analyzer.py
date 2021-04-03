import datetime as dt
from dateutil import tz

import pandas as pd


class Results:
    def __init__(self):
        self.product = 'XLM-USD'
        self.start_time: dt.datetime = None
        self.end_time: dt.datetime = None
        self.open_price = 0.0
        self.close_price = 0.0
        self.maker_buys = 0
        self.maker_sells = 0

    def over_threshold(self, threshold):
        return self.maker_buys > threshold or self.maker_sells > threshold

    def report(self):
        utz_tz = tz.gettz('UTC')
        eastern_tz = tz.gettz('America/New_York')
        start_time = self.start_time.replace(tzinfo=utz_tz)
        local_time = start_time.astimezone(tz=eastern_tz)
        local_time_str = local_time.strftime('%m/%d %H:%M')

        maker_buys_larger = self.maker_buys > self.maker_sells
        diff_str = "Maker buys - sells" if maker_buys_larger else "Maker sells - buys"
        diff = self.maker_buys - self.maker_sells if maker_buys_larger else self.maker_sells - self.maker_buys
        diff_k = int(round(diff / 1000))

        lines = [
            f'{local_time_str}',
            f'{diff_str}: {diff_k}K',
            f'{self.open_price:.6} -> {self.close_price:.6}',
        ]
        return '\n'.join(lines)


class PeriodAnalyzer:
    """ Calculate various statistics for a given period of trades
    """
    def __init__(self):
        self.raw_trades = None
        self.trades_df = None
        self.results = Results()

    def set_trades(self, raw_trades):
        self.raw_trades = raw_trades
        self.create_dataframe()

    def analyze(self):
        self.create_dataframe()
        self.calculate_totals()
        self.get_price_range()
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
        self.results.maker_buys = df[df.type == 'sell']['size'].sum()
        self.results.maker_sells = df[df.type == 'buy']['size'].sum()

    def get_price_range(self):
        df = self.trades_df
        self.results.open_price = df.iloc[0]['price']
        self.results.close_price = df.iloc[-1]['price']
