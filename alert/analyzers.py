import datetime as dt
from dateutil import tz

import pandas as pd


class Results:
    def __init__(self):
        self.start_time: dt.datetime = None
        self.end_time: dt.datetime = None
        self.open_price = 0.0
        self.close_price = 0.0
        self.maker_buys = 0
        self.maker_sells = 0

    def report(self):
        utz_tz = tz.gettz('UTC')
        eastern_tz = tz.gettz('America/New_York')
        start_time = self.start_time.replace(tzinfo=utz_tz)
        local_time = start_time.astimezone(tz=eastern_tz)
        local_time_str = local_time.strftime('%m/%d %H:%M')
        utc_time_str = start_time.strftime('%m/%d %H:%M')

        maker_buys_larger = self.maker_buys > self.maker_sells
        diff_str = "extra buys" if maker_buys_larger else "extra sells"
        diff = self.maker_buys - self.maker_sells if maker_buys_larger else self.maker_sells - self.maker_buys
        diff_k = int(round(diff / 1000))
        buys_k = int(round(self.maker_buys / 1000))
        sells_k = int(round(self.maker_sells / 1000))

        lines = [
            f'{diff_k:,}K {diff_str}',
            f'{self.open_price:.6} -> {self.close_price:.6}',
            f'Buys: {buys_k:,}K',
            f'Sells: {sells_k:,}K',
            f'{local_time_str}',
            f'{utc_time_str} (UTC)'
        ]
        return '\n'.join(lines)


class PeriodAnalyzer:
    """ Calculate various statistics for a given period of trades
    """
    def __init__(self):
        self.df = None

    def set_trades_df(self, df):
        self.df = df

    def analyze(self) -> Results:
        df = self.df
        if df is None:
            raise Exception('Analyzer has no data.')

        results = Results()
        results.maker_buys = df[df.type == 'sell']['size'].sum()
        results.maker_sells = df[df.type == 'buy']['size'].sum()
        results.open_price = df.iloc[0]['price']
        results.close_price = df.iloc[-1]['price']
        results.start_time = self.df.index[0]
        results.end_time = self.df.index[-1]
        return results


