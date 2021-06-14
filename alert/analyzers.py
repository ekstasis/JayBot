import datetime as dt
from dateutil import tz


class Results:
    def __init__(self):
        self.start_time: dt.datetime = None
        self.end_time: dt.datetime = None
        self.open_price = 0.0
        self.close_price = 0.0
        self.maker_buys = 0.0
        self.maker_sells = 0.0

    def report(self):
        utz_tz = tz.gettz('UTC')
        eastern_tz = tz.gettz('America/New_York')
        start_time = self.start_time.replace(tzinfo=utz_tz)
        local_time = start_time.astimezone(tz=eastern_tz)
        local_time_str = local_time.strftime('%m/%d %H:%M')
        utc_time_str = start_time.strftime('%m/%d %H:%M')

        lines = self.report_helper()
        lines = lines + [f'{local_time_str}',
                         f'{utc_time_str} (UTC)']

        return '\n'.join(lines)

    def report_helper(self):
        """ Puts together the volume info of the message, dealing with formatting for large vs small numbers
        """
        maker_buys_larger = self.maker_buys > self.maker_sells
        diff_str = "extra buys" if maker_buys_larger else "extra sells"
        raw_diff = self.maker_buys - self.maker_sells if maker_buys_larger else self.maker_sells - self.maker_buys

        diff = raw_diff if raw_diff < 1_000 else raw_diff / 1_000
        buys = self.maker_buys if self.maker_buys < 1_000 else self.maker_buys / 1_000
        sells = self.maker_sells if self.maker_sells < 1_000 else self.maker_sells / 1_000
        diff = int(round(diff))
        buys = int(round(buys))
        sells = int(round(sells))

        lines = [ f'{diff:,}K {diff_str}' ] if raw_diff > 1_000 else [f'{diff:,} {diff_str}']
        lines.append(f'{self.open_price:.6} -> {self.close_price:.6}')
        buys_line = f'Buys: {buys:,}'
        sells_line = f'Sells: {sells:,}'
        if self.maker_buys > 1_000:
            buys_line += 'K'
        if self.maker_sells > 1_000:
            sells_line += 'K'
        lines += [buys_line, sells_line]
        return lines


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


