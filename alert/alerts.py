import datetime as dt
import logging

import pandas as pd

import sql_client as sc
from .messenger import TelegramChat
from .analyzers import PeriodAnalyzer, Results
import util
from .messenger import OTHER_CHAT_IDS


class Alert:
    def __init__(self, name: str, table: str, messenger: TelegramChat, analyzer: PeriodAnalyzer, conn = None, test: bool = False):
        self.name = name
        self.table = table
        if conn is None:
            self.conn = self.create_connection()
        else:
            self.conn = conn

        if test:
            self.messenger = TelegramChat(chat_id=OTHER_CHAT_IDS['Test'])
        else:
            self.messenger = messenger

        self.analyzer = analyzer
        self.threshold = 0

    def create_connection(self):
        host = util.get_host()
        return sc.connection(host=host)

    def analyze(self):
        return None

    def should_alert(self, results) -> str:
        return None

    def send_message(self, msg: str):
        self.messenger.send_message(msg)

    def time_to_run(self) -> bool:
        return True

    def run(self):
        if not self.time_to_run():
            logging.debug(f"{self.name}, didn't run")
            return
        else:
            results = self.analyze()
            msg = self.should_alert(results)
            if msg is not None:
                self.send_message(msg)
            logging.info(f'{self.name}, RAN')


class MinuteAlert(Alert):
    def init_analyzer(self):
        now = dt.datetime.utcnow()
        minute_edge = now.replace(second=0, microsecond=0)
        begin = minute_edge - dt.timedelta(seconds=60)
        qry = f"SELECT * FROM {self.table} WHERE time >= '{begin}' and time < '{minute_edge}' ORDER BY trade_id ASC"
        trades = sc.do_query_with(conn=self.conn, query=qry)
        self.conn.close()
        df = self.create_dataframe(trades)
        self.analyzer.set_trades_df(df)

    def set_threshold(self, threshold):
        self.threshold = threshold

    def create_dataframe(self, trades) -> pd.DataFrame:
        df = pd.DataFrame(trades)
        df.index = pd.DatetimeIndex(df['time'])
        df.drop('time', axis=1, inplace=True)
        df.sort_index(inplace=True)
        df['size'] = df['size'].astype(float)
        return df

    def analyze(self) -> Results:
        self.init_analyzer()
        return self.analyzer.analyze()

    def should_alert(self, results) -> str:
        if results.maker_buys > self.threshold or results.maker_sells > self.threshold:
            logging.info(f'{self.name}: {results.report()}')
            return results.report()
        else:
            return None


class MinuteDiffAlert(MinuteAlert):
    def should_alert(self, results) -> str:
        diff = abs(results.maker_buys - results.maker_sells)
        if diff > self.threshold:
            logging.info(results.report())
            return results.report()
        else:
            return None


class HourAlert(Alert):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_hour_run = -1

    def init_analyzer(self):
        now = dt.datetime.utcnow()
        minute_edge = now.replace(second=0, microsecond=0)
        begin = minute_edge - dt.timedelta(seconds=3600)
        qry = f"SELECT * FROM {self.table} WHERE time >= '{begin}' and time < '{minute_edge}' ORDER BY trade_id ASC"
        trades = sc.do_query_with(conn=self.conn, query=qry)
        self.conn.close()
        df = self.create_dataframe(trades)
        self.analyzer.set_trades_df(df)

    def analyze(self) -> Results:
        self.init_analyzer()
        return self.analyzer.analyze()

    def set_threshold(self, threshold):
        self.threshold = threshold

    def create_dataframe(self, trades) -> pd.DataFrame:
        df = pd.DataFrame(trades)
        df.index = pd.DatetimeIndex(df['time'])
        df.drop('time', axis=1, inplace=True)
        df.sort_index(inplace=True)
        df['size'] = df['size'].astype(float)
        return df

    def time_to_run(self) -> bool:
        now = dt.datetime.utcnow()
        return now.minute == 0 and now.hour != self.last_hour_run

    def should_alert(self, results) -> str:
        diff = abs(results.maker_buys - results.maker_sells)
        if diff > self.threshold:
            logging.info(results.report())
            return results.report()
        else:
            return None

    def run(self):
        if not self.time_to_run():
            logging.debug(f"{self.name}, didn't run")
            return
        else:
            self.last_hour_run = dt.datetime.utcnow().hour
            results = self.analyze()
            msg = self.should_alert(results)
            if msg is not None:
                self.send_message(msg)
            logging.info(f'{self.name}, RAN')

class FourHourAlert(HourAlert):
    def init_analyzer(self):
        now = dt.datetime.utcnow()
        minute_edge = now.replace(second=0, microsecond=0)
        begin = minute_edge - dt.timedelta(seconds=4*3600)
        qry = f"SELECT * FROM {self.table} WHERE time >= '{begin}' and time < '{minute_edge}' ORDER BY trade_id ASC"
        trades = sc.do_query_with(conn=self.conn, query=qry)
        self.conn.close()
        df = self.create_dataframe(trades)
        self.analyzer.set_trades_df(df)

    def time_to_run(self) -> bool:
        now = dt.datetime.utcnow()
        return now.minute == 0 and now.hour != self.last_hour_run and now.hour % 4 == 0