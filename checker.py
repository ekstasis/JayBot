import datetime
import time
import sys
import socket
import traceback

import telegram
# import pdb_attach
# pdb_attach.listen(50000)  # Listen on port 50000.

import sql_client as sc
import volume_analyzer as va


FREQUENCY = 60  # seconds
TRADE_AGE = 60  # seconds
ALERT_CHAT_ID = -511211282
ERROR_CHAT_ID = -533383147
WHALE_CHAT_ID = -504882847
TOKEN = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'
TABLE = 'matches_xlm'
WHALE_SIZE_THRESHOLD = 600000
# TEST_SIZE_THRESHOLD = 6


class Checker:
    def __init__(self, should_display_only, analyzer, alert_bot, conn):
        self.trade_period = FREQUENCY
        self.table = TABLE
        self.trades = None
        self.alert_bot = alert_bot
        self.whale_threshold = WHALE_SIZE_THRESHOLD
        # self.test_threshold = TEST_SIZE_THRESHOLD
        self.start_time = datetime.datetime.utcnow()
        self.display_only = should_display_only
        self.analyzer: va.PeriodAnalyzer = analyzer
        self.conn = conn

    def get_last_trades(self):
        now = datetime.datetime.utcnow()
        minute_edge = now.replace(second=0, microsecond=0)

        begin = minute_edge - datetime.timedelta(seconds=self.trade_period)
        qry = f"SELECT * FROM {self.table} WHERE time >= '{begin}' and time < '{minute_edge}' ORDER BY trade_id ASC"
        self.trades = sc.do_query_with(conn=self.conn, query=qry)
        self.conn.close()

    def analyze_trades(self):
        self.analyzer.set_trades(self.trades)
        self.analyzer.analyze()
        results = self.analyzer.results

        if results.over_threshold(WHALE_SIZE_THRESHOLD):
            msg = results.report()

            print(f'\n{msg}')

            if not self.display_only:
                self.alert_bot.sendMessage(chat_id=WHALE_CHAT_ID, text=msg)

    def check_last_trade_is_not_old(self):
        try:
            trade = self.trades[-1]
        except IndexError:
            qry = f'SELECT * from {self.table} ORDER BY trade_id DESC LIMIT 1'
            trade = sc.do_query_with(conn=self.conn, query=qry)[0]

        trade_time = trade['time']
        trade_id = trade['trade_id']
        now = datetime.datetime.utcnow()
        timedelta = (now - trade_time).seconds

        if timedelta > TRADE_AGE:
            msg = f'The last recorded trade ({trade_id}) is {timedelta} seconds old!!'
            print(f'\n********************\n{msg}\n********************')

            if display_only:
                return
            else:
                self.alert_bot.sendMessage(chat_id=ERROR_CHAT_ID, text=msg)

    @staticmethod
    def heartbeat():
        now = datetime.datetime.utcnow()
        msg = f'\n{now.hour}:00' if now.minute == 0 else '.'
        print(msg, end='', flush=True)


if __name__ == '__main__':
    args = sys.argv
    display_only = False
    if len(args) >= 2:
        display_only = args[1] == 'display_only'

    if display_only:
        print("\n*** Display Only ***\n")
    else:
        print("\n*** SENDING TO TELEGRAM ***\n")

    print(f'Checking every {FREQUENCY} seconds.  Trade age limit: {TRADE_AGE}.  Table: {TABLE}')
    print(f'Whale threshold:  {WHALE_SIZE_THRESHOLD}')

    vol_analyzer = va.PeriodAnalyzer()
    telegram_bot = telegram.Bot(token=TOKEN)
    host = socket.gethostname()
    if host == 'JB-MBP-15':
        host = 'debian_from_mac'

    db_conn = sc.connection(database=host)

    checker = Checker(display_only, analyzer=vol_analyzer, alert_bot=telegram_bot, conn=db_conn)

    while True:
        seconds_into_minute = time.localtime().tm_sec
        time.sleep(60-seconds_into_minute)

        try:
            checker.get_last_trades()
            checker.analyze_trades()
            checker.check_last_trade_is_not_old()
            checker.heartbeat()

        except KeyError:
            telegram_bot.sendMessage(chat_id=ERROR_CHAT_ID, text=f'Error on {host}')
            traceback.print_exc()
            if checker.analyzer.raw_trades is not None:
                print(checker.analyzer.raw_trades)
            if checker.analyzer.trades_df is not None:
                print(checker.analyzer.trades_df.head())

        except Exception:
            telegram_bot.sendMessage(chat_id=ERROR_CHAT_ID, text=f'Error on {host}')
            traceback.print_exc()
