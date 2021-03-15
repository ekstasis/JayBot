import datetime
import time
import sys

import telegram

import sql_client as sc
import volume_analyzer as va


FREQUENCY = 60  # seconds
TRADE_AGE = 60  # seconds
ALERT_CHAT_ID = -511211282
ERROR_CHAT_ID = -533383147
WHALE_CHAT_ID = -504882847
TOKEN = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'
TABLE = 'matches_xlm'
WHALE_SIZE_THRESHOLD = 300000
TEST_SIZE_THRESHOLD =  300000


class Checker:
    def __init__(self, display_only, analyzer):
        self.trade_period = FREQUENCY
        self.table = TABLE
        self.trades = None
        self.alert_bot = telegram.Bot(token=TOKEN)
        self.whale_threshold = WHALE_SIZE_THRESHOLD
        self.test_threshold = TEST_SIZE_THRESHOLD
        self.start_time = datetime.datetime.utcnow()
        self.display_only = display_only
        self.analyzer: va.PeriodAnalyzer= analyzer

    def get_last_trades(self):
        now = datetime.datetime.utcnow()
        minute_edge = now.replace(second=0, microsecond=0)

        begin = minute_edge - datetime.timedelta(seconds=self.trade_period)
        qry = f"select * from {self.table} where time between '{begin}' and '{minute_edge}' ORDER BY trade_id ASC"
        self.trades = sc.do_query(qry)

    def analyze_trades(self):
        self.analyzer.set_trades(self.trades)
        self.analyzer.analyze()
        results = self.analyzer.results

        if results.over_threshold(TEST_SIZE_THRESHOLD):
            msg = results.report()
            print(f'\n{msg}')

            if not self.display_only:
                self.alert_bot.sendMessage(chat_id=ALERT_CHAT_ID, text=msg)

                if results.over_threshold(WHALE_SIZE_THRESHOLD):
                    self.alert_bot.sendMessage(chat_id=WHALE_CHAT_ID, text=msg)

    def check_last_trade_is_not_old(self):
        try:
            trade = self.trades[-1]
        except IndexError:
            qry = f'SELECT * from {self.table} ORDER BY trade_id DESC LIMIT 1'
            trade = sc.do_query(qry)[0]

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

    def heartbeat(self):
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

    analyzer = va.PeriodAnalyzer()
    checker = Checker(display_only, analyzer=analyzer)

    while True:
        seconds_into_minute = time.localtime().tm_sec
        time.sleep(60-seconds_into_minute)
        checker.get_last_trades()
        checker.analyze_trades()
        checker.check_last_trade_is_not_old()
        checker.heartbeat()
