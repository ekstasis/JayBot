import datetime
import time
import sys

import sql_client as sc
import telegram

FREQUENCY = 60  # seconds
TRADE_AGE = 60  # seconds
ALERT_CHAT_ID = -511211282
ERROR_CHAT_ID = -533383147
WHALE_CHAT_ID = -504882847
TOKEN = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'
TABLE = 'xlm_matches'
WHALE_SIZE_THRESHOLD = 300000
TEST_SIZE_THRESHOLD = 300000


class Checker:
    def __init__(self, display_only):
        self.trade_period = FREQUENCY
        self.table = TABLE
        self.trades = None
        self.alert_bot = telegram.Bot(token=TOKEN)
        self.whale_threshold = WHALE_SIZE_THRESHOLD
        self.test_threshold = TEST_SIZE_THRESHOLD
        self.start_time = datetime.datetime.utcnow()
        self.display_only = display_only

    def get_last_trades(self):
        now = datetime.datetime.utcnow()
        begin = now - datetime.timedelta(seconds=self.trade_period)
        qry = f"select * from {self.table} where time between '{begin}' and '{now}' ORDER BY trade_id ASC"
        self.trades = sc.do_query(qry)

    def analyze_trades(self):
        sells = 0
        buys = 0
        for trade in self.trades:
            size = trade['size']
            trade_type = trade['type']
            if trade_type == 'buy':
                buys += size
            else:
                sells += size

        if sells > self.test_threshold or buys > self.test_threshold:
            start = self.trades[0]['time'].time().strftime("%H:%M:%S")
            end = self.trades[-1]['time'].time().strftime("%H:%M:%S")
            start_price = self.trades[0]['price']
            end_price = self.trades[-1]['price']

            msg = f'Buys: {buys:>7}'
            msg += f'\nSells: {sells:>7}\n'
            msg += f'{start_price} - {end_price}\n'
            msg += f'{start} - {end}\n'
            print()
            print(msg)

            if self.display_only:
                return
            else:
                self.alert_bot.sendMessage(chat_id=ALERT_CHAT_ID, text=msg)

                if sells > self.whale_threshold or buys > self.whale_threshold:
                    pass
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

    checker = Checker(display_only)

    while True:
        checker.get_last_trades()
        checker.analyze_trades()
        checker.check_last_trade_is_not_old()
        checker.heartbeat()

        time.sleep(FREQUENCY)
