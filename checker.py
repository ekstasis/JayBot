import datetime
import time

import sql_client as sc
import telegram

FREQUENCY = 60  # seconds
TRADE_AGE = 20  # seconds
ALERT_CHAT_ID = -511211282
ERROR_CHAT_ID = -533383147
WHALE_CHAT_ID = -504882847
TOKEN = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'
TABLE = 'xlm_matches'
TRADE_SIZE_THRESHOLD = 300000


class Checker:
    def __init__(self):
        self.trade_period = FREQUENCY
        self.table = TABLE
        self.trades = None
        self.alert_bot = telegram.Bot(token=TOKEN)
        self.trade_size_threshold = TRADE_SIZE_THRESHOLD

    def get_last_trades(self):
        now = datetime.datetime.utcnow()
        begin = now - datetime.timedelta(seconds=self.trade_period)
        # print(f'Trades from {begin} to {now} ({self.trade_period} seconds)')
        qry = f"select * from {self.table} where time between '{begin}' and '{now}' ORDER BY trade_id DESC"
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

        if sells > self.trade_size_threshold or buys > self.trade_size_threshold:
            now = datetime.datetime.utcnow()
            time_str = now.time().strftime(format='%H:%M:%S')
            msg = f'WHALE ALERT:\n Buys: {buys:>7}\nSells: {sells:>7}\n'
            msg += f'({self.trade_period} second period starting at {time_str} UTC)'
            self.alert_bot.sendMessage(chat_id=ALERT_CHAT_ID, text=msg)
            self.alert_bot.sendMessage(chat_id=WHALE_CHAT_ID, text=msg)


            print(msg)

    def check_last_trade_is_not_old(self):
        try:
            trade = self.trades[0]
        except IndexError:
            qry = f'SELECT * from {self.table} ORDER BY trade_id DESC LIMIT 1'
            trade = sc.do_query(qry)[0]

        trade_time = trade['time']
        trade_id = trade['trade_id']
        now = datetime.datetime.utcnow()
        timedelta = (now - trade_time).seconds

        if timedelta > TRADE_AGE:
            msg = f'The last recorded trade ({trade_id}) is {timedelta} seconds old!!'
            self.alert_bot.sendMessage(chat_id=ERROR_CHAT_ID, text=msg)
            print(f'\n********************\n{msg}\n********************')

    def heartbeat(self):
        now = datetime.datetime.utcnow()
        msg = f'\n{now.hour}:00' if now.minute == 0 else '.'
        print(msg, end='', flush=True)


if __name__ == '__main__':
    print(f'Checking every {FREQUENCY} seconds.  Trade age limit: {TRADE_AGE}.  Table: {TABLE}')

    checker = Checker()

    while True:
        checker.get_last_trades()
        checker.analyze_trades()
        checker.check_last_trade_is_not_old()
        checker.heartbeat()

        time.sleep(FREQUENCY)
