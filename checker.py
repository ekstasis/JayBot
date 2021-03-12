import datetime
import time

import sql_client as sc

FREQUENCY = 30  # seconds
TRADE_AGE = 5  # seconds

if __name__ == '__main__':

    while True:

        table_name = "xlm_matches"
        qry = "select * from %s ORDER BY trade_id DESC LIMIT 1" % table_name
        trade = sc.do_query(qry)[0]
        trade_time = trade['time']
        trade_id = trade['trade_id']

        now = datetime.datetime.utcnow()
        timedelta = now - trade_time

        if timedelta.seconds > TRADE_AGE:
            print(f"LAST TRADE IS {timedelta.seconds} SECONDS OLD!!!!  LAST TRADE:  ", trade_time)
            print(f"Table: {table_name}")
        else:
            print(trade_id)

        time.sleep(FREQUENCY)
