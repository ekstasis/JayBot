import asyncio
import time
import sys

from copra.websocket import Channel, Client
import sql_client as sc
from products import products


def write_record(message, conn, should_test):
    """Takes a "match" message from Coinbase Pro websocket and writes it to MYSQL database
    """
    if message['type'] != 'match':
        if message['type'] == 'subscriptions':
            print(message)
            if not should_test:
                time.sleep(3)  # gives user a chance to quit if forgot to start in Test mode
        return

    trade_type = "buy" if message['side'] == "sell" else "sell"  # record TAKER side of trade
    trade_id = int(message['trade_id'])
    product_id = message['product_id']

    if should_test:  # separate mysql table for testing
        table = 'matches_test'
    else:
        table = products[product_id]

    sql = "INSERT INTO %s" % table
    sql += " SET type = '%s', size = '%s', price = '%s', trade_id = '%s', sequence = '%s', "
    sql += " maker_order_id = '%s', taker_order_id = '%s', product_id = '%s', "
    sql = sql % (trade_type, message['size'], message['price'], trade_id, message['sequence'],
                 message['maker_order_id'], message['taker_order_id'], message['product_id'])
    sql += " time = STR_TO_DATE('%s', '%s');" % (message['time'], "%Y-%m-%dT%H:%i:%s.%fZ")

    rows = conn.cursor().execute(sql)
    if rows != 1:
        raise Exception('Failed to write to database')
    else:
        print(f"Wrote trade {trade_id} to table '{table}' ({product_id})")  # log to stdout
        conn.commit()


class WSClient(Client):
    def __init__(self, loop, channels, msg_handler, conn, should_test):
        self.message_handler = msg_handler
        self.conn = conn
        self.test = should_test
        super().__init__(loop, channels)

    def set_up(self, msg_handler, conn):
        self.message_handler = msg_handler
        self.conn = conn

    def on_message(self, message):
        self.message_handler(message, self.conn, self.test)


if __name__ == '__main__':
    args = sys.argv

    test = len(args) >= 2 and args[1] == 'test'

    if test:
        print("\n*** TESTING ***\n")
    else:
        print("\n*** LIVE LIVE LIVE LIVE ***\n")

    product_list = ['XLM-USD', 'ETH-USD']
    channel_list = Channel('matches', product_list)

    db_conn = sc.connection()
    host = sc.get_connect_info(db_conn)
    print(f'Host: {host["host"]}\nDB: {host["db"]}\n')

    event_loop = asyncio.get_event_loop()

    client = WSClient(loop=event_loop, channels=channel_list, msg_handler=write_record, conn=db_conn, should_test=test)

    try:
        event_loop.run_forever()
    except KeyboardInterrupt:
        event_loop.run_until_complete(client.close())
        event_loop.close()
    finally:
        db_conn.close()
