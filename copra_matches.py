import asyncio
import time
import sys

from copra.websocket import Channel, Client
import sql_client as sc


def write_record(message, conn, should_test):
    if message['type'] != 'match':
        if message['type'] == 'subscriptions':
            print(message)
        return

    cursor = conn.cursor()

    trade_type = "buy" if message['side'] == "sell" else "sell"
    trade_id = int(message['trade_id'])

    table = 'xlm_matches' if not should_test else 'xlm_matches_test2'
    sql = "INSERT INTO %s" % table
    sql += " SET type = '%s', size = '%s', price = '%s', trade_id = '%s', sequence = '%s', "
    sql += " maker_order_id = '%s', taker_order_id = '%s', "
    sql = sql % (trade_type, message['size'], message['price'], trade_id, message['sequence'], message['maker_order_id'], message['taker_order_id'])
    sql += " time = STR_TO_DATE('%s', '%s');" % (message['time'], "%Y-%m-%dT%H:%i:%s.%fZ")

    rows = cursor.execute(sql)
    if rows != 1:
        raise Exception('Failed to write to database')
    else:
        print(f"Wrote trade {trade_id} to table '{table}'")
        conn.commit()


class WSClient(Client):
    def __init__(self, loop, channels, msg_handler, conn, should_test):
        self.message_handler = msg_handler
        self.conn = conn
        self.test = should_test
        self.prev_trade_id = 0
        super().__init__(loop, channels)

    def set_up(self, msg_handler, conn):
        self.message_handler = msg_handler
        self.conn = conn

    def on_message(self, message):

        if message['type'] == 'last_match':
            self.prev_trade_id = int(message['trade_id'])

        if message['type'] == 'match':
            trade_id = int(message['trade_id'])
            if trade_id - self.prev_trade_id > 1:
                print(f"SKIPPED TRADES:  last: {trade_id} prev: {self.prev_trade_id}")
            self.prev_trade_id = trade_id

        self.message_handler(message, self.conn, self.test)


if __name__ == '__main__':
    args = sys.argv

    test = False
    if len(args) >= 2:
        test = args[1] == 'test'

    if test:
        print("\n*** TESTING ***\n")
    else:
        print("\n*** LIVE LIVE LIVE LIVE ***\n")
        time.sleep(5)

    product_id = "XLM-USD"
    event_loop = asyncio.get_event_loop()
    channel = Channel('matches', product_id)
    db_conn = sc.connection()
    client = WSClient(loop=event_loop, channels=channel, msg_handler=write_record, conn=db_conn, should_test=test)

    try:
        print("Running on XLM_USD ...")
        event_loop.run_forever()
    except KeyboardInterrupt:
        event_loop.run_until_complete(client.close())
        event_loop.close()
    finally:
        db_conn.close()
