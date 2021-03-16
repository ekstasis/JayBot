import asyncio
import time
import sys

from copra.websocket import Channel, Client
import sql_client as sc
from products import products


def write_record(message, conn, should_test):
    if message['type'] != 'match':
        if message['type'] == 'subscriptions':
            print(message)
            if not should_test:
                time.sleep(5)
        return


    trade_type = "buy" if message['side'] == "sell" else "sell"
    trade_id = int(message['trade_id'])
    product_id = message['product_id']

    if should_test:
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
        print(f"Wrote trade {trade_id} to table '{table}' ({product_id})")
        conn.commit()
        # written_size = sc.do_query_with(conn, f"SELECT size FROM {table} WHERE trade_id = '{trade_id}'")[0]['size']
        # msg_size = float(message['size'])
        # if written_size == msg_size:
        #     print("good")
        # else:
        #     print("BABABADBABDKJSLDFLKSJDFLKSJDFLKSJDFLKSJDFLKSJDFLKSDJF")


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

    test = False
    if len(args) >= 2:
        test = args[1] == 'test'

    if test:
        print("\n*** TESTING ***\n")
    else:
        print("\n*** LIVE LIVE LIVE LIVE ***\n")

    event_loop = asyncio.get_event_loop()
    channel_list = Channel('matches', list(products.keys()))
    db_conn = sc.connection()
    client = WSClient(loop=event_loop, channels=channel_list, msg_handler=write_record, conn=db_conn, should_test=test)

    try:
        event_loop.run_forever()
    except KeyboardInterrupt:
        event_loop.run_until_complete(client.close())
        event_loop.close()
    finally:
        db_conn.close()
