import asyncio
import dateutil.parser

from copra.websocket import Channel, Client
import sql_client as sc


def write_record(message, conn):
    if message['type'] != 'match':
        return
    cursor = conn.cursor()
    table = 'matches'
    sql = "INSERT INTO %s" % table
    sql += "(`date`, `time`, `microseconds`, `maker_side`, `size`, `price`, `trade_id`, `sequence`, "
    sql += "`maker_order_id`, `taker_order_id`, `product_id`)"
    sql += "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    timestamp = dateutil.parser.parse(message['time'])
    date = timestamp.date()
    hour_min = timestamp.time()
    microseconds = timestamp.microsecond
    trade_type = "buy" if message['side'] == "sell" else "sell"
    print(f'{hour_min}...', end='')

    data = (date, hour_min, microseconds, message['side'], message['size'], message['price'], message['trade_id'],
            message['sequence'], message['maker_order_id'], message['taker_order_id'], message['product_id'])
    rows = cursor.execute(sql, data)
    if rows != 1:
        raise Exception('Failed to write to database')
    else:
        print('!  ')
        conn.commit()

    # print(f'{date} {hour_min}:  maker side: {trade_type} size: {message["size"]}, price: {message["price"]}')


class WSClient(Client):
    def __init__(self, loop, channels, msg_handler, conn):
        self.message_handler = msg_handler
        self.conn = conn
        super().__init__(loop, channels)

    def set_up(self, msg_handler, conn):
        self.message_handler = msg_handler
        self.conn = conn

    def on_message(self, message):
        self.message_handler(message, self.conn)


if __name__ == '__main__':
    product_id = "XLM-USD"
    event_loop = asyncio.get_event_loop()
    channel = Channel('matches', product_id)
    db_conn = sc.connection()
    client = WSClient(loop=event_loop, channels=channel, msg_handler=write_record, conn=db_conn)

    try:
        print("Running on XLM_USD ...")
        event_loop.run_forever()
    except KeyboardInterrupt:
        event_loop.run_until_complete(client.close())
        event_loop.close()
    finally:
        db_conn.close()
