import time
import dateutil.parser
import sql_client as db

# import cbpro
from cbpro_fixed_websocket_client import WebsocketClient


def write_record(message):
    conn = db.connection()
    print(message['time'])
    with conn as cursor:
        table = 'matches'
        sql = "INSERT INTO %s" % table
        sql += "(`date`, `time`, `microseconds`, `maker_side`, `size`, `price`, `trade_id`, `sequence`, "
        sql += "`maker_order_id`, `taker_order_id`, `product_id`)"
        sql += "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        # message['sequence'] = 1
        timestamp = dateutil.parser.parse(message['time'])
        date = timestamp.date()
        hour_min = timestamp.time()
        microseconds = timestamp.microsecond

        data = (date, hour_min, microseconds, message['side'], message['size'], message['price'], message['trade_id'],
                message['sequence'], message['maker_order_id'], message['taker_order_id'], message['product_id'])
        rows = cursor.execute(sql, data)
        if rows != 1:
            raise Exception('Failed to write to database')


class WSC(WebsocketClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # self.time = dt.datetime.utcnow()
        self.sell_volume = 0
        self.buy_volume = 0
        self.test = False
        self.minute = 4

    def on_message(self, msg):
        if not msg['type'] == 'match':
            return
        write_record(message=msg)
        return
        # check for new candle
        # now = dt.datetime.utcnow()
        # count = now.minute % 5
        # new_candle = count < self.minute
        # self.minute = count
        #
        # if new_candle:
        #     self.sell_volume = 0
        #     self.buy_volume = 0
        #
        # side = msg['side']
        # size = float(msg['size'])
        # # price = float(msg['price'])
        # volume = size
        #
        # if side == 'sell':
        #     self.buy_volume += volume
        # if side == 'buy':
        #     self.sell_volume += volume
        # print(now.hour, ':', now.minute, self.buy_volume, '\t\t', self.sell_volume,
        # '\t\t', self.buy_volume - self.sell_volume)


if __name__ == '__main__':
    chans = ['matches']
    prods = ['XLM-USD']
    wsc = WSC(channels=chans, products=prods)
    wsc.start()
    try:
        while True:
            time.sleep(3600)
    finally:
        wsc.close()

