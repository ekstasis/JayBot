import time
import datetime as dt
import pprint as pp

from pymongo import MongoClient
import cbpro


# def __init__(self, url="wss://ws-feed.pro.coinbase.com", products=None, message_type="subscribe", mongo_collection=None,
#              should_print=True, auth=False, api_key="", api_secret="", api_passphrase="", channels=None):
# https://docs.pro.coinbase.com/#websocket-feed

class WSC(cbpro.WebsocketClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # self.time = dt.datetime.utcnow()
        self.sell_volume = 0
        self.buy_volume = 0

        self.minute = 4

    def on_message(self, msg):
        if not msg['type'] == 'match':
            return

        # check for new candle
        now = dt.datetime.utcnow()
        count = now.minute % 5
        new_candle = count < self.minute
        self.minute = count

        if new_candle:
            self.sell_volume = 0
            self.buy_volume = 0

        side = msg['side']
        size = float(msg['size'])
        # price = float(msg['price'])
        volume = size

        if side == 'sell':
            self.buy_volume += volume
        if side == 'buy':
            self.sell_volume += volume

        # print("{0:.2f}".format(self.buy_volume), '\t\t', "{0:.2f}".format(self.sell_volume), '\t\t', self.buy_volume - self.sell_volume)
        print(now.hour, ':', now.minute, self.buy_volume, '\t\t', self.sell_volume, '\t\t', self.buy_volume - self.sell_volume)
        # print("{0:.2f}".format(size))


def on_close(self):
    print('All Done.  Received %d messages.' % self.msg_count)


chans = ['matches']
prods = ['XLM-USD']
wsc = WSC(channels=chans, products=prods)
# wsc= cbpro.WebsocketClient(channels=chans, products=prods)
wsc.start()
try:
    time.sleep(500)
finally:
    wsc.close()

""" MONGO """


def mongo():
    mongo_client = MongoClient('mongodb://localhost:27017/')

    # specify the database and collection
    db = mongo_client.cryptocurrency_database
    BTC_collection = db.BTC_collection

    # instantiate a WebsocketClient instance with a Mongo collection as a parameter
    mongo_wsc = cbpro.WebsocketClient(url="wss://ws-feed.pro.coinbase.com", products="BTC-USD",
                                      mongo_collection=BTC_collection, should_print=False)
    mongo_wsc.start()

# pub_client = cbpro.PublicClient()
# history = pub_client.get_product_historic_rates()
