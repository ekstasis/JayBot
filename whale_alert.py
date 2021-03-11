import time

from Junk.cbpro_fixed_websocket_client import WebsocketClient


def msg_hander(message):
    if not message['type'] == 'match':
        return

    size = int(message['size'])
    trade_type = 'buy' if message['side'] == 'sell' else 'sell'
    print(trade_type, ":", size)

    if size > 1000:
        command = 'say "'
        command += '[[volm 0.3]]'
        command += 'Someone bought ' if trade_type is 'buy' else 'Someone sold '
        command += str(size) + '"'


class WSC(WebsocketClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.msg_handler = None

    def on_message(self, msg):
        self.msg_handler(message=msg)


if __name__ == '__main__':
    chans = ['matches']
    prods = ['XLM-USD']
    wsc = WSC(channels=chans, products=prods)
    wsc.msg_handler = msg_hander
    wsc.start()
    try:
        time.sleep(3600)
    finally:
        wsc.close()

