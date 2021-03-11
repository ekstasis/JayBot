import time

from cbpro_fixed_websocket_client import WebsocketClient


def msg_hander(message):
    print(message)


class WSC(WebsocketClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.msg_handler = None

    def on_message(self, msg):
        self.msg_handler(message=msg)


if __name__ == '__main__':
    chans = ['heartbeat']
    prods = ['XLM-USD']
    wsc = WSC(channels=chans, products=prods)
    wsc.msg_handler = msg_hander
    wsc.start()
    try:
        time.sleep(3600)
    finally:
        wsc.close()

