import asyncio
import logging

from copra.websocket import Channel, Client


class HeartBeatClient(Client):
    def on_message(self, message):
        print(message)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler())

    loop = asyncio.get_event_loop()

    ws = HeartBeatClient(loop, [Channel('heartbeat', 'BTC-USD')])

    async def add_a_channel():
        await asyncio.sleep(2)
        ws.subscribe(Channel('heartbeat', 'LTC-USD'))
        loop.create_task(remove_a_channel())

    async def remove_a_channel():
        await asyncio.sleep(2)
        ws.unsubscribe(Channel('heartbeat', 'BTC-USD'))

    loop.create_task(add_a_channel())

    try:
        loop.run_forever()

    except KeyboardInterrupt:
        loop.run_until_complete(ws.close())
        loop.close()
