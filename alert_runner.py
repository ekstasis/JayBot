import sys
import time
import logging
import datetime as dt

import alert.alerts as alerts
from alert.analyzers import PeriodAnalyzer
import util
import alert.messenger as chat


def init_logging(level):
    log_format = '%(levelname)s %(asctime)s %(module)s: %(message)s'
    log_time_format = '%H:%M:%S'
    logging.basicConfig(level=level, format=log_format, datefmt=log_time_format)


def heartbeat():
    now = dt.datetime.utcnow()
    msg = f'\n{now.hour}:00' if now.minute == 0 else '.'
    print(msg, end='', flush=True)


if __name__ == '__main__':
    args = sys.argv
    debug = False
    if len(args) >= 2:
        debug = args[1] == 'debug'
    if debug:
        init_logging(logging.DEBUG)
    else:
        init_logging(logging.INFO)
    logging.info('Starting Alerts ...')

    min_vol_chat = chat.TelegramChat.create_bot('1_min_vol')
    min_diff_chat = chat.TelegramChat.create_bot('1_min_diff')
    hour_diff_chat = chat.TelegramChat.create_bot('1_hr_diff')

    test_chat = chat.TelegramChat.create_bot('Test')
    error_chat = chat.TelegramChat.create_bot('JayBotErrors')

    analyzer = PeriodAnalyzer()
    table = 'matches_xlm'

    diff_1m = alerts.MinuteDiffAlert(name='1m diff', table=table, messenger=min_diff_chat, analyzer=analyzer)
    diff_1m.set_threshold(300000)

    vol_1m = alerts.MinuteAlert(name='1m_vol', table=table, messenger=min_vol_chat, analyzer=analyzer)
    vol_1m.set_threshold(600000)

    diff_1h = alerts.HourAlert(name='1h_diff', table=table, messenger=hour_diff_chat, analyzer=analyzer)
    diff_1h.set_threshold(0)

    diff_4h = alerts.FourHourAlert(name='4h_diff', table=table, messenger=hour_diff_chat, analyzer=analyzer)

    alerts = [vol_1m, diff_1m, diff_1h, diff_4h]

    while True:
        seconds_into_minute = time.localtime().tm_sec
        time.sleep(60 - seconds_into_minute)
        logging.debug('== START ==')
        if logging.getLogger().level == logging.INFO:
            heartbeat()
        try:
            for alert in alerts:
                alert.run()
            logging.debug('== END ==')
        except Exception as err:
            logging.error(msg=err, exc_info=True)
            host = util.get_host()
            text = f'alert_runner error on {host}: {err}'
            error_chat.send_message(msg=f'Error on {host}: {err}')
