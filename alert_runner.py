import sys
import time
import logging
import datetime as dt

import alert.alerts as alerts
from alert.analyzers import PeriodAnalyzer
import util
from alert.messenger import TelegramChat, XLM_CHAT_IDS, OTHER_CHAT_IDS, ETH_CHAT_IDS


def init_logging(level):
    log_format = '%(levelname)s %(asctime)s %(module)s: %(message)s'
    log_time_format = '%H:%M:%S'
    logging.basicConfig(level=level, format=log_format, datefmt=log_time_format)


def heartbeat():
    """ This prints '.' every minute to indicate the alerts are running
    """
    now = dt.datetime.utcnow()
    msg = f'\n{now.hour}:00' if now.minute == 0 else '.'
    print(msg, end='', flush=True)


if __name__ == '__main__':
    args = sys.argv
    debug = False
    if len(args) >= 2:
        debug = args[1] == 'debug'
    should_test = len(args) == 3 and args[2] == 'test'
    if debug:
        init_logging(logging.DEBUG)
    else:
        init_logging(logging.INFO)

    analyzer = PeriodAnalyzer()

    xlm_table = 'matches_xlm'
    eth_table = 'matches_eth'

    """ XLM Alerts """
    xlm_min_diff_chat = TelegramChat(chat_id=XLM_CHAT_IDS['1_min_diff'])
    xlm_diff_1m = alerts.MinuteDiffAlert(name='xlm 1m diff', table=xlm_table,
                                         messenger=xlm_min_diff_chat, analyzer=analyzer, test=should_test)
    xlm_diff_1m.set_threshold(300_000)

    xlm_hour_diff_chat = TelegramChat(chat_id=XLM_CHAT_IDS['1_hr_diff'])
    xlm_diff_1h = alerts.HourAlert(name='xlm 1h_diff', table=xlm_table, messenger=xlm_hour_diff_chat,
                                   analyzer=analyzer, test=should_test)
    xlm_diff_1h.set_threshold(0)

    xlm_four_hour_chat = TelegramChat(chat_id=XLM_CHAT_IDS['FOUR_HOUR'])
    xlm_diff_4h = alerts.FourHourAlert(name='xlm 4h_diff', table=xlm_table, messenger=xlm_four_hour_chat,
                                       analyzer=analyzer, test=should_test)
    xlm_diff_1h.set_threshold(0)

    """ ETH Alerts """
    eth_min_diff_chat = TelegramChat(chat_id=ETH_CHAT_IDS['ETH 1-Minute'])
    eth_diff_1m = alerts.MinuteDiffAlert(name='eth 1m diff', table=eth_table, messenger=eth_min_diff_chat,
                                         analyzer=analyzer, test=should_test)
    eth_diff_1m.set_threshold(200)

    eth_hour_diff_chat = TelegramChat(chat_id=ETH_CHAT_IDS['ETH 1-Hour'])
    eth_diff_1h = alerts.HourAlert(name='eth_1h_diff', table=eth_table, messenger=eth_hour_diff_chat,
                                   analyzer=analyzer, test=should_test)
    eth_diff_1h.set_threshold(0)

    eth_4hour_diff_chat = TelegramChat(chat_id=ETH_CHAT_IDS['ETH 4-Hour'])
    eth_diff_4h = alerts.FourHourAlert(name='eth_4h_diff', table=eth_table, messenger=eth_4hour_diff_chat,
                                   analyzer=analyzer, test=should_test)
    eth_diff_4h.set_threshold(0)

    alerts = [xlm_diff_1m, xlm_diff_1h, xlm_diff_4h]
    # alerts += [eth_diff_1m, eth_diff_1h, eth_diff_4h]

    error_chat = TelegramChat(chat_id=OTHER_CHAT_IDS['JayBotErrors'])

    first_run = True

    while True:
        seconds_into_minute = time.localtime().tm_sec
        if first_run:
            logging.info(f'== Starting Alerts in {60 - seconds_into_minute} seconds .... ==')
            first_run = False
        time.sleep(60 - seconds_into_minute)

        if util.get_host() == 'jaybizserver':
            time.sleep(3)

        if logging.getLogger().level > logging.INFO:
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
