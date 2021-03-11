import datetime as dt
import logging
import os
import socket
import subprocess
import sys
from configparser import ConfigParser, ExtendedInterpolation
from functools import lru_cache

import pandas as pd
import pymysql as pms

import gobotDB as db


""" Server stuff """


def host_name():
    return socket.gethostname()


def jb_mac_check():
    return 'JB-' in host_name()


def get_config(section=None, config_file_path=None):
    """ Returns the entire config or just the config for "section"
        if specified (i.e., particular host)
    """
    if config_file_path is None:
        config_file_path = bot_path() + 'app_support/config.ini'
    config = ConfigParser(default_section='defaults', interpolation=ExtendedInterpolation())
    config.read(config_file_path)
    if section is not None:
        return config[section]
    else:
        return config

def get_config_value(key, host=None):
    if host is None:
        host = host_name()
    return get_config(host)[key]


def server_info(config=None, incl_db=True):
    """ Extracts server connection info from a config if given, otherwise
        uses the config section for the dectected host
    """
    if config is None:
        host = host_name()
        config = get_config(section=host)

    keys = ['host', 'user', 'password', 'charset']
    if incl_db:
        keys += ['database']
    server = {key: config[key] for key in keys}
    return server


""" Misc
"""
def float2string(flt, places=8):
    return '{num:.{dec}f}'.format(num=flt, dec=places)

def a_float2string(flts, places=8):
    """ takes and returns array """
    return [ float2string(item, places) for item in flts]

def time_hours_ago(hours, time=None):
    """ For determing datetime some numbers of hours prior to either now or a specified date
    """
    now = dt.datetime.utcnow() if not time else time
    offset = dt.timedelta(hours=hours)
    return now - offset


def prevent_mac_sleep():
    """ uses process number to start sleeping agin when python finishes
    """
    pid = os.getpid()
    args = ['caffeinate', '-w', str(pid)]
    message = 'Using "caffeinate" to prevent system sleep while process #%i is running ...' % pid
    logging.debug(message)
    subprocess.Popen(args)

def time_range(frm, until=0):
    # start, end are "hours ago"
    now = pd.datetime.utcnow()
    f = pd.DateOffset(hours=frm)
    t = pd.DateOffset(hours=until)
    start = now - f
    end = now - t
    return start, end
