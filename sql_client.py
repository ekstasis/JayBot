import logging
from typing import List
import socket

import pymysql as pms

databases = {
    'JB-MBP-15': {'host': '127.0.0.1', 'port': 3306,
                  'user': 'gobot', 'password': 'goldenboy',
                  'db': 'CB_XLM_USD_ORDER_BOOK', 'charset': 'utf8mb4',
                  'cursorclass': pms.cursors.DictCursor},
    'debian': {'host': '127.0.0.1', 'unix_socket': '/var/run/mysqld/mysqld.sock',
               'user': 'james', 'password': 'doingbots',
               'db': 'JayBot', 'charset': 'utf8mb4',
               'cursorclass': pms.cursors.DictCursor},
    'debian_from_mac': {'host': '192.168.1.121', 'port': 3306,
                        'user': 'james', 'password': 'doingbots',
                        'db': 'JayBot', 'charset': 'utf8mb4',
                        'cursorclass': pms.cursors.DictCursor},
    'jaybizserver': {'host': '66.228.58.116', 'port': 3306,
                     'user': 'james', 'password': 'Ccess9atabase',
                     'db': 'JayBiz', 'charset': 'utf8mb4',
                     'cursorclass': pms.cursors.DictCursor}
}


def connection(host=None):
    if host is None:
        info = databases[socket.gethostname()]
    else:
        info = databases[host]

    return pms.connect(**info)


def do_query(query):
    """ Executes query on bot's db and returns result of fetchall """
    conn = connection()
    try:
        logging.debug("conn.host: %s", conn.host)
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    except:
        logging.error('Failure to execute query:  %s', query)
        raise
    finally:
        conn.close()
    return result


def do_query_with(conn, query):
    """ Executes query with a given db connection, leaving it open """
    if not conn.open:
        conn.connect()
    try:
        logging.debug("conn.host: %s", conn.host)
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    except:
        logging.error('Failure to execute query:  %s', query)
        conn.close()
        raise
    return result


def write_records(query, bot='devbot'):
    """ Executes update/insert query and returns # of rows effected
    """
    conn = connection(bot)
    try:
        with conn.cursor() as cursor:
            rows = cursor.execute(query)
            logging.debug('rows changed: %s', rows)
        conn.commit()
    except:
        logging.exception('Failure to write query to DB:  %s', query)
        raise
    finally:
        conn.close()
    return rows


def test_connection(conn, test_query=None):
    if not test_query:
        test_query = "SELECT * FROM tradingpairs LIMIT 1"
    logging.debug('Host:  %s' % conn.host)
    logging.debug('Host_Info:  %s' % conn.host_info)
    logging.debug('Database name:  "%s"' % conn.db.decode())
    logging.debug('Test query:  "%s"' % test_query)
    c = conn.cursor()
    c.execute(test_query)
    result = c.fetchall()
    return result


def write_list_of_queries(queries: List[str], conn: pms.connections.Connection = None, bot: str = 'config'):
    if conn is None:
        conn = connection(bot)
    try:
        with conn as cursor:
            for qry in queries:
                cursor.execute(qry)
        conn.commit()
    except:
        logging.exception('Failed to write queries: %s', queries)
        raise
    finally:
        conn.close()


def get_connect_info(conn: pms.connections.Connection):
    keys = ['host', 'port', 'user', 'password', 'db', 'charset', 'cursorclass']
    info = {key: conn.__dict__[key] for key in keys}
    return info


""" Example from https://pymysql.readthedocs.io/en/latest/user/examples.html
import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='user',
                             password='passwd',
                             db='db',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Create a new record
        sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
        cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

    # connection is not autocommit by default. So you must commit to save
    # your changes.
    connection.commit()

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
        cursor.execute(sql, ('webmaster@python.org',))
        result = cursor.fetchone()
        print(result)
finally:
    connection.close()
"""
