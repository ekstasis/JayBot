import logging
import logging.handlers as lh
# import helpers as hlp

""" Log Formatting Defaults """
log_func_fmt = '%(levelname)s %(funcName)s(): %(message)s'
log_file_fmt = '%(levelname)s %(asctime)s %(module)s: %(message)s'
brief_datetime_fmt = '%m%d %H:%M:%S'


def main():
    init_logging(logging.DEBUG, emaillevel=logging.ERROR, email_addresses=jb)
    # logging.debug('debug')
    # logging.info('info')
    # logging.error('logging.error()')
    try:
        print(1/0)
    except ZeroDivisionError:
        logging.exception('oh no!')
    logging.shutdown()


def init_logging(rootlevel, filelevel=None, filename=None,
                 emaillevel=None, email_addresses=None):
    # specify rootlevel to cause logging to stdout
    if rootlevel is not None:
        logging.basicConfig(level=rootlevel, format=log_func_fmt,
                            datefmt=brief_datetime_fmt)
        root = logging.getLogger()
    else:  # otherwise just set default level of logging
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

    # specify filelevel and filename to start file logging
    if filelevel is not None:
        logfile = log_file_path(filename)
        file_handler = logging.FileHandler(filename=logfile, mode='a') # APPEND
        file_handler.setLevel(filelevel)
        formatter = logging.Formatter(log_file_fmt, datefmt=brief_datetime_fmt)
        file_handler.setFormatter(formatter)
        if file_handler not in root.handlers:
            root.addHandler(file_handler)

    if emaillevel is not None:  # and not hlp.jb_mac_check():
        email_handler = setup_email_handler(emaillevel, email_addresses)
        try:
            root.addHandler(email_handler)
        except:
            logging.exception('Failed to add email logging handler to logging system')


def setup_email_handler(level, addresses):
    smtp_ip = hlp.get_config_value('smtp')
    port = 25
    fromaddr = 'imabot@jgfilms.com'
    subject = '[%s] Errors were logged' % hlp.host_name().upper()
    capacity = 100

    handler = BufferingSMTPHandler(mailhost=smtp_ip, port=port, fromaddr=fromaddr, toaddrs=addresses,
                                   subject=subject, capacity=capacity)
    handler.setLevel(level)
    return handler


def log_file_path(name):
    """ Returns string with path to /blah/blah/gobot/logs/*name*.log
    """
    return log_dir_path() + name


def log_dir_path():
    """ Returns string with path to /blah/blah/gobot/logs/
    """
    pathArray = __file__.split('/')[1:]
    firstGobot = pathArray.index('gobot')
    botPath = "/" + "/".join(pathArray[:firstGobot+1])
    logFilePath = botPath + "/logs/"
    return logFilePath


class BufferingSMTPHandler(lh.BufferingHandler):
    """ BufferingSMTPHandler, an alternative implementation of SMTPHandler.
    Copyright (C) 2001-2002 Vinay Sajip. All Rights Reserved."""
    def __init__(self, mailhost, port, fromaddr, toaddrs, subject, capacity, user=None, pw=None):
        lh.BufferingHandler.__init__(self, capacity)
        self.mailhost = mailhost
        self.mailport = port
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.user = user
        self.pw = pw
        self.setFormatter(logging.Formatter(log_file_fmt, datefmt=brief_datetime_fmt))

    def flush(self):
        if len(self.buffer) > 0:
            if self.toaddrs == 'none':
                logging.warning("Error Emails Not Being Sent because config.ini email setting is 'none'")
                self.buffer = []
                return
            try:
                import smtplib
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_PORT
                smtp = smtplib.SMTP(self.mailhost, port)
                msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % \
                      (self.fromaddr, self.toaddrs, self.subject)
                for record in self.buffer:
                    s = self.format(record)  # todo: can store count of these for spam control?
                    msg = msg + s + "\n"
                if self.user:
                    smtp.ehlo()  # for tls add this line
                    smtp.starttls()  # for tls add this line
                    smtp.ehlo()  # for tls add this line
                    smtp.login(self.user, self.pw)
                smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                smtp.quit()
            except:
                self.handleError(None)  # no particular record
            self.buffer = []


class TlsSMTPHandler(logging.handlers.SMTPHandler):
    def emit(self, record):
        """ Emit a record.  Format the record and send it to the specified addressees.
        """
        try:
            import smtplib
            #            import string # for tls add this line
            #             try:
            from email.utils import formatdate
            # except ImportError:
            #     formatdate = self.date_time
            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port)
            msg = self.format(record)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.fromaddr,
                ",".join(self.toaddrs),
                self.getSubject(record),
                formatdate(), msg)
            if self.username:
                smtp.ehlo()  # for tls add this line
                smtp.starttls()  # for tls add this line
                smtp.ehlo()  # for tls add this line
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


if __name__ == '__main__':
    main()
