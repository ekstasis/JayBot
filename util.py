import socket


def get_host() -> str:
    host = socket.gethostname()
    # if host == 'JB-MBP-15':
    #     host = 'debian_from_mac'
    return host