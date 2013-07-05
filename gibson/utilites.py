""" gibson.utilites
"""
import socket
import os.path
import binascii


def _parse_address(item):
    addr, port, family = None, None, socket.AF_UNSPEC
    if isinstance(item, str):
        if ']:' in item and item.startswith('['):
            # may be IP6 as string with format [addr]:port
            addr, port = item[1:].split(']:')
        elif ':' in item and '.' in item:
            # may be IP4 as string with format addr:port
            addr, port = item.split(':')
        elif os.path.exists(item):
            # unix socket
            addr = item
            family = socket.AF_UNIX
    elif isinstance(item, tuple, list) and len(item)==2:
        addr, port = item
    if port is not None:
        try:
            port = int(port)
            if port<0 or port>65535:
                raise
            if ':' in addr:
                ip = socket.inet_pton(socket.AF_INET6, addr)
                family = socket.AF_INET6
            else:
                ip = socket.inet_pton(socket.AF_INET, addr)
                family = socket.AF_INET
        except:
            addr = None    
    return addr, port, family


def _open_socket(addr, port, family, timeout=None, blocking=False):
    if family!=socket.AF_UNIX:
        addr = (addr, port)
    sock = socket.socket(family, socket.SOCK_STREAM, 0)
    if blocking:
        sock.setblocking(1)
        sock.settimeout(timeout or 1.0)
    else:
        sock.setblocking(0)
    sock.connect(addr)
    return sock

      
def _server_selector(servers, key):
    return servers[binascii.crc32(key) % len(servers)]