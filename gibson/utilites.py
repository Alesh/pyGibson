""" gibson.utilites
"""
import socket
import os.path
import logging
import binascii



class Connection(object):
    """ Gibson server connection.
    
    Args:
        address: Address of server socket. This is string of unix socket address or IP4 representation
                 like '127.0.0.1:10128' or IP6 like '[::1]:10128'. Or tuple (address, port)
        timeout: Socket operations timeout. Default `1.0`.
        blocking: Blocking socket or not. Default `True`.
    
    """
    def __init__(self, address, timeout=None, blocking=True):
        self._sock = None
        self._blocking = 1 if blocking else 0
        self._timeout = float(timeout) or 1.0
        self._addr, self._port, self._family = self._parse_address(address)
        if self._addr is not None:
            self._sock = self._open_socket(self._addr, self._port, self._family, self._timeout, self._blocking)
        else:
            raise ValueError("Incorrect value of 'address' argument.")
    
    @property
    def address(self):
        """ Connection address. """
        return self._addr

    @property
    def port(self):
        """ Connection port. """
        return self._port

    @property
    def family(self):
        """ Connection socket family. """
        return self._family

    @property
    def socket(self):
        """ Connection socket. """
        return self._sock
    
    
    def close(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock = None

    
    def reconnect(self):
        self._sock = self._open_socket(self._addr, self._port, self._family, self._timeout, self._blocking)
        return self._sock is not None
    

    @staticmethod
    def default_selector(connections, key=None, prefix=None):
        """ The default function, which selects the target
        servers to send commands based on the parameters `key` or `prefix`.
        
        Args:
            connections: List of `Connection` objects.
            key: Argument `key`  for single key set operation which needs list of target servers.
            prefix: Argument `prefix` for multiple key set operation which needs list of target servers.
        """        
        if key is not None:
            return [connections[binascii.crc32(key) % len(connections)]]
        elif prefix is not None:
            return connections

    
    @staticmethod
    def _parse_address(value):
        addr, port, family = None, None, socket.AF_UNSPEC
        if isinstance(value, str):
            if ']:' in value and value.startswith('['):
                # may be IP6 as string with format [addr]:port
                addr, port = value[1:].split(']:')
            elif ':' in value and '.' in value:
                # may be IP4 as string with format addr:port
                addr, port = value.split(':')
            elif os.path.exists(os.path.dirname(value)):
                # unix socket
                addr = value
                family = socket.AF_UNIX
        elif isinstance(value, tuple, list) and len(value)==2:
            addr, port = value
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
    
    
    @staticmethod
    def _open_socket(addr, port, family, timeout=None, blocking=True):
        if family!=socket.AF_UNIX:
            addr = (addr, port)
        try:
            sock = socket.socket(family, socket.SOCK_STREAM, 0)
            if blocking:
                sock.setblocking(1)
                sock.settimeout(timeout or 1.0)
            else:
                sock.setblocking(0)
            sock.connect(addr)
        except:
            logging.exception('Gibson client: Exception occurred when opening socket.')
            sock = None
        return sock