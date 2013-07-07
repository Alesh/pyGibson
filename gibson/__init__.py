""" gibson
"""
import errno
import socket
import logging
import gibson.utilites
from gibson import protocol

class Client(object):
    """ Gibson client.
    
    Args:
        servers: List of values  describing the server connections.
            See format in `gibson.utilites.Connection` argument `address`.
        timeout: Network operations timeout.
        buffer_size: Network operations buffer size.
        debug: If `True` debug logging will be activated.
        connection_selector: Function, which selects the target servers to send commands.
            It must interface the same as `gibson.utilites.Connection.default_selector`.
            See details there.
    """
    def __init__(self, servers, timeout=None, debug=False,
                 buffer_size=None, connection_selector=None):
        self.debug = debug
        self.timeout = float(timeout or 1.0)
        self._last_error = None
        self._encoding = 'utf-8'
        self._connections = list()
        self._buffer_size = buffer_size or 1024*4
        self._connection_selector = connection_selector or gibson.utilites.Connection.default_selector
        for item in servers:
            connection = gibson.utilites.Connection(item, self.timeout, True)
            if connection.address is None:
                raise ValueError("Incorrect item '{0}' in 'servers' argument.".format(item))
            if connection.socket is None:
                raise RuntimeError("Cannot connect to '{}'.".format(item))
            self._connections.append(connection)
        if not self._connections:
            raise ValueError("Incorrect value of 'servers' argument.")

    @property
    def connected(self):
        """ Returns `True` if client connected. """
        return all([connection.socket is not None for connection in self._connections])

    @property
    def lastError(self):
        """ Last error. """
        return self._last_error
    
    
    def close(self):
        """ Closes all connections. """
        for connection in self._connections:
            connection.close()
            

    def set(self, key, value, ttl=None):
        """ Sets value to store with `key`."""
        query = protocol.make_query(protocol.OP_SET, self._encoding, ttl or 0, key, value)
        return self._request(key, query)
    

    def ttl(self, key, ttl):
        """ Sets TTL for `key` in store."""
        query = protocol.make_query(protocol.OP_TTL, self._encoding, key, ttl)
        return self._request(key, query)

        
    def get(self, key):
        """ Returns value for `key` from store. """
        query = protocol.make_query(protocol.OP_GET, self._encoding, key)
        return self._request(key, query)

   
    def delete(self, key):
        """ Deletes value for `key` from store. """
        query = protocol.make_query(protocol.OP_DEL, self._encoding, key)
        return self._request(key, query)

   
    def inc(self, key):
        """ Increments value for `key` in store. """
        query = protocol.make_query(protocol.OP_INC, self._encoding, key)
        return self._request(key, query)

   
    def dec(self, key):
        """ Decrements value for `key` in store. """
        query = protocol.make_query(protocol.OP_DEC, self._encoding, key)
        return self._request(key, query)


    def lock(self, key, timeout):
        """ Locks `key` in store. """
        query = protocol.make_query(protocol.OP_LOCK, self._encoding, key, timeout)
        return self._request(key, query)


    def unlock(self, key):
        """ Unlocks `key` in store. """
        query = protocol.make_query(protocol.OP_UNLOCK, self._encoding, key)
        return self._request(key, query)


    def _request(self, key, query):
        def _send_query(sock, query):
            while query:
                sent_bytes = sock.send(query)
                query = query[sent_bytes:]        
        connections = self._connection_selector(self._connections, key)
        if connections:
            connection = connections[0]
            if self.debug:
                logging.debug(
                    "Sending query to '{0}'; key='{1}'; cmd={2}, size={3}".format(
                        str(connection.address), key, *protocol._explain_query(query)))
            try:
                _send_query(connection.socket, query)
            except socket.error as err:
                if err.errno != errno.EPIPE:
                    raise
                connection.reconnect()
                _send_query(connection.socket, query)
            return self._recv_result(connection.socket)
        else:
            raise RuntimeError("The connection selector has returned `None`.")


    def _recv_result(self, sock):
        buff = b''
        self._last_error = None
        buff = sock.recv(self._buffer_size)
        code, buff = protocol.parse_code(buff)
        if code in (protocol.REPL_ERR, protocol.REPL_ERR_NOT_FOUND,
                    protocol.REPL_ERR_NAN, protocol.REPL_ERR_MEM, protocol.REPL_ERR_LOCKED):
            self._last_error = code
            return None
        elif code!=protocol.REPL_KVAL:
            if code==protocol.REPL_VAL:
                encoding, datasize, buff = protocol.parse_result_header(buff)
                while len(buff)<datasize:
                    buff += sock.recv(self._buffer_size)
                result =  buff if isinstance(buff, str) else buff.decode(self._encoding)
                if encoding==protocol.GB_ENC_NUMBER:
                    result = protocol.conv2number(result)
                elif encoding==protocol.GB_ENC_LZF:
                    raise NotImplementedError('LZF encoding is not implemented') # now reserved
                return result
            else:
                return True
        else:
            pass # ToDo: read multi result