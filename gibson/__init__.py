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
        """ Sets TTL for `key`."""
        query = protocol.make_query(protocol.OP_TTL, self._encoding, key, ttl)
        return self._request(key, query)

        
    def get(self, key):
        """ Returns value for `key`. """
        query = protocol.make_query(protocol.OP_GET, self._encoding, key)
        return self._request(key, query)

   
    def delete(self, key):
        """ Deletes value for `key`. """
        query = protocol.make_query(protocol.OP_DEL, self._encoding, key)
        return self._request(key, query)

   
    def inc(self, key):
        """ Increments value for `key`. """
        query = protocol.make_query(protocol.OP_INC, self._encoding, key)
        return self._request(key, query)

   
    def dec(self, key):
        """ Decrements value for `key`. """
        query = protocol.make_query(protocol.OP_DEC, self._encoding, key)
        return self._request(key, query)


    def lock(self, key, timeout):
        """ Locks `key`. """
        query = protocol.make_query(protocol.OP_LOCK, self._encoding, key, timeout)
        return self._request(key, query)


    def unlock(self, key):
        """ Unlocks `key`. """
        query = protocol.make_query(protocol.OP_UNLOCK, self._encoding, key)
        return self._request(key, query)


    def mset(self, prefix, value):
        """ Sets `value` for all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MSET, self._encoding, prefix, value)
        return self._mrequest(protocol.OP_MSET, prefix, query)


    def mget(self, prefix):
        """ Returs key-value dict for all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MGET, self._encoding, prefix)
        return self._mrequest(protocol.OP_MGET, prefix, query)


    def mttl(self, prefix, timeout):
        """ Sets TTL for all key with `prefix`."""
        query = protocol.make_query(protocol.OP_MTTL, self._encoding, prefix, timeout)
        return self._mrequest(protocol.OP_MTTL, prefix, query)


    def mdelete(self, prefix):
        """ Detetes all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MDEL, self._encoding, prefix)
        return self._mrequest(protocol.OP_MDEL, prefix, query)
    

    def minc(self, prefix):
        """ Increments values of  all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MINC, self._encoding, prefix)
        return self._mrequest(protocol.OP_MINC, prefix, query)


    def mdec(self, prefix):
        """ Decrements values of all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MDEC, self._encoding, prefix)
        return self._mrequest(protocol.OP_MDEC, prefix, query)


    def mlock(self, prefix, timeout):
        """ Locks all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MLOCK, self._encoding, prefix, timeout)
        return self._mrequest(protocol.OP_MLOCK, prefix, query)


    def munlock(self, prefix):
        """ Unlocks all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MUNLOCK, self._encoding, prefix)
        return self._mrequest(protocol.OP_MUNLOCK, prefix, query)

    def count(self, prefix):
        """ Counts all key with `prefix`. """
        query = protocol.make_query(protocol.OP_COUNT, self._encoding, prefix)
        return self._mrequest(protocol.OP_COUNT, prefix, query)


    def sizeOf(self, key):
        """ Returns size of value for `key`. """
        query = protocol.make_query(protocol.OP_SIZEOF, self._encoding, key)
        return self._request(key, query)
    

    def msizeOf(self, prefix):
        """ Returns size of value for all key with `prefix`. """
        query = protocol.make_query(protocol.OP_MSIZEOF, self._encoding, prefix)
        return self._mrequest(protocol.OP_MSIZEOF, prefix, query)

    
    def _mrequest(self, op, prefix, query):
        result = list()
        errors = list()
        connections = self._connection_selector(self._connections, None, prefix)
        if connections:
            for connection in connections:
                if self.debug:
                    logging.debug(
                        "Sending mquery to '{0}'; prefix='{1}'; cmd={2}, size={3}".format(
                            str(connection.address), prefix, *protocol._explain_query(query)))
                try:
                    self._send_query(connection.socket, query)
                except socket.error as err:
                    if err.errno != errno.EPIPE:
                        raise
                    connection.reconnect()
                    self._send_query(connection.socket, query)
                result.append(self._recv_result(connection.socket))
                errors.append(self._last_error)
            if any(errors):
                self._last_error = errors
            print 'errors', errors
            return self._mresult(op, result)
        else:
            raise RuntimeError("The connection selector has returned `None`.")
    
    
    def _mresult(self, op, results):
        result = None
        print 'results', results
        if op in (protocol.OP_MSET, protocol.OP_MTTL, protocol.OP_MDEL, protocol.OP_COUNT,
                  protocol.OP_MINC, protocol.OP_MDEC, protocol.OP_MLOCK, protocol.OP_MUNLOCK, protocol.OP_MSIZEOF):
            result = 0
            for number in results:
                if number is not None:
                    result += number
        elif op in (protocol.OP_MGET, ):
            result = dict()
            for item in results:
                if item is not None:
                    result.update(item)
        return result
        

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
                self._send_query(connection.socket, query)
            except socket.error as err:
                if err.errno != errno.EPIPE:
                    raise
                connection.reconnect()
                self._send_query(connection.socket, query)
            return self._recv_result(connection.socket)
        else:
            raise RuntimeError("The connection selector has returned `None`.")


    def _send_query(self, sock, query):
        while query:
            sent_bytes = sock.send(query)
            query = query[sent_bytes:]        


    def _recv_result(self, sock):
        buff = b''
        self._last_error = None
        buff = sock.recv(self._buffer_size)
        code, buff = protocol.parse_code(buff)
        if code in (protocol.REPL_ERR, protocol.REPL_ERR_NOT_FOUND,
                    protocol.REPL_ERR_NAN, protocol.REPL_ERR_MEM, protocol.REPL_ERR_LOCKED):
            self._last_error = code
            return None

        encoding, datasize, buff = protocol.parse_result_header(buff)
        while len(buff)<datasize:
            buff += sock.recv(self._buffer_size)

        if code!=protocol.REPL_KVAL:
            if code==protocol.REPL_VAL:
                result =  buff if isinstance(buff, str) else buff.decode(self._encoding)
                if encoding==protocol.GB_ENC_NUMBER:
                    result = protocol.conv2number(result)
                elif encoding==protocol.GB_ENC_LZF:
                    raise NotImplementedError('LZF encoding is not implemented') # now reserved
                return result
            else:
                return True
        else:
            result = dict()
            items_cnt, buff = protocol.parse_size(buff)
            while items_cnt:
                key_size, buff = protocol.parse_size(buff)
                key = buff[:key_size]; buff = buff[key_size:]
                encoding, value_size, buff = protocol.parse_result_header(buff)
                value = buff[:value_size]; buff = buff[value_size:]
                if encoding==protocol.GB_ENC_NUMBER:
                    value = protocol.conv2number(value)
                result[key] = value
                items_cnt -= 1
            return result

        