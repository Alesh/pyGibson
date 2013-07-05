""" gibson.client
"""
import errno
import socket
from gibson.utilites import _parse_address, _open_socket, _server_selector
from gibson import protocol

class Client(object):
    
    def __init__(self, servers, debug=None, timeout=None, server_selector=None):
        self._servers = list()
        self._encoding = 'utf-8'
        self._buffer_size = 1024*4
        self.debug = True if debug else False
        self.timeout = float(timeout or 1.0)
        self._set_servers(servers)
        self._server_selector = server_selector or _server_selector
        self._last_error = None


    @property
    def connected(self):
        return all([item[0] is not None for item in self._servers])
    
    
    def close(self):
        for item in self._servers:
            sock, _, _, _ = item
            if sock is not None:
                sock.shutdown(socket.SHUT_RDWR)
                item[0] = None

    
    def set(self, key, value, ttl=None):
        query = protocol.make_query(protocol.OP_SET, self._encoding, ttl or 0, key, value)
        return self._request(key, query)
    

    def ttl(self, key, ttl):
        query = protocol.make_query(protocol.OP_TTL, self._encoding, key, ttl)
        return self._request(key, query)

        
    def get(self, key):
        query = protocol.make_query(protocol.OP_GET, self._encoding, key)
        return self._request(key, query)

   
    def delete(self, key):
        query = protocol.make_query(protocol.OP_DEL, self._encoding, key)
        return self._request(key, query)

   
    def inc(self, key):
        query = protocol.make_query(protocol.OP_INC, self._encoding, key)
        return self._request(key, query)

   
    def dec(self, key):
        query = protocol.make_query(protocol.OP_DEC, self._encoding, key)
        return self._request(key, query)


    def _request(self, key, query):
        sock, connection = self._select_socket(key)
        try:
            self._send_query(sock, query)
        except socket.error as err:
            if err.errno != errno.EPIPE:
                raise
            sock = self._reconnect(connection)
            self._send_query(sock, query)
        return self._recv_result(sock)

    def _send_query(self, sock, query):
        while query:
            sent_bytes = sock.send(query)
            query = query[sent_bytes:]

    def _recv_result(self, sock):
        buff = b''
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
                self._last_error = None
                result =  buff if isinstance(buff, str) else buff.decode(self._encoding)
                if encoding==protocol.GB_ENC_NUMBER:
                    result = protocol.conv2number(result)
                elif encoding==protocol.GB_ENC_LZF:
                    pass # now reserved
                return result
            else:
                return True
        else:
            pass # ToDo: read multi result

    def _set_servers(self, servers):
        if len(servers)==0:
            raise ValueError("Incorrect value of 'servers' argument.")
        for item in servers:
            addr, port, family = _parse_address(item)
            if addr is None:
                raise ValueError("Incorrect value of 'servers' argument.")
            sock = _open_socket(addr, port, family, self.timeout, True)
            self._servers.append([sock, addr, port, family])
    
    def _reconnect(self, connection):
        sock, addr, port, family = connection
        new_sock = _open_socket(addr, port, family, self.timeout, True)
        index = self._servers.index([sock, addr, port, family])
        self._servers[index][0] = new_sock
        return new_sock
    
    def _select_socket(self, key):
        if self.connected:
            sock, addr, port, family = self._server_selector(self._servers, key)
        else:
            raise RuntimeError('Client disconnected.')
        return sock, (sock, addr, port, family)
        


