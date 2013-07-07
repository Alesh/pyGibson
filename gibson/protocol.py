""" gibson.protocol
"""

import struct

OP_SET     = 0x01
OP_TTL     = 0x02
OP_GET     = 0x03
OP_DEL     = 0x04
OP_INC     = 0x05
OP_DEC     = 0x06
OP_LOCK    = 0x07
OP_UNLOCK  = 0x08
OP_MSET    = 0x09
OP_MTTL    = 0x0A
OP_MGET    = 0x0B
OP_MDEL    = 0x0C
OP_MINC    = 0x0D
OP_MDEC    = 0x0E
OP_MLOCK   = 0x0F
OP_MUNLOCK = 0x10
OP_COUNT   = 0x11
OP_STATS   = 0x12
OP_PING    = 0x13
OP_SIZEOF  = 0x14
OP_MSIZEOF = 0x14
OP_ENCOF   = 0x15
OP_END     = 0xFF


REPL_ERR           = 0x00 # Generic error while executing the query.
REPL_ERR_NOT_FOUND = 0x01 # Specified key was not found.
REPL_ERR_NAN       = 0x02 # Expected a number ( TTL or TIME ) but the specified value was invalid.
REPL_ERR_MEM       = 0x03 # The server reached configuration memory limit and will not accept any new value until its freeing routine will be executed.
REPL_ERR_LOCKED    = 0x04 # The specificed key was locked by a OP_LOCK or a OP_MLOCK query.
REPL_OK            = 0x05 # Query succesfully executed, no data follows.
REPL_VAL           = 0x06 # Query succesfully executed, value data follows.
REPL_KVAL          = 0x07 # Query succesfully executed, multiple key => value data follows.

COUNTER_FMT         = '=q'  # Warning: only for 64 bit servers
QUERY_HEADER_FMT    = '=IH'
RESPONSE_CODE_FMT   = '=H'
RESPONSE_HEADER_FMT = '=BQ' # Warning: only for 64 bit servers
SIZE_FMT = '=Q'

GB_ENC_PLAIN  = 0x00 # Raw string data follows.
GB_ENC_LZF    = 0x01 # Compressed data, this is a reserved value not used for replies.
GB_ENC_NUMBER = 0x02 # Packed long number follows, four bytes for 32bit architectures, eight bytes for 64bit.


def conv2number(data):
    size = struct.calcsize(COUNTER_FMT)
    result,  = struct.unpack(COUNTER_FMT, data[:size])
    return result
    

def make_query(op_code, encoding, *args):
    query_data = b''
    for arg in args:
        if not isinstance(arg, str):
            arg = str(arg)
        query_data += (b' ' if query_data else b'') + arg.encode(encoding)
    query_length = len(query_data) + 2
    return struct.pack(QUERY_HEADER_FMT, query_length, op_code)+query_data


def _explain_query(query):
    size, cmd = struct.unpack(QUERY_HEADER_FMT, query[:struct.calcsize(QUERY_HEADER_FMT)])
    return (cmd, size)
    

def parse_code(data):
    size = struct.calcsize(RESPONSE_CODE_FMT)
    remainder = data[size:]
    code,  = struct.unpack(RESPONSE_CODE_FMT, data[:size])
    return code, remainder


def parse_result_header(data):
    size = struct.calcsize(RESPONSE_HEADER_FMT)
    remainder = data[size:]
    encoding, datasize = struct.unpack(RESPONSE_HEADER_FMT, data[:size])
    return encoding, datasize, remainder

def parse_size(data):
    size = struct.calcsize(SIZE_FMT)
    remainder = data[size:]
    result = struct.unpack(SIZE_FMT, data[:size])[0]
    return result, remainder
    
    

    