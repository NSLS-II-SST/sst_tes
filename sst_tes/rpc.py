from ophyd.ophydobj import OphydObject
import json
import socket
import zmq


class RPCException(Exception):
    pass


class RPCInterface(OphydObject):
    def __init__(self, *args, address="", port=None, client='socket', **kwargs):
        super().__init__(*args, **kwargs)
        if port is not None:
            if client == 'zmqreq':
                self.rpc = ZMQREQClient(address, port)
            elif client == 'socket':
                self.rpc = SocketClient(address, port)
            else:
                raise ValueError(f"client type {client} not understood")
                
        else:
            self.rpc = self._get_comm_function()

    def describe_rpc(self):
        return f'RPC:{self.rpc.address}:{self.rpc.port}'
        
    def _get_comm_function(self):
        if hasattr(self, "rpc"):
            return self.rpc
        else:
            parent = self.parent
            return self._get_comm_tail(parent)
        
    def _get_comm_tail(self, parent):
        if hasattr(parent, "rpc"):
            return parent.rpc
        elif hasattr(parent, "parent"):
            if parent is None:
                raise IOError("No parent has an RPC Client")
            return self._get_comm_tail(parent.parent)
        else:
            raise IOError("No parent has an RPC Client")


class JSONClientBase:
    def __init__(self, address, port):
        self.address = address
        self.port = port

    def formatMsg(self, method, *params, **kwargs):
        msg = {"method": method}
        if params is not None and params != []:
            msg["params"] = params
        if kwargs is not None and kwargs != {}:
            msg["kwargs"] = kwargs
        return json.dumps(msg).encode()

    def __getattr__(self, attr):
        def _method(*params, **kwargs):
            return self.sendrcv(attr, *params, **kwargs)
        return _method

class SocketClient(JSONClientBase):
    """
    JSON RPC over a built-in TCP Socket
    """
    def sendrcv(self, method, *params, **kwargs):
        msg = self.formatMsg(method, *params, **kwargs)
        s = socket.socket()
        s.connect((self.address, self.port))
        s.send(msg)
        m = json.loads(s.recv(1024).decode())
        s.close()
        return m

class ZMQREQClient(JSONClientBase):
    """
    JSON RPC over a ZMQ Req socket
    """
    def __init__(self, address, port, timeout=5000):
        self.ctx = zmq.Context()
        self.address = address
        self.port = port
        self.addrstr = f"tcp://{address}:{port}"
        self.timeout = timeout

    def sendrcv(self, method, *params, **kwargs):
        msg = self.formatMsg(method, *params, **kwargs)
        s = self.ctx.socket(zmq.REQ)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(self.addrstr)
        s.send(msg)
        # https://zguide.zeromq.org/docs/chapter4/
        # Not implementing retries because it is unlikely to matter
        if (s.poll(self.timeout) & zmq.POLLIN) != 0:
            m = s.recv_json()
            s.close()
            return m
        else:
            s.close()
            raise TimeoutError(f"ZMQ Communication with {self.addrstr} timed out after {self.timeout}ms")

