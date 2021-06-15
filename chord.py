import random
import argparse
import socket
import threading
import time
import pickle
import zmq
import logging
from constants import *

log = logging.Logger(name='chord node')
logging.basicConfig(level=logging.DEBUG)

THREADS = 5

class conn:
    def __init__(self, id, address):
        self.nodeID = id
        self.address = address

    def __eq__(self, other):
        return not other is None and self.nodeID == other.nodeID \
               and self.address == other.address

    def __str__(self):
        return str(self.nodeID)

    def __repr__(self):
        return self.__str__()


class Node:
    def __init__(self, dns, role, portin=5000, portout=5001):
        self.replication = 5

        self.dns = dns
        self.role = role
        host = socket.gethostname()
        host = socket.gethostbyname(host)

        self.context = zmq.Context()

        self.lsock = self.context.socket(zmq.ROUTER)
        self.listen_address = f'tcp://{host}:{portin}'
        self.lsock.bind(self.listen_address)

        self.worker_address = f'inproc://workers{portin}'
        self.wsock = self.context.socket(zmq.DEALER)
        self.wsock.bind(self.worker_address)
        for i in range(THREADS):
            threading.Thread(target=self.worker, args=(self.worker_address,)).start()


        self.ssem = threading.Semaphore()
        self.ssock = self.context.socket(zmq.DEALER)
        self.ssock.bind(f'tcp://{host}:{portout}')
        self.ssock.setsockopt(zmq.RCVTIMEO, 10000)

        server = f'tcp://{self.dns[0]}:{self.dns[1]}'
        reply = self.ssocket_send((JOIN_GROUP, self.role, self.listen_address), server)
        if reply is None:
            raise Exception('server not responding')
        self.nodeID, self.NBits = reply
        self.MAXNodes = (1 << self.NBits)
        self.FTsem = threading.Semaphore()
        self.FT = [self.conn for _ in range(self.NBits + 1)]
        self.predecessor = None

        self.succsem = threading.Semaphore()
        self.successors = []

        log.warning(f'node {self.nodeID} started')
        exceptions = [self.nodeID]
        while True:
            reply = self.ssocket_send((RANDOM_NODE, self.role, exceptions), server)
            if reply is None:
                raise Exception('server not responding')
            id, name = reply
            if name is None:
                log.warning('node starts alone')
                break
            log.warning(f'joining node {id}')

            if self.ping(name):
                node = conn(id, name)
                self.join(node)
                break
            else:
                exceptions.append(id)

        log.warning('starting daemons')
        threading.Thread(target=self.stabilize_daemon).start()
        threading.Thread(target=zmq.device, args=(zmq.QUEUE, self.lsock, self.wsock,)).start()
        #zmq.device(zmq.QUEUE, self.lsock, self.wsock)


        threading.Thread(target=self.successors_daemon).start()

    def ssocket_send(self, msg, address, WaitForReply=True):
        msg = pickle.dumps(msg)
        self.ssem.acquire()
        self.ssock.connect(address)
        self.ssock.send(msg)
        if WaitForReply:
            try:
                reply = pickle.loads(self.ssock.recv())
            except Exception as e:
                reply = None
        else:
            reply = None
        self.ssock.disconnect(address)
        self.ssem.release()
        return reply


    def ping(self, node):
        if node == self.listen_address: return True
        reply = self.ssocket_send((PING, None), node)
        return not reply is None and reply == PONG

    def worker(self, worker_address):
        sock = self.context.socket(zmq.ROUTER)
        sock.connect(worker_address)

        while True:
            ident1, ident2, data = sock.recv_multipart()

            data = pickle.loads(data)
            response = self.manage_request(data)

            bits = pickle.dumps(response)
            sock.send_multipart([ident1, ident2, bits])

    def manage_request(self, data):
        code, *args = data
        response = None
        if code == PING:
            # log.warning(f'received PING request')
            response = PONG

        if code == NODEID:
            log.warning(f'received NODEID request')
            response = self.nodeID

        if code == SUCCESSOR:
            log.warning(f'received SUCCESSOR request')
            response = self.successor

        if code == PREDECESSOR:
            log.warning(f'received PREDECESSOR request')
            response = self.predecessor

        if code == NOTIFY:
            conn = args[0]
            log.warning(f'received NOTIFY request for node {conn}')
            self.notify(conn)

        if code == LOOKUP:
            log.warning(f'received LOOKUP request')
            conn = self.lookup(args[0])
            response = conn

        if code == FIND_PREDECESSOR:
            log.warning(f'received FIND_PREDECESSOR request')
            conn = self.find_predecessor(args[0])
            log.warning(f'predecessor of {args[0]} is {conn}')
            response = conn

        return response



    @property
    def conn(self):
        return conn(self.nodeID, self.listen_address)

    def _successor_(self, address):
        if address == self.listen_address:
            return self.successor
        return self.ssocket_send((SUCCESSOR, None), address)

    @property
    def successor(self):
        '''
        node = self.FT[1]
        if node is None or not self.ping(node.address):
            self.succsem.acquire()
            if len(self.successors) > 0:
                self.successors.pop(0)
            self.succsem.release()
            self.FTsem.acquire()
            if len(self.successors) > 0:
                self.FT[1] = self.successors[0]
            else:
                self.FT[1] = self.conn
            self.FTsem.release()
            return self.successor
        '''
        return self.FT[1]

    @successor.setter
    def successor(self, value):
        self.FTsem.acquire()
        self.FT[1] = value
        self.FTsem.release()
        self.succsem.acquire()
        self.successors = [value]
        self.succsem.release()

    def _predecessor_(self, address):
        if address == self.listen_address:
            return self.predecessor
        return self.ssocket_send((PREDECESSOR, None), address)

    @property
    def predecessor(self):
        return self.FT[0]

    @predecessor.setter
    def predecessor(self, value):
        self.FTsem.acquire()
        self.FT[0] = value
        self.FTsem.release()

    def start(self, i):
        return (self.nodeID + (1<<(i - 1))) % self.MAXNodes

    def finger(self, i):
        return self.FT[i]


    # Says if node id is in range [a,b)
    def between(self, id, a, b):
        #log.warning(f'node {self.nodeID}: asked if {a}<={id}<{b}')
        if a < b: return id >= a and id < b
        return id >= a or id < b


    def join(self, node):
        while True:
            succ = self.ssocket_send((LOOKUP, (self.nodeID + 1) %  self.MAXNodes), node.address)
            self.successor = succ
            if not succ is None:
                self._notify_(self.conn, succ.address)
                log.warning(f'successor is {succ.nodeID}')
                break

    def lookup(self, id):
        p = self.find_predecessor(id)
        if p is None: return self.conn
        ret = self._successor_(p.address)
        return ret

    def _find_predecessor_(self, id, address):
        log.warning(f'calling remote predecessor for {id} to {address}')
        return self.ssocket_send((FIND_PREDECESSOR, id), address)

    def find_predecessor(self, id):
        log.warning(f'trying to find predecessor of {id}')
        succ = self.successor
        if succ is None or self.between(id, self.nodeID + 1, succ.nodeID + 1):
            return self.conn
        cur = self.closest_preceding_finger(id)
        if cur == self.conn:
            return cur
        return self._find_predecessor_(id, cur.address)

    def closest_preceding_finger(self, id):
        log.warning(f'searching closest finger for {id}')
        for i in range(self.NBits, 0, -1):
            f = self.finger(i)
            if f is None: continue
            if self.between(f.nodeID, self.nodeID + 1, id):
                return f
        return self.conn


    def stabilize_daemon(self):
        while True:
            log.warning(f'FT[{self.nodeID}]={[i for i in self.FT]}')
            time.sleep((len(self.successors) + 1) * 3)
            self.stabilize()
            self.fix_finger()

    def stabilize(self):
        log.warning('stabilizing')
        succ = self.successor
        p = self._predecessor_(succ.address)
        if not p is None:
            if p == self.conn:
                return
            if self.conn == succ or self.between(p.nodeID, self.nodeID, succ.nodeID):
                self.successor = p
                self._notify_(self.conn, p.address)


    def _notify_(self, node, address):
        log.warning(f'sending NOTIFY to {address}')
        self.ssocket_send((NOTIFY, node), address, False)

    def notify(self, node):
        p = self.predecessor
        if p is None or not self.ping(p.address) or self.between(node.nodeID,
            self.predecessor.nodeID + 1, self.nodeID):
            self.predecessor = node

    def fix_finger(self):
        i = random.randint(2, self.NBits)
        self.FTsem.acquire()
        self.FT[i] = self.lookup(self.start(i))
        self.FTsem.release()


    def successors_daemon(self):
        while True:
            self.fix_successors()
            time.sleep((len(self.successors) + 1) * 5)

    def fix_successors(self):
        if len(self.successors) == 0: return
        if len(self.successors) < self.NBits:
            if not self.ping(self.successor.address):
                self.succsem.acquire()
                self.successors.pop(0)
                self.succsem.release()
                self.FTsem.acquire()
                if len(self.successors) > 0:
                    self.FT[1] = self.successors[0]
                else:
                    self.FT[1] = self.conn
                self.FTsem.release()
                log.warning(f'Successors: {[i for i in self.successors]}')
                return

            node = self.successors[-1]
            if node is None or not self.ping(node.address):
                self.succsem.acquire()
                self.successors.pop()
                self.succsem.release()
                return
            new = self._successor_(node.address)
            if not new is None and not new in self.successors and new != self.conn:
                self.succsem.acquire()
                self.successors.append(new)
                self.succsem.release()
                #log.warning(f'added {new} as successor')
                log.warning(f'Successors: {[i for i in self.successors]}')





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ns', '--nameserver', required=True, type=str, help='Name server address')
    parser.add_argument('--port1', default=5000, required=False, type=int, help='Port for incoming communications')
    parser.add_argument('--port2', default=5001, required=False, type=int, help='Port for outgoing communications')
    parser.add_argument('-r', '--role', default='chordNode', required=False, type=str, help='Node role')
    args = parser.parse_args()

    nameserver = args.nameserver
    role = args.role

    host, port = nameserver.split(':')
    port = int(port)
    nameserver = (host, port)

    port1 = args.port1
    port2 = args.port2

    node = Node(nameserver, role, port1, port2)
