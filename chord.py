import random
import argparse
import socket
import threading
import time
import pickle
import zmq
import broker
import logging

log = logging.Logger(name='chord node')
logging.basicConfig(level=logging.DEBUG)

PING = 1
PONG = 2
NODEID = 3
SUCCESSOR = 4
PREDECESSOR = 5
NOTIFY = 6
LOOKUP = 7
CPF = 8

class conn:
    def __init__(self, id, address):
        self.nodeID = id
        self.address = address

    def __eq__(self, other):
        return self.nodeID == other.nodeID \
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

        self.lsem = threading.Semaphore()
        self.lsock = self.context.socket(zmq.ROUTER)
        self.listen_address = f'tcp://{host}:{portin}'
        self.lsock.bind(self.listen_address)
        #self.lsock.setsockopt(zmq.RCVTIMEO, 5000)

        self.ssem = threading.Semaphore()
        self.ssock = self.context.socket(zmq.DEALER)
        self.ssock.bind(f'tcp://{host}:{portout}')
        recvtime = random.randint(1000, 5000)
        self.ssock.setsockopt(zmq.RCVTIMEO, recvtime)

        server = f'tcp://{self.dns[0]}:{self.dns[1]}'
        reply = self.ssocket_send((broker.JOIN_GROUP, self.role, self.listen_address), server)
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
            reply = self.ssocket_send((broker.RANDOM_NODE, self.role, exceptions), server)
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
        threading.Thread(target=self.manageConnections).start()
        threading.Thread(target=self.stabilize_daemon).start()


        #threading.Thread(target=self.successors_daemon).start()


    def lsocket_send(self, ident, msg):
        msg = pickle.dumps(msg)
        self.lsem.acquire()
        self.lsock.send_multipart([ident, msg])
        self.lsem.release()

    def lsocket_recv(self):
        self.lsem.acquire()
        try:
            ident, reply = self.lsock.recv_multipart()
            reply = pickle.loads(reply)
        except Exception as e:
            ident, reply = None, None
        self.lsem.release()

        return ident, reply

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
        for i in range(5):
            reply = self.ssocket_send((PING, None), node)
            if not reply is None and reply == PONG:
                return  True
            time.sleep(2)
        return False

    def _successor_(self, address):
        if address == self.listen_address:
            return self.successor
        return self.ssocket_send((SUCCESSOR, None), address)

    @property
    def conn(self):
        return conn(self.nodeID, self.listen_address)

    @property
    def successor(self):
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


    def manageConnections(self):
        while True:
            ident, data = self.lsocket_recv()
            if data is None: continue

            threading.Thread(target=self.manageRequest, args=(ident, data,)).start()
            #self.manageRequest(ident, data)

    def manageRequest(self, ident, data):
        code, *args = data
        if code == PING:
            #log.warning(f'received PING request')
            self.lsocket_send(ident, PONG)

        if code == NODEID:
            log.warning(f'received NODEID request')
            self.lsocket_send(ident, self.nodeID)

        if code == SUCCESSOR:
            log.warning(f'received SUCCESSOR request')
            self.lsocket_send(ident, self.successor)

        if code == PREDECESSOR:
            log.warning(f'received PREDECESSOR request')
            self.lsocket_send(ident, self.predecessor)

        if code == NOTIFY:
            conn = args[0]
            log.warning(f'received NOTIFY request for node {conn}')
            self.notify(conn)

        if code == LOOKUP:
            log.warning(f'received LOOKUP request')
            conn = self.lookup(args[0])
            self.lsocket_send(ident, conn)

        if code == CPF:
            log.warning(f'received CPF request')
            conn = self.closest_preceding_finger(args[0])
            self.lsocket_send(ident, conn)


    def join(self, node):
        while True:
            succ = self._lookup_(self.nodeID, node.address)
            self.successor = succ
            if not succ is None:
                log.warning(f'successor is {succ.nodeID}')
                break


    def _lookup_(self, id, address):
        if address == self.listen_address:
            return self.lookup(id)
        return self.ssocket_send((LOOKUP, id), address)

    def lookup(self, id):
        log.warning(f'trying to find successor of {id}')
        p = self.find_predecessor(id)
        ret = self._successor_(p.address)
        log.warning(f'node {self.nodeID}: successor of {id} is {ret}')
        return ret

    def find_predecessor(self, id):
        log.warning(f'node {self.nodeID}: trying to find predecessor of {id}')
        cur = self.conn
        while True:
            succ = self._successor_(cur.address)
            if succ is None or self.between(id, cur.nodeID + 1, succ.nodeID + 1):
                break
            cur = self._closest_preceding_finger_(id, cur.address)
        log.warning(f'node {self.nodeID}: predecessor of {id} is {cur}')
        return cur


    def _closest_preceding_finger_(self, id, address):
        if address == self.listen_address:
            return self.closest_preceding_finger(id)
        else:
            return self.ssocket_send((CPF, id), address)

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
            time.sleep(1)
            self.stabilize()
            self.fix_finger()
            self.fix_successors()

    def stabilize(self):
        log.warning('stabilizing')
        if self.successor is None: return
        x = self._predecessor_(self.successor.address)
        if not x is None and self.nodeID + 1 != self.successor.nodeID \
                and self.between(x.nodeID, self.nodeID + 1, self.successor.nodeID):
            log.warning(f'new successor is {x.nodeID}')
            self.successor = x
        self._notify_(self.conn, self.successor.address)


    def _notify_(self, node, address):
        if address == self.listen_address:
            self.notify(node)
        else:
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

    def fix_successors(self):
        if len(self.successors) == 0: return
        if len(self.successors) < self.NBits:
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
                log.warning(f'added {new} as successor')
                log.warning(f'Successors: {[i for i in self.successors]}')
        time.sleep(len(self.successors) + 1)





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
