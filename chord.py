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
UPDATE_FT = 7
LOOKUP = 8
CPF = 9

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

        self.lsock = self.context.socket(zmq.ROUTER)
        self.listen_address = f'tcp://{host}:{portin}'
        self.lsock.bind(self.listen_address)
        #self.lsock.setsockopt(zmq.RCVTIMEO, 5000)

        self.sem = threading.Semaphore()
        self.ssock = self.context.socket(zmq.DEALER)
        self.ssock.bind(f'tcp://{host}:{portout}')
        self.ssock.setsockopt(zmq.RCVTIMEO, 5000)

        server = f'tcp://{self.dns[0]}:{self.dns[1]}'
        reply = self.ssocket_send((broker.JOIN_GROUP, self.role, self.listen_address), server)
        if reply is None:
            raise Exception('server not responding')
        self.nodeID, self.NBits = reply
        self.MAXNodes = (1 << self.NBits)
        self.FT = [conn(self.nodeID, self.listen_address) for _ in range(self.NBits + 1)]
        self.predecessor = None

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

        self.successors = []
        #threading.Thread(target=self.successors_daemon).start()


    def lsocket_send(self, ident, msg):
        msg = pickle.dumps(msg)
        self.lsock.send_multipart([ident, msg])

    def lsocket_recv(self):
        try:
            ident, reply = self.lsock.recv_multipart()
            reply = pickle.loads(reply)
        except Exception as e:
            ident, reply = None, None

        return ident, reply

    def ssocket_send(self, msg, address):
        msg = pickle.dumps(msg)
        self.sem.acquire()
        self.ssock.connect(address)
        self.ssock.send(msg)
        try:
            reply = pickle.loads(self.ssock.recv())
        except Exception as e:
            reply = None
        self.ssock.disconnect(address)
        self.sem.release()
        return reply

    def ping(self, node):
        if node == self.listen_address: return True
        reply = self.ssocket_send((PING, None), node)
        return not reply is None and reply == PONG

    def _successor_(self, address):
        if address == self.listen_address:
            return self.successor
        return self.ssocket_send((SUCCESSOR, None), address)

    @property
    def successor(self):
        node = self.FT[1]
        if node is None or not self.ping(node.address):
            try:
                self.successors = self.successors[1:]
                self.FT[1] = self.successors[0]
            except Exception as e:
                self.FT[1] = conn(self.nodeID, self.listen_address)
        return self.FT[1]

    @successor.setter
    def successor(self, value):
        self.FT[1] = value
        self.successors = [value]

    def _predecessor_(self, address):
        if address == self.listen_address:
            return self.predecessor
        return self.ssocket_send((PREDECESSOR, None), address)

    @property
    def predecessor(self):
        return self.FT[0]

    @predecessor.setter
    def predecessor(self, value):
        self.FT[0] = value

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
            self.manageRequest(ident, data)

    def manageRequest(self, ident, data):
        code, *args = data
        if code == PING:
            log.warning(f'received PING request')
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

        if code == UPDATE_FT:
            log.warning(f'received UPDATE_FT request')
            conn, i = args
            self.update_finger_table(conn, i)

        if code == LOOKUP:
            log.warning(f'received LOOKUP request')
            conn = self.lookup(args[0])
            self.lsocket_send(ident, conn)

        if code == CPF:
            log.warning(f'received CPF request')
            conn = self.closest_preceding_finger(args[0])
            self.lsocket_send(ident, conn)


    def join(self, node):
        self.successor = self._lookup_(self.nodeID, node.address)

        '''
        if self.ping(node.address):
            log.warning(f'node {self.nodeID} joining node {node.nodeID}')
            self.init_finger_table(node)
            self.update_others()
        else:
            self.FT = [conn(self.nodeID, self.listen_address) for _ in range(self.NBits + 1)]
        '''

    '''
    def init_finger_table(self, node):
        log.warning(f'node {self.nodeID} updating FT with node {node}')
        self.successor = self._lookup_((self.nodeID + 1) % self.MAXNodes, node.address)
        self.predecessor = self._predecessor_(self.successor.address)
        self._notify_(conn(self.nodeID, self.listen_address), self.successor.address)
        for i in range(1, self.NBits):
            if self.between(self.start(i + 1), self.nodeID, self.finger(i).nodeID):
                self.FT[i + 1] = self.FT[i]
            else:
                self.FT[i + 1] = self._lookup_(self.start(i + 1), node.address)
        log.warning(f'FT[{self.nodeID}]={[i for i in self.FT]}')


    def update_others(self):
        for i in range(1, self.NBits + 1):
            p = self.find_predecessor((self.nodeID - (1 << (i - 1)) + self.MAXNodes) % self.MAXNodes)
            if p is None or p.nodeID == self.nodeID: continue
            log.warning(f'sending update FT from {self.nodeID} to {p} for index {i}')
            self._update_finger_table_(conn(self.nodeID, self.listen_address), i, p.address)

    def _update_finger_table_(self, node, i, address):
        if address == self.listen_address:
            self.update_finger_table(node, i)
        else:
            self.ssocket_send((UPDATE_FT, node, i), address)

    def update_finger_table(self, node, i):
        log.warning(f'received update FT from node {node} for index {i}')
        if self.between(node.nodeID, self.nodeID, self.finger(i).nodeID):
            self.FT[i] = node
            log.warning(f'FT[{i}] is now {node}')
            p = self.predecessor
            if p is None or p.nodeID in [self.nodeID, node.nodeID]:
                return
            log.warning(f'updating FT of predecessor {p}')
            self._update_finger_table_(conn(node.nodeID, node.listen_address), i, p.address)
            log.warning(f'updated FT[{self.nodeID}]= {[i for i in self.FT]}')
    '''

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
        cur = conn(self.nodeID, self.listen_address)
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
        return conn(self.nodeID, self.listen_address)


    def stabilize_daemon(self):
        while True:
            log.warning(f'FT[{self.nodeID}]={[i for i in self.FT]}')
            time.sleep(1)
            self.stabilize()
            self.fix_finger()

    def stabilize(self):
        log.warning('stabilizing')
        if self.successor is None: return
        x = self._predecessor_(self.successor.address)
        if not x is None and self.nodeID + 1 != self.successor.nodeID \
                and self.between(x.nodeID, self.nodeID + 1, self.successor.nodeID):
            log.warning(f'new successor is {x.nodeID}')
            self.successor = x
        self._notify_(conn(self.nodeID, self.listen_address), self.successor.address)


    def _notify_(self, node, address):
        if address == self.listen_address:
            self.notify(node)
        else:
            log.warning(f'sending NOTIFY to {address}')
            self.ssocket_send((NOTIFY, node), address)

    def notify(self, node):
        p = self.predecessor
        if p is None or not self.ping(p.address) or self.between(node.nodeID,
            self.predecessor.nodeID + 1, self.nodeID):
            self.predecessor = node

    def fix_finger(self):
        i = random.randint(2, self.NBits)
        self.FT[i] = self.lookup(self.start(i))

    def successors_daemon(self):
        while True:
            if len(self.successors) == 0: continue
            if len(self.successors) < self.NBits:
                node = self.successors[-1]
                try:
                    new = self._successor_(node.address)
                    self.successors.append(new)
                    log.warning(f'added {new} as successor')
                except Exception:
                    self.successors.pop()
                    log.warning('deleted successor')
                    continue
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
