import Pyro4
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

BUFERSIZE = 1024

class Node:
    def __init__(self, nameserver, role, port=5001):
        self.replication = 5

        self.nameserver = nameserver
        self.role = role

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        host = socket.gethostname()
        host = socket.gethostbyname(host)
        self.socket.bind(f'tcp://{host}:{port}')
        self.socket.setsockopt(zmq.RCVTIMEO, 5000)

    def ssocket_send(self, msg, address):
        msg = pickle.dumps(msg)
        self.socket.connect(address)
        self.socket.send(msg)
        try:
            reply = pickle.loads(self.socket.recv())
        except Exception as e:
            reply = None
        self.socket.disconnect(address)
        return reply

    def start(self, pyroname):
        self.pyroname = pyroname

        server = f'tcp://{nameserver[0]}:{nameserver[1]}'
        self.socket.connect(server)
        reply = self.ssocket_send((broker.JOIN_GROUP, self.role, self.pyroname), server)
        if reply is None:
            raise Exception('server not responding')
        self._nodeID = reply

        reply = self.ssocket_send((broker.NBits, None), server)
        if reply is None:
            raise Exception('server not responding')
        self.NBits = reply
        self.MAXNodes = (1 << self.NBits)
        self.FT = [self for _ in range(self.NBits + 1)]

        log.warning(f'node {self._nodeID} started')
        exceptions = [self._nodeID]
        while True:
            reply = self.ssocket_send((broker.RANDOM_NODE, self.role, exceptions), server)
            if reply is None:
                raise Exception('server not responding')
            code, id, name = reply
            if name is None: break
            node = Pyro4.Proxy(name)
            log.warning(f'joining node {id}')
            try:
                self.join(node)
                break
            except Exception:
                exceptions.append(id)
        self.socket.close()

        log.warning('starting daemons')
        threading.Thread(target=self.stabilize_daemon).start()

        self.successors = []



    @Pyro4.expose
    @property
    def successor(self):
        node = self.FT[1]
        try:
            id = node.nodeID
            return node
        except Exception:
            try:
                self.successors = self.successors[1:]
                self.FT[1] = self.successors[0]
            except Exception:
                self.FT[1] = self
            return self.successor

    @Pyro4.expose
    @successor.setter
    def successor(self, value):
        self.FT[1] = value

    @Pyro4.expose
    @property
    def predecessor(self):
        node = self.FT[0]
        try:
            id = node.nodeID
            return node
        except Exception:
            self.FT[0] = self.find_predecessor(self.nodeID)
        return self.predecessor

    @Pyro4.expose
    @predecessor.setter
    def predecessor(self, value):
        self.FT[0] = value

    @Pyro4.expose
    @property
    def nodeID(self):
        return self._nodeID

    def finger(self, i):
        node = self.FT[i]
        try:
            id = node.nodeID
            return node
        except Exception as e:
            if i == 1: self.FT[i] = self.successor
            else: self.FT[i] = self.find_successor((self.nodeID + (1 << (i - 1))) % self.MAXNodes)
            return self.finger(i)


    # Says if node id is in range [a,b)
    def between(self, id, a, b):
        #log.warning(f'node {self.nodeID}: asked if {a}<={id}<{b}')
        if a < b: return id >= a and id < b
        return id >= a or id < b

    def join(self, node):
        if node:
            log.warning(f'node {self.nodeID} joining node {node.nodeID}')
            self.init_finger_table(node)
            self.update_others()

    def init_finger_table(self, node):
        log.warning(f'node {self.nodeID} updating FT with node {node.nodeID}')
        self.successor = node.find_successor((self.nodeID + 1) % self.MAXNodes)
        self.predecessor = self.successor.predecessor
        self.successor.predecessor = self
        for i in range(1, self.NBits):
            if self.between(self.finger(i + 1).nodeID, self.nodeID, self.finger(i).nodeID):
                self.FT[i + 1] = self.finger(i)
            else:
                self.FT[i + 1] = node.find_successor((self.nodeID + (1 << i)) % self.MAXNodes)
        log.warning(f'FT[{self.nodeID}]={[i.nodeID for i in self.FT]}')


    def update_others(self):
        for i in range(1, self.NBits + 1):
            p = self.find_predecessor((self.nodeID - (1 << (i - 1)) + self.MAXNodes) % self.MAXNodes)
            if p.nodeID == self.nodeID: continue
            log.warning(f'sending update FT from {self.nodeID} to {p.nodeID} for index {i}')
            p.update_finger_table(self, i)

    @Pyro4.expose
    def update_finger_table(self, node, i):
        log.warning(f'received update FT from node {node.nodeID} for index {i}')
        if self.between(node.nodeID, self.nodeID, self.finger(i).nodeID):
            self.FT[i] = node
            log.warning(f'FT[{i}] is now {node.nodeID}')
            p = self.predecessor
            if p.nodeID in [self.nodeID, node.nodeID]:
                return
            log.warning(f'updating FT of predecessor {p.nodeID}')
            p.update_finger_table(node, i)
            log.warning(f'updated FT[{self.nodeID}]= {[i.nodeID for i in self.FT]}')

    @Pyro4.expose
    def find_successor(self, id):
        predecessor = self.find_predecessor(id)
        log.warning(f'node {self.nodeID}: successor of {id} is {predecessor.successor.nodeID}')
        return predecessor.successor

    def find_predecessor(self, id):
        log.warning(f'node {self.nodeID}: trying to find predecessor of {id}')
        cur = self
        while not self.between(id, cur.nodeID + 1, cur.successor.nodeID + 1):
            log.warning(f'node {self.nodeID}: searching closest finger for {id} in node {cur.nodeID}')
            cur = cur.closest_preceding_finger(id)
        log.warning(f'node {self.nodeID}: predecessor of {id} is {cur.nodeID}')
        return cur

    @Pyro4.expose
    def closest_preceding_finger(self, id):
        for i in range(self.NBits, 0, -1):
            if self.between(self.finger(i).nodeID, self.nodeID + 1, id):
                return self.finger(i)
        return self

    def stabilize_daemon(self):
        while True:
            time.sleep(3)
            self.stabilize()
            self.fix_fingers()

    def stabilize(self):
        x = self.successor.predecessor
        if self.between(x.nodeID, self.nodeID + 1, self.successor.nodeID):
            self.FT[1] = x
        self.successor.notify(self)

    @Pyro4.expose
    def notify(self, node):
        if self.predecessor is None or self.between(node.nodeID,
            self.predecessor.nodeID + 1, self.nodeID):
            self.FT[0] = node

    def fix_fingers(self):
        i = random.randint(1, self.NBits)
        self.FT[i] = self.find_successor((self.nodeID + (1 << (i - 1))) % self.MAXNodes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ns', '--nameserver', required=True, type=str, help='Name server address')
    parser.add_argument('--port1', default=5000, required=False, type=int, help='Pyro daemon port')
    parser.add_argument('--port2', default=5001, required=False, type=int, help='Port for outgoing communications')
    parser.add_argument('-r', '--role', default='chordNode', required=False, type=str, help='Node role')
    args = parser.parse_args()

    nameserver = args.nameserver
    role = args.role

    host, port = nameserver.split(':')
    port = int(port)
    nameserver = (host, port)

    port2 = args.port2

    node = Node(nameserver, role, port2)

    hostname = socket.gethostname()
    host = socket.gethostbyname(hostname)
    port = args.port1

    daemon = Pyro4.Daemon(host, port)
    uri = daemon.register(node)
    threading.Thread(target=daemon.requestLoop).start()
    node.start(uri)