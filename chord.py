import Pyro4, random, argparse, socket, threading, time, pickle, zmq, broker

BUFERSIZE = 1024

class Node:
    def __init__(self, nameserver, role, nbits=30):
        self.replication = 5

        self.nameserver = nameserver
        self.role = role

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.RCVTIMEO, 1000)

        self.NBits = nbits
        self.MAXNodes = (1 << self.NBits)
        self.FT = [self for _ in range(self.NBits + 1)]

        self.data = {}

    def set_pyro_name(self, pyroname):
        self.pyroname = pyroname

    def start(self):
        self.socket.connect(self.nameserver)
        self.socket.send(pickle.dumps((broker.JOIN_GROUP, self.role, self.pyroname)))
        try:
            reply = pickle.loads(self.socket.recv())
        except Exception as e:
            raise Exception('server not responding')
        self._nodeID = pickle.loads(reply)

        print(f'node {self.nodeID} started')

        self.socket.send(pickle.dumps((broker.RANDOM_NODE, self.role, self.nodeID)))
        try:
            reply = pickle.loads(self.socket.recv())
        except Exception as e:
            raise Exception('server not responding')
        initialNode = pickle.loads(reply)
        self.socket.close()
        self.join(initialNode)

    @Pyro4.expose
    @property
    def successor(self):
        return self.FT[1]

    @Pyro4.expose
    @successor.setter
    def successor(self, value):
        self.FT[1] = value

    @Pyro4.expose
    @property
    def predecessor(self):
        return self.FT[0]

    @Pyro4.expose
    @predecessor.setter
    def predecessor(self, value):
        self.FT[0] = value

    @Pyro4.expose
    @property
    def nodeID(self):
        return self._nodeID

    # Says if node id is in range [a,b)
    def between(self, id, a, b):
        #print(f'node {self.nodeID}: asked if {a}<={id}<{b}')
        if a < b: return id >= a and id < b
        return id >= a or id < b

    def join(self, node):
        if node:
            node = Pyro4.Proxy(node)
            print(f'node {self.nodeID} joining node {node.nodeID}')
            self.init_finger_table(node)
            self.update_others()

        threading.Thread(target=self.stabilize_daemon).start()
        threading.Thread(target=self.replicate_daemon).start()

    def init_finger_table(self, node):
        print(f'node {self.nodeID} updating FT with node {node.nodeID}')
        self.FT[1] = node.find_successor((self.nodeID + 1) % self.MAXNodes)
        self.FT[0] = self.successor.predecessor
        self.successor.predecessor = self
        for i in range(1, self.NBits):
            if self.between(self.FT[i + 1].nodeID, self.nodeID, self.FT[i].nodeID):
                self.FT[i + 1] = self.FT[i]
            else:
                self.FT[i + 1] = node.find_successor((self.nodeID + (1 << i)) % self.MAXNodes)
        print(f'FT[{self.nodeID}]=',[i.nodeID for i in self.FT])


    def update_others(self):
        for i in range(1, self.NBits + 1):
            p = self.find_predecessor((self.nodeID - (1 << (i - 1)) + self.MAXNodes) % self.MAXNodes)
            if p.nodeID == self.nodeID: continue
            print(f'sending update FT from {self.nodeID} to {p.nodeID} for index {i}')
            p.update_finger_table(self, i)

    @Pyro4.expose
    def update_finger_table(self, node, i):
        print(f'received update FT from node {node.nodeID} for index {i}')
        if self.between(node.nodeID, self.nodeID, self.FT[i].nodeID):
            self.FT[i] = node
            print(f'FT[{i}] is now {node.nodeID}')
            p = self.predecessor
            if p.nodeID in [self.nodeID, node.nodeID]: return
            p.update_finger_table(node, i)
            print(f'updated FT[{self.nodeID}]=', [i.nodeID for i in self.FT])

    @Pyro4.expose
    def find_successor(self, id):
        predecessor = self.find_predecessor(id)
        print(f'node {self.nodeID}: successor of {id} is {predecessor.successor.nodeID}')
        return predecessor.successor

    def find_predecessor(self, id):
        print(f'node {self.nodeID}: trying to find predecessor of {id}')
        cur = self
        while not self.between(id, cur.nodeID + 1, cur.successor.nodeID + 1):
            print(f'node {self.nodeID}: searching closest finger for {id} in node {cur.nodeID}')
            cur = cur.closest_preceding_finger(id)
        print(f'node {self.nodeID}: predecessor of {id} is {cur.nodeID}')
        return cur

    @Pyro4.expose
    def closest_preceding_finger(self, id):
        for i in range(self.NBits, 0, -1):
            if self.between(self.FT[i].nodeID, self.nodeID + 1, id):
                return self.FT[i]
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

    def save_data(self, key, value, replicated=False):
        if not key in self.data.keys():
            self.data[key] = (value, replicated)

    def replicate_daemon(self):
        while True:
            cur = self.successor
            for i in range(self.replication):
                for j in self.data:
                    if not j[1][1]:
                        cur.save_data(j[0], j[1][0], True)
                cur = cur.successor
            time.sleep(len(self.data) / 10)

    def get_data(self, key):
        if not key in self.data.keys():
            return None
        return self.data[key]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ns', '--nameserver', required=True, type=str, help='Name server address')
    parser.add_argument('-p', '--port', default=9999, required=False, type=int, help='Node port')
    parser.add_argument('-r', '--role', default='chordNode', required=False, type=str, help='Node role')
    args = parser.parse_args()

    nameserver = args.nameserver
    role = args.role

    host, port = nameserver.split(':')
    port = int(port)
    nameserver = (host, port)

    node = Node(nameserver, role)

    hostname = socket.gethostname()
    host = socket.gethostbyname(hostname)
    port = args.port

    daemon = Pyro4.Daemon(host, port)
    uri = daemon.register(node)
    node.set_pyro_name(uri)
    threading.Thread(target=daemon.requestLoop).start()
    node.start()