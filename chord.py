import Pyro4, random, argparse, socket, threading, time

class node:
    def __init__(self, pyroname, servername='chordServer'):
        if servername is None: servername = 'chordServer'
        self.pyroname = pyroname
        self.servername = servername
        self.server = Pyro4.Proxy(f'PYRONAME:{servername}')

        self._nodeID = self.server.registerNode(pyroname)
        self.NBits = self.server.NBits
        self.MAXNodes = (1 << self.NBits)
        self.FT = [self for _ in range(self.NBits + 1)]

    def start(self):
        print(f'node {self.nodeID} started')
        initialNode = self.server.getNode(self.nodeID)
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
            print(f'node {self.nodeID} joining node {node.nodeID}')
            self.init_finger_table(node)
            self.update_others()

        threading.Thread(target=self.stabilize_daemon).start()

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--servername', required=False, type=str, help='Pyro server name')
    parser.add_argument('--pyroname', required=False, type=str, help='Pyro node name')
    args = parser.parse_args()

    servername, pyroname = None, None
    if args.servername:
        servername = args.servername
    if args.pyroname:
        pyroname = args.pyroname
    else:
        pyroname = 'name' + str(random.randint(1, 1000000000))

    node = node(pyroname, servername)

    hostname = socket.gethostname()
    host = socket.gethostbyname(hostname)

    daemon = Pyro4.Daemon()
    uri = daemon.register(node)
    ns = Pyro4.locateNS()
    ns.register(node.pyroname, uri)
    threading.Thread(target=daemon.requestLoop).start()

    node.start()