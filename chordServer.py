import random
from conn import conn

class node:
    def __init__(self, nbits=30):
        if nbits is None: nbits = 5
        self.NBits = nbits
        self.MAXNodes = 1 << self.NBits
        self.nodes = []

    def registerNode(self, address, udp_address):
        temp = [i for i in self.nodes if i.address == address
                              and i.udp_address == udp_address]
        if len(temp) != 0:
            temp[0].active = True
            return temp[0].nodeID

        arr = [i.nodeID for i in self.nodes]
        while True:
            id = random.randint(0, self.MAXNodes - 1)
            if not id in arr: break

        self.nodes.append(conn(id, address, udp_address))
        return id

    def addToGroup(self, id, address, udp_address):
        self.nodes.append(conn(id, address, udp_address))

    def getRandomNode(self, exc=None):
        if exc is None: exc = []
        arr = [i for i in self.nodes if not i.nodeID in exc and i.active]
        if len(arr) == 0: return None
        r = random.choice(arr)
        return r

