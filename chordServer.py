import random

class chordConn:
    def __init__(self, key, address, udp_address):
        self.key = key
        self.address = address
        self.udp_address = udp_address
        self.active = True

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
            return temp[0].key

        arr = [i.key for i in self.nodes]
        while True:
            id = random.randint(0, self.MAXNodes - 1)
            if not id in arr: break

        self.nodes.append(chordConn(id, address, udp_address))
        return id

    def addToGroup(self, id, address, udp_address):
        self.nodes.append(chordConn(id, address, udp_address))

    def getRandomNode(self, exc=None):
        if exc is None: exc = []
        arr = [i for i in self.nodes if not i.key in exc and i.active]
        if len(arr) == 0: return None
        r = random.choice(arr)
        return r

