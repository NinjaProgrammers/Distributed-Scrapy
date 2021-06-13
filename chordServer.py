import random

class chordConn:
    def __init__(self, role, key, proxy):
        self.key = key
        self.proxy = proxy
        self.role = role

class Node:
    def __init__(self, nbits=30):
        if nbits is None: nbits = 5
        self.NBits = nbits
        self.MAXNodes = 1 << self.NBits
        self.nodes = []

    def registerNode(self, role, pyroname):
        arr = [i.key for i in self.nodes]
        while True:
            id = random.randint(0, self.MAXNodes - 1)
            if not id in arr: break

        self.nodes.append(chordConn(role, id, pyroname))
        return id

    def addToGroup(self, id, role, pyroname):
        self.nodes.append(chordConn(role, id, pyroname))

    def getRandomNode(self, role, exc=None):
        if exc is None: exc = []
        arr = [i for i in self.nodes if not i.key in exc and i.role == role]
        if len(arr) == 0: return None
        r = random.choice(arr)
        return r

