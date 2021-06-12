import Pyro4, random, argparse, socket

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

    def getRandomNode(self, role, exc=-1):
        arr = [i.key for i in self.nodes if i.key != exc and i.role == role]
        if len(arr) == 0: return None
        r = random.choice(arr)
        return r.proxy

