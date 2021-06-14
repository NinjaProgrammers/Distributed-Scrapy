import chord
import threading
import random

class capsule:
    def __init__(self, key, data):
        self.key = key
        self.data = data

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other.key


SAVE_URL = 101
GET_URL = 102

class node(chord.Node):
    def __init__(self, dns, role, portin=5000, portout=5001):
        super().__init__(dns, role, portin, portout)

        self.dsem = threading.Semaphore()
        self.data = {}

    def manageRequest(self, ident, data):
        super().manageRequest(ident, data)

        code, *args = data
        if code == SAVE_URL:
            key, text = args
            cap = capsule(key, text)
            node = self.lookup(cap.__hash__() % self.MAXNodes)
            if node == self.conn:
                self.dsem.acquire()
                self.data[cap.key] = cap
                self.dsem.release()
            else:
                self.ssocket_send((SAVE_URL, key, text), node.address, False)

        if code == GET_URL:
            key = args[0]
            node = self.lookup(hash(key) % self.MAXNodes)
            if node == self.conn:
                self.dsem.acquire()
                text = self.data[key].data
                self.dsem.release()
            else:
                text = self.ssocket_send((GET_URL, None), node.address)
            self.lsocket_send(ident, text)

    def replicate_daemon(self):
        while True:
            p = self.predecessor
            if p is None: continue

            index = random.randint(0, len(self.successors) - 1)
            succ = self.successors[index]
            for key, cap in self.data.items():
                if self.between(key, p.nodeID + 1, self.nodeID + 1):
                    self.ssocket_send((SAVE_URL, cap.key, cap.data), succ.address, False)

