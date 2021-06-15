from chord import Node
import threading
import random
import argparse
from constants import *
import logging
import pickle

log = logging.Logger(name='Cache')
logging.basicConfig(level=logging.DEBUG)

class capsule:
    def __init__(self, key, data):
        self.key = key
        self.data = data

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other.key


class CacheNode(Node):
    def __init__(self, dns, role, portin=5000, portout=5001):

        self.dsem = threading.Semaphore()
        self.data = {}
        super().__init__(dns, role, portin, portout)

    def manageRequest(self, ident, data):
        super().manageRequest(ident, data)

        code, *args = data
        if code == SAVE_URL:
            key, text = args
            cap = capsule(key, text)
            log.warning("Save url request for:" + cap.key)
            node = self.lookup(cap.__hash__() % self.MAXNodes)
            if node == self.conn:
                log.warning("Saving url: " + cap.key)
                self.dsem.acquire()
                self.data[cap.key] = cap
                print(self.data)
                self.dsem.release()
                self.lsocket_send(ident, "OK")
            else:
                log.warning("Sending SAVE_URL to node: " + node.address)
                self.ssocket_send((SAVE_URL, key, text), node.address, False)

        if code == GET_URL:
            key = args[0]
            log.warning("GET url request for:" + key)
            node = self.lookup(hash(key) % self.MAXNodes)
            if node == self.conn:
                log.warning("I'm in charge of the url: " + key)
                self.dsem.acquire()
                try:
                    text = self.data[key].data
                    if text is None:
                        text = 'Empty'
                    else:
                        log.warning("I have  the url: " + key)
                except KeyError:
                    log.warning("I don't have the url: " + key)
                    text = 'Empty'
                self.dsem.release()
            else:
                log.warning("Sending GET_URL to node: " + node.address)
                text = self.ssocket_send((GET_URL, key), node.address)
            #log.warning("SENDING URL" + text[0:4])
            self.lsocket_send(ident,text)

    def replicate_daemon(self):
        while True:
            p = self.predecessor
            if p is None: continue

            index = random.randint(0, len(self.successors) - 1)
            succ = self.successors[index]
            for key, cap in self.data.items():
                if self.between(key, p.nodeID + 1, self.nodeID + 1):
                    self.ssocket_send((SAVE_URL, cap.key, cap.data), succ.address, False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ns', '--nameserver', required=True, type=str, help='Name server address')
    parser.add_argument('--port1', default=5050, required=False, type=int, help='Port for incoming communications')
    parser.add_argument('--port2', default=5051, required=False, type=int, help='Port for outgoing communications')
    parser.add_argument('-r', '--role', default='chordNode', required=False, type=str, help='Node role')
    args = parser.parse_args()

    nameserver = args.nameserver
    role = args.role

    host, port = nameserver.split(':')
    port = int(port)
    nameserver = (host, port)

    port1 = args.port1
    port2 = args.port2

    node = CacheNode(nameserver, role, port1, port2)