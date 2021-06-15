from chord import Node
import threading
import random
import argparse
from constants import *
import logging
import pickle
import hashlib
import time

log = logging.Logger(name='Cache')
logging.basicConfig(level=logging.DEBUG)

class capsule:
    def __init__(self, key, hash, data):
        self.key = key
        self.data = data
        self.hash = hash

    def __hash__(self):
        return self.hash

    def __eq__(self, other):
        return self.key == other.key

    def __str__(self):
        return str(self.key)

    def __repr__(self):
        return self.__str__()


class CacheNode(Node):
    def __init__(self, dns, role, portin=5000, portout=5001):
        self.dsem = threading.Semaphore()
        self.data = {}
        super().__init__(dns, role, portin, portout)
        threading.Thread(target=self.replicate_daemon).start()

    def hash_string(self, target: str):
        log.warning(f'hashing string {target}')
        p = 61
        x = 0
        for c in target:
            x = (p * x + ord(c)) % self.MAXNodes
        return x

    def manage_request(self, data):
        response = super().manage_request(data)

        code, *args = data
        if code == SAVE_URL:
            key, text = args
            hash = self.hash_string(key)
            cap = capsule(key, hash, text)
            log.warning("Save url request for:" + cap.key)
            node = self.lookup(cap.hash)
            if node == self.conn:
                log.warning("Saving url: " + cap.key)
                self.dsem.acquire()
                self.data[cap.key] = cap
                print(self.data)
                self.dsem.release()
                response = "OK"
            else:
                log.warning("Sending SAVE_URL to node: " + node.address)
                self.ssocket_send((SAVE_URL, key, text), node.address, False)

        if code == GET_URL:
            key = args[0]
            hash = self.hash_string(key)
            log.warning("GET url request for:" + key)

            node = self.lookup(hash)
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
                log.warning(f"Sending GET_URL {key}   H: {hash} to node: {node.address}")
                text = self.ssocket_send((GET_URL, key), node.address)
            #log.warning("SENDING URL" + text[0:4])
            response = text

        if code == PULL:
            id = args[0]
            log.warning(f'received PULL request fron node {id}')
            arr = [c for c in self.data.values() if self.between(c.hash, self.nodeID + 1, id + 1)]
            response = arr

        if code == PUSH:
            arr = args[0]
            log.warning(f'received PUSH request for {arr}')
            for c in arr:
                if not c.key in self.data.keys():
                    self.data[c.key] = c

        return response

    def join(self, node):
        super().join(node)
        self.pull()

    def pull(self):
        reply = self.ssocket_send((PULL, self.nodeID), self.successor.address)
        log.warning(f'pulled {reply}')
        if reply is None: return
        for c in reply:
            if not c.key in self.data.keys():
                self.data[c.key] = c

    def replicate_daemon(self):
        while True:
            p = self.predecessor
            if not p is None and len(self.successors) != 0:
                succ = random.choice(self.successors)
                arr = [c for c in self.data.values() if self.between(c.hash, p.nodeID + 1, self.nodeID + 1)]
                if len(arr) > 0:
                    log.warning(f'pushing to {succ}')
                    self.ssocket_send((PUSH, arr), succ.address, False)
            time.sleep((len(self.successors) + 1) * 5)


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