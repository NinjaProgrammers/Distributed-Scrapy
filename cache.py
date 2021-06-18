from chord import Node, REPLY, RESEND
import threading
import random
import argparse
from constants import *
from logFormatter import logger
import time


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
    def __init__(self, dns):
        self.dsem = threading.Semaphore()
        self.data = {}
        super().__init__(dns)

        threading.Thread(target=self.replicate_daemon).start()

    def hash_string(self, target: str):
        p = 101
        x = 0
        for c in target:
            x = (p * x + ord(c)) % self.MAXNodes
        return x

    def manage_request(self, data):
        response = super().manage_request(data)

        code, *args = data
        if code == SAVE_DATA:
            key, text = args
            hash = self.hash_string(key)
            cap = capsule(key, hash, text)
            logger.info("Received SAVE_URL request for:" + cap.key)
            node = self.lookup(cap.hash)
            if node == self.conn:
                logger.info(f'This is the responsible for key {cap.key}')
                self.save_data(cap)
                response = ACK
            else:
                logger.info("Sending SAVE_URL to node " + node.address)
                response = self.ssocket_send((SAVE_DATA, key, text), node, REPLY)


        if code == GET_DATA:
            key = args[0]
            hash = self.hash_string(key)
            logger.info("Received GET_URL request for " + key)

            while True:
                response = None
                node = self.lookup(hash)

                if node == self.conn:
                    logger.info("This is the responsible of the url " + key)
                    try:
                        response = self.data[key].data
                        logger.info("The url is saved")
                    except KeyError:
                        logger.info('The url is not saved')
                        response = 'Empty'
                    break
                else:
                    logger.info(f"Sending GET_URL for key={key} hash={hash} to node {node}")
                    response = self.ssocket_send((GET_DATA, key), node, REPLY)
                    if not response is None:
                        break

        if code == PULL:
            id = args[0]
            logger.debug(f'Received PULL request from node {id}')
            arr = [c for c in self.data.values() if self.between(c.hash, self.nodeID + 1, id + 1)]
            response = arr

        if code == PUSH:
            arr = args[0]
            logger.debug(f'Received PUSH request for {arr}')
            for c in arr:
                if not c.key in self.data.keys():
                    self.data[c.key] = c

        return response

    def save_data(self, cap):
        logger.debug("Saving url " + cap.key)
        self.dsem.acquire()
        self.data[cap.key] = cap
        self.dsem.release()

        def replicate(cap):
            for i, succ in enumerate(self.successors):
                if i < 5:
                    logger.debug(f'Replicating data to {succ.nodeID}')
                    self.ssocket_send((PUSH, [cap]), succ)

        threading.Thread(target=replicate, args=(cap,)).start()


    def join(self, node):
        response = super().join(node)
        if response: self.pull()
        return response

    def pull(self):
        reply = self.ssocket_send((PULL, self.nodeID), self.successor, REPLY)
        logger.debug(f'Pulled {reply} from {self.successor.nodeID}')
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
                    logger.debug(f'Pushing data to {succ}')
                    self.ssocket_send((PUSH, arr), succ)
            time.sleep((len(self.successors) + 1) * 5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ns', '--nameserver', required=True, type=str, help='Name server address')
    args = parser.parse_args()

    nameserver = args.nameserver

    host, port = nameserver.split(':')
    port = int(port)
    nameserver = (host, port)

    node = CacheNode(nameserver)