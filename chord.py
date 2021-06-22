import random
import argparse
import socket
import threading
import time
import pickle
import zmq
from logFormatter import logger
from constants import *
from conn import conn

THREADS = 30
REPLY = 1
RESEND = 2

class Node:
    def __init__(self, dns):
        self.dns = dns
        host = socket.gethostname()
        host = socket.gethostbyname(host)

        # Opening zmq sockets
        self.context = zmq.Context()
        self.lsock = self.context.socket(zmq.ROUTER)
        port = self.lsock.bind_to_random_port(f'tcp://{host}')
        self.listen_address = f'tcp://{host}:{port}'
        logger.info(f'Listening TCP requests at {host}:{port}')

        # Opening zmq inproc sockets
        self.worker_address = f'inproc://workers{port}'
        self.wsock = self.context.socket(zmq.DEALER)
        self.wsock.bind(self.worker_address)
        for i in range(THREADS):
            threading.Thread(target=self.worker, args=(self.worker_address,), daemon=True).start()

        # Opening UDP sockets for PING PONG messages
        self.sping = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sping.settimeout(1)
        self.spong = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.spong.bind((host, 0))
        self.udp_address = self.spong.getsockname()
        logger.info(f'Listening UDP requests at {self.udp_address}')

        # Connecting to server
        server = f'tcp://{self.dns[0]}:{self.dns[1]}'
        udp_server = (self.dns[0], 5555)
        server_node = conn('unknown', server, udp_server)
        logger.info(f'Joining to server at {server}')
        reply = self.ssocket_send((JOIN_GROUP, self.listen_address, self.udp_address), server_node, REPLY)
        if reply is None:
            raise Exception('Server not responding')
        self.nodeID, self.NBits = reply
        self.MAXNodes = (1 << self.NBits)
        self.FTsem = threading.Semaphore()
        self.FT = [self.conn for _ in range(self.NBits + 1)]
        self.predecessor = conn()
        logger.info(f'Node started with ID {self.nodeID}')

        # Initializing successors list, neccessary for keeping nodes stability
        self.succsem = threading.Semaphore()
        self.successors = []

        # Asking for initial existing chord node to connect to
        exceptions = [self.nodeID]
        while True:
            logger.debug(f'Getting random chord node to connect to')
            reply = self.ssocket_send((RANDOM_NODE, exceptions), server_node, REPLY)
            if reply is None:
                raise Exception('server not responding')
            id, address, udp_address = reply
            if address is None:
                logger.info('There is no other nodes, starting alone')
                break

            node = conn(id, address, udp_address)
            if self.join(node):
                break
            else:
                exceptions.append(id)

        # Starting demons
        logger.debug('Starting daemons')
        threading.Thread(target=self.stabilize_daemon, name='ThreadStabilize', daemon=True).start()
        threading.Thread(target=zmq.device, args=(zmq.QUEUE, self.lsock, self.wsock,), name='ThreadDevice', daemon=True).start()
        threading.Thread(target=self.pong, name='PONG', daemon=True).start()
        threading.Thread(target=self.successors_daemon, name='ThreadSuccessors', daemon=True).start()

    def ssocket_send(self, msg, node: conn, flags: int =0):
        '''
        Sends a message to node.address through TCP zmq DEALER sockets,
        uses REPLY and RESEND flags to know if wait for answer and if resend
        the message several times while waiting for it respectivily
        :param msg: Message to send
        :param node: Object that stores the fields address and udp_address of the listening node
        :param flags: Possible flags are REPLY = 1 and RESEND = 2
        :return: Listening node answer if REPLY flag was activated and node is running,
        None in any other case
        '''
        logger.debug(f'Sending message to {node}')
        msg = pickle.dumps(msg)
        sock = self.context.socket(zmq.DEALER)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.connect(node.address)
        sock.send(msg)
        reply = None
        retransmits = 0
        while (flags & REPLY) > 0 and retransmits < 10:
            retransmits += 1
            try:
                if (flags & RESEND) > 0:
                    sock.send(msg)
                reply = pickle.loads(sock.recv())
            except Exception as e:
                pass

            if reply is None and self.ping(node.udp_address):
                time.sleep(1)
                continue
            else:
                break
        if (flags & REPLY) > 0 and reply is None:
            logger.warning(f'Didn\'t receive response from node {node}')
        sock.disconnect(node.address)
        return reply

    def join(self, node: conn) -> bool:
        '''
        Try to join to node.address using node.udp_address to send PING messages
        to know if node is running
        :param node: Object that stores the fields address and udp_address of the listening node
        :return: True or False if join was successful
        '''
        logger.info(f'Joining node {node}')
        succ = self.ssocket_send((LOOKUP, (self.nodeID + 1) %  self.MAXNodes), node, REPLY)
        if succ is None:
            logger.warning('Joining was not successful')
            return False
        self.successor = succ
        self._notify_(self.conn, succ)
        logger.info(f'Node\'s successor is {succ.nodeID}')
        return True

    def pong(self) -> None:
        '''
        Daemon for listening PING request via a UDP socket
        '''
        while True:
            msg, addr = self.spong.recvfrom(1024)
            if not msg: continue
            msg = pickle.loads(msg)
            if msg == PING:
                logger.debug(f'received PING from {addr}, sending PONG response')
                reply = pickle.dumps(PONG)
                self.spong.sendto(reply, addr)

    def ping(self, address: tuple) -> bool:
        '''
        Send a PING message to an address, return True or False
        if PONG response was received
        :param address: Address to PING
        :return: True or False if PONG response was received
        '''
        logger.debug(f'Sending PING to {address}')
        if address == self.udp_address: return True
        msg = pickle.dumps(PING)
        for i in range(3):
            self.sping.sendto(msg, address)
            try:
                reply, addr = self.sping.recvfrom(1024)
            except Exception:
                continue
            reply = pickle.loads(reply)
            if reply == PONG and addr == address:
                logger.warning('Received PONG response')
                return True
        logger.warning(f'Not received PONG response')
        return False

    def worker(self, worker_address: str) -> None:
        '''
        Daemon for managing incoming requests
        :param worker_address: Inproc address to connect to
        '''
        sock = self.context.socket(zmq.ROUTER)
        sock.connect(worker_address)

        while True:
            try:
                ident1, ident2, data = sock.recv_multipart()
                data = pickle.loads(data)
            except Exception as e:
                continue

            response = self.manage_request(data)
            bits = pickle.dumps(response)
            sock.send_multipart([ident1, ident2, bits])

    def manage_request(self, data: tuple):
        '''
        Manage the incoming request
        :param data: Request to process
        :return: Request's answer
        '''
        code, *args = data
        response = None

        if code == SUCCESSOR:
            response = self.successor
            logger.debug(f'Received SUCCESSOR request, returned {response}')

        if code == PREDECESSOR:
            response = self.predecessor
            logger.debug(f'Received PREDECESSOR request, returned {response}')

        if code == NOTIFY:
            node = args[0]
            logger.debug(f'Received NOTIFY request from node {node}')
            self.notify(node)

        if code == LOOKUP:
            response = self.lookup(args[0])
            logger.debug(f'Received LOOKUP request for key {args[0]}, returned {response}')

        if code == FIND_PREDECESSOR:
            response = self.find_predecessor(args[0])
            logger.debug(f'Received FIND_PREDECESSOR request for key {args[0]}, returned {response}')

        return response


    @property
    def conn(self) -> conn:
        '''
        conn object of the current node
        '''
        return conn(self.nodeID, self.listen_address, self.udp_address)

    def _successor_(self, node: conn) -> conn:
        '''
        Asks for successor of other chord node
        :param node: Other node conn object
        :return: Successor's conn object
        '''
        if node == self.conn:
            return self.successor
        return self.ssocket_send((SUCCESSOR, None), node, REPLY)


    @property
    def successor(self) -> conn:
        '''
        Node's successor
        '''
        return self.FT[1]

    @successor.setter
    def successor(self, value):
        self.FTsem.acquire()
        self.FT[1] = value
        self.FTsem.release()
        self.succsem.acquire()
        self.successors = [value]
        self.succsem.release()

    def _predecessor_(self, node: conn) -> conn:
        '''
        Asks for predecessor of other chord node
        :param node: Other node conn object
        :return: Predecessor's conn object
        '''
        if node == self.conn:
            return self.predecessor
        return self.ssocket_send((PREDECESSOR, None), node, REPLY)

    @property
    def predecessor(self) -> conn:
        '''
        Node's Predecessor
        '''
        return self.FT[0]

    @predecessor.setter
    def predecessor(self, value):
        self.FTsem.acquire()
        self.FT[0] = value
        self.FTsem.release()

    def start(self, i: int) -> int:
        '''
        Compute id of (i-1)-th finger
        :param i: Finger's index
        :return: (nodeID + 2**(i-1))
        '''
        return (self.nodeID + (1<<(i - 1))) % self.MAXNodes

    def finger(self, i) -> conn:
        '''
        conn object of node's (i-1)-th finger
        :param i: Finger's index
        :return: conn object
        '''
        return self.FT[i]

    def between(self, id, a, b) -> bool:
        '''
        Says if id is between a and b in current chord ring
        :param id: int
        :param a: int
        :param b: int
        :return: bool
        '''
        if a < b: return id >= a and id < b
        return id >= a or id < b


    def lookup(self, id: int) -> conn:
        '''
        Search for node responsible for key id
        :param id: int
        :return: conn
        '''
        p = self.find_predecessor(id)
        if p is None: return self.conn
        ret = self._successor_(p)
        return ret

    def _find_predecessor_(self, id: int, node: conn) -> conn:
        '''
        Asks remote chord node to find predecessor of key id
        :param id: int
        :param node: conn
        :return: conn
        '''
        logger.debug(f'Asking {node} for predecessor of {id}')
        return self.ssocket_send((FIND_PREDECESSOR, id), node, REPLY)

    def find_predecessor(self, id: int) -> conn:
        '''
        Finds predecessor of key id
        :param id: int
        :return: conn
        '''
        succ = self.successor
        if succ is None or self.between(id, self.nodeID + 1, succ.nodeID + 1):
            return self.conn
        cur = self.closest_preceding_finger(id)
        if cur == self.conn:
            return cur
        return self._find_predecessor_(id, cur)

    def closest_preceding_finger(self, id: int) -> conn:
        '''
        Return closest preceding finger of key id in node's Finger Table
        :param id: int
        :return: conn
        '''
        for i in range(self.NBits, 0, -1):
            f = self.finger(i)
            if f is None: continue
            if self.between(f.nodeID, self.nodeID + 1, id):
                return f
        return self.conn

    def _notify_(self, node: conn, conn: conn):
        '''
        Notifies remote node that current node may be his predecessor
        :param node: Current node conn object
        :param conn: Remote node conn object
        '''
        logger.debug(f'Sending NOTIFY to {conn}')
        self.ssocket_send((NOTIFY, node), conn)

    def notify(self, node: conn):
        '''
        Checks if node can be the predecessor of current node
        :param node: Remote node conn object
        '''
        p = self.predecessor
        if p is None or not p.is_valid or not self.ping(p.udp_address) or self.between(node.nodeID,
            self.predecessor.nodeID + 1, self.nodeID):
            self.predecessor = node

    def stabilize_daemon(self):
        '''
        Daemon for fixing node's successor and node's fingers
        '''
        while True:
            time.sleep((len(self.successors) + 1) * 3)
            logger.debug(f'Stabilizing')
            self.stabilize()
            logger.debug(f'Fixing finger')
            self.fix_finger()
            logger.info(f'FT[{self.nodeID}]={[i for i in self.FT]}')

    def stabilize(self):
        '''
        Fix node's successor
        '''
        succ = self.successor
        p = self._predecessor_(succ)
        if not p is None:
            if p == self.conn:
                return
            if not p.is_valid and succ != self.conn:
                self._notify_(self.conn, succ)
            elif p.is_valid and (self.conn == succ or self.between(p.nodeID, self.nodeID, succ.nodeID)):
                self.successor = p
                self._notify_(self.conn, p)

    def fix_finger(self):
        '''
        Fix random node's finger
        '''
        i = random.randint(2, self.NBits)
        self.FTsem.acquire()
        self.FT[i] = self.lookup(self.start(i))
        self.FTsem.release()

    def successors_daemon(self):
        '''
        Daemon to fix and maintain successors list, useful for updating
        current node's successor when it not responding PING requests
        '''
        while True:
            logger.debug('Fixing successors')
            self.fix_successors()
            time.sleep(2)

    def fix_successors(self):
        '''
        Maintains successors list up to date
        '''
        if not self.ping(self.successor.udp_address):
            logger.warning(f'Successor is unreachable')
            if len(self.successors) > 0:
                self.succsem.acquire()
                self.successors.pop(0)
                self.succsem.release()

            self.FTsem.acquire()
            if len(self.successors) > 0:
                self.FT[1] = self.successors[0]
            else:
                self.FT[1] = self.conn
                self.FT[0] = conn()
            self.FTsem.release()

            logger.info(f'New successor is {self.successor}')
            return

        if len(self.successors) > 0 and len(self.successors) < self.NBits:
            logger.debug('Adding successor')
            node = self.successors[-1]
            if node is None or not self.ping(node.udp_address):
                self.succsem.acquire()
                self.successors.pop()
                self.succsem.release()
                return
            new = self._successor_(node)
            if not new is None and not new in self.successors and new != self.conn:
                self.succsem.acquire()
                self.successors.append(new)
                self.succsem.release()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ns', '--nameserver', required=True, type=str, help='Name server address')
    args = parser.parse_args()

    nameserver = args.nameserver

    host, port = nameserver.split(':')
    port = int(port)
    nameserver = (host, port)

    node = Node(nameserver)
