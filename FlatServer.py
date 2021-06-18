import socket
import pickle
import threading
import time
import zmq
import random
from constants import *
from conn import conn
from logFormatter import logger

WAIT_TIMEOUT = 5
MESSAGE_TIMEOUT = 5000

TRIES = 3
RETRANSMITS = 5
THREADS = 5

REPLY = 1
RESEND = 2

class Node:

    def __init__(self, portin=5000, serveraddress=None):
        hostname = socket.gethostname()
        self.host = socket.gethostbyname(hostname)

        # Starting zmq sockets
        self.context = zmq.Context()
        self.lsock = self.context.socket(zmq.ROUTER)
        self.listen_address = f'tcp://{self.host}:{portin}'
        self.lsock.bind(self.listen_address)
        logger.info(f'Receiving incoming TCP communications at {self.listen_address}')

        # Starting zmq inproc sockets
        self.worker_address = f'inproc://workers{portin}'
        self.wsock = self.context.socket(zmq.DEALER)
        self.wsock.bind(self.worker_address)
        for i in range(THREADS):
            threading.Thread(target=self.worker, args=(self.worker_address,), name=f'ThreadWorker-{i}').start()

        # Nodes Info
        self.leaderID = -1
        self.nodeID = -1
        self.connections = []

        # Starting UDP sockets for PING PONG messages
        self.sping = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sping.settimeout(1)
        self.spong = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.spong.bind((self.host, 5555))
        self.udp_address = self.spong.getsockname()
        logger.info(f"Receiving incoming UDP communications at {self.udp_address}")

        # Trying to connect to another flat server node
        if serveraddress:
            address = f'tcp://{serveraddress[0]}:{serveraddress[1]}'
            self.join(address, (serveraddress[0], 5555))
        else:
            self.leaderID = self.nodeID = 0
            self.connections = [conn(self.nodeID, self.listen_address, self.udp_address)]

        self.electionID = -1
        self.new_node_queue = []

        threading.Thread(target=self.pong, name='ThreadPONG').start()
        threading.Thread(target=self.pingingDaemon, name='ThreadPing').start()
        threading.Thread(target=zmq.device, args=(zmq.QUEUE, self.lsock, self.wsock,), name='ThreadDevice').start()


    @property
    def leader(self):
        '''
        Leader conn object
        '''
        return self.getConnectionByID(self.leaderID)

    def getConnectionByAddress(self, address: str) -> conn:
        '''
        Get the conn object that corresponds to certain address
        :param address: Address of the node
        :return: conn oject
        '''
        arr = [i for i in self.connections if i.address == address]
        if len(arr) == 0: return None
        return arr[0]

    def getConnectionByID(self, ID: int) -> conn:
        '''
        Get the conn object that corresponds to certain id
        :param ID: Id of the node
        :return: conn oject
        '''
        arr = [i for i in self.connections if i.nodeID == ID]
        if len(arr) == 0: return None
        return arr[0]

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
        logger.debug(f'[{self.nodeID}]: Sending message to {node.address}')
        msg = pickle.dumps(msg)
        sock = self.context.socket(zmq.DEALER)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.connect(node.address)
        sock.send(msg)
        reply = None
        while (flags & REPLY) > 0:
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
            logger.warning(f'[{self.nodeID}]: Didn\'t receive response from node {node.address}')
        sock.disconnect(node.address)
        return reply

    def pong(self):
        '''
        Daemon for listening PING request via a UDP socket
        '''
        while True:
            try:
                msg, addr = self.spong.recvfrom(1024)
            except Exception:
                continue
            if not msg: continue
            msg = pickle.loads(msg)
            if msg == PING:
                logger.debug(f'[{self.nodeID}]: received PING from {addr}')
                reply = pickle.dumps(PONG)
                self.spong.sendto(reply, addr)

    def ping(self, address: tuple):
        '''
        Send a PING message to an address, return True or False
        if PONG response was received
        :param address: Address to PING
        :return: True or False if PONG response was received
        '''
        logger.debug(f'[{self.nodeID}]: Sending PING to {address}')
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
                logger.debug(f'[{self.nodeID}]: Received PONG response')
                return True
        logger.warning(f'[{self.nodeID}]: Didn\'t receive PONG response')
        return False

    def join(self, address: str, udp_address: tuple):
        '''
        Try to join to address using udp_address to send PING messages
        to know if node is running
        :param address: Address of the listening node
        :param udp_address: UDP address of the listening node
        '''
        node = conn('unknown', address, udp_address)
        # message to join a group
        msg = (JOIN, self.listen_address, self.udp_address)
        logger.debug(f'Sending JOIN request to {address}')
        reply = self.ssocket_send(msg, node, REPLY)

        if reply is None:
            raise Exception('Cannot reach node')

        code, *args = reply
        if code != ACCEPTED:
            raise Exception(f'[{self.nodeID}]: Unrecognized message {code}, expected ACCEPTED')

        self.nodeID, data = args
        self.manage_ACCEPTED(data)
        logger.debug(f'[{self.nodeID}]: Received ACCEPTED reply')


    def worker(self, worker_address: str):
        '''
        Daemon for managing incoming requests
        :param worker_address: Inproc address to connect to
        '''
        sock = self.context.socket(zmq.ROUTER)
        sock.connect(worker_address)

        while True:
            ident1, ident2, data = sock.recv_multipart()

            def send_response(msg):
                data = pickle.dumps(msg)
                sock.send_multipart((ident1, ident2, data))

            data = pickle.loads(data)
            response = self.manage_request(send_response, data)
            bits = pickle.dumps(response)
            sock.send_multipart([ident1, ident2, bits])


    def manage_request(self, send_response, data):
        '''
        Manage the incoming request
        :param send_response: function to send responses
        :param data: Request to process
        '''
        code, *args = data
        if self.nodeID < 0 and code != ACCEPTED:
            return

        if code == JOIN:
            logger.debug(f'[{self.nodeID}]: Received JOIN request from {args[0]}')
            # Accept new node in the group
            address, udp_address = args
            reply = self.manageJOIN(address, udp_address)
            send_response(reply)

        if code == ADD_NODE:
            logger.debug(f'[{self.nodeID}]: Received ADD_NODE request for {args}')
            id, address, udp_address = args
            item = self.getConnectionByID(id)
            if not item is None:
                item.active = True
            else:
                item = conn(id, address, udp_address)
            self.connections.append(item)

        if code == ELECTION:
            logger.debug(f'[{self.nodeID}]: Received ELECTION request from node {args[0]}')
            send_response((ACK, None))
            self.manageELECTION(args[0])

        if code == COORDINATOR:
            logger.info(f'[{self.nodeID}]: Received COORDINATOR response from node {args[0]}')
            send_response((ACK, None))
            if args[0] < self.leaderID: return
            self.leaderID = args[0]
            self.electionID = -1

            while len(self.new_node_queue) > 0:
                address, udp_address = self.new_node_queue.pop()
                self.manageJOIN(address, udp_address)

        if code == ACCEPTED:
            logger.debug(f'[{self.nodeID}]: Received ACCEPTED response')
            self.nodeID, data = args
            self.manage_ACCEPTED(data)
            node = self.getConnectionByID(self.nodeID)
            node.active = True

        if code == PULL:
            logger.debug(f'[{self.nodeID}]: Received PULL request from {args[0]}')
            ans = self.getConnectionByID(args[0])
            msg = (PUSH, ans , self.leaderID)
            send_response(msg)


    def manageJOIN(self, address: str, udp_address: tuple):
        '''
        Manage JOIN requests
        :param address: TCP address of the node that sends the request
        :param udp_address: UDP address of the node that sends the request
        :return: ACCEPTED response
        '''
        node = self.getConnectionByAddress(address)
        if not node is None: node.active = False

        join = (JOIN, address, udp_address)
        while True:
            while self.electionID != -1:
                pass

            leader = self.leader
            if leader.nodeID == self.nodeID:
                if node is None:
                    newID = len(self.connections)
                    node = conn(newID, address, udp_address)
                    self.connections.append(node)

                node.active = True
                logger.debug(f'[{self.nodeID}]: Sending ACCEPTED response to {address}')
                accepted = (ACCEPTED, node.nodeID, self.get_data())

                addnode = (ADD_NODE, node.nodeID, address, udp_address)
                self.broadcast(addnode, exc=[address])
                return accepted

            reply = self.ssocket_send(join, leader, REPLY)
            if not reply is None:
                return reply

            self.manageELECTION(self.nodeID)


    def manage_ACCEPTED(self, data):
        '''
        Manage ACCEPTED response data
        '''
        self.connections, self.leaderID = data

    def get_data(self):
        '''
        Get the data required for responding PULL messages
        :return:
        '''
        return [self.connections, self.nodeID]

    def broadcast(self, msg, exc=None):
        '''
        Broadcast messages to all nodes not in exc
        :param msg: Message to broadcast
        :param exc: Exception nodes
        '''
        if exc is None: exc = []
        exc.append(self.listen_address)

        for node in self.connections:
            if node.address not in exc:
                logger.debug(f'[{self.nodeID}]: Broadcasting to {node.address}')
                self.ssocket_send(msg, node)

    def manageELECTION(self, electionID: int):
        '''
        Starts an election procedure
        '''
        if self.electionID != -1: return
        self.electionID = electionID
        self.leaderID = -1

        logger.info(f'[{self.nodeID}]: Started election in node {self.nodeID}')
        msg = (ELECTION, self.electionID)
        lastBully = None
        for node in [i for i in self.connections if i.nodeID > self.nodeID and i.active]:
            reply = self.ssocket_send(msg, node, REPLY)
            if not reply is None and (lastBully is None or lastBully.nodeID < node.nodeID):
                logger.debug(f'[{self.nodeID}]: Received bully response from {node.nodeID}')
                lastBully = node
                break

        if lastBully is None:
            logger.info(f'[{self.nodeID}]: Stablishing as coordinator')
            self.leaderID = self.nodeID
            msg = (COORDINATOR, self.nodeID)
            for node in self.connections:
                if node.address != self.listen_address:
                    logger.debug(f'[{self.nodeID}]: sending COORDINATOR request to {node.address}')
                    self.ssocket_send(msg, node)
            self.electionID = -1
            for (address, udp_address) in self.new_node_queue:
                self.manageJOIN(address, udp_address)
        else:
            for (address, udp_address) in self.new_node_queue:
                self.ssocket_send((JOIN, address, udp_address), lastBully)


    def missing(self) -> int:
        '''
        Get the id of a node that may not be in current connections list
        :return: Id of the node
        '''
        self.connections.sort()
        node = len(self.connections)
        for i, item in enumerate(self.connections):
            if i != item.nodeID:
                node = i
        return node

    def pingingDaemon(self):
        '''
        Daemon for send PING requests to random nodes and PULL information from them
        :return:
        '''
        while True:
            seq = [i for i in self.connections if i.nodeID != self.nodeID and i.active]
            if len(self.connections) > 0 and len(seq) == 0:
                logger.warning(f'[{self.nodeID}]: Node is isolated')

                node = random.choice(self.connections)
                if node.address != self.listen_address:
                    try:
                        self.join(node.address, node.udp_address)
                        node.active = True
                    except Exception as e:
                        pass


            if len(seq) != 0:
                node = random.choice(seq)

                if not self.ping(node.udp_address):
                    node.active = False
                    if node.nodeID == self.leaderID:
                        self.manageELECTION(self.nodeID)

                else:
                    node.active = True

                    missing = self.missing()
                    logger.debug(f'[{self.nodeID}]: Sending PULL request to {node.nodeID} for {missing}')
                    msg = (PULL, missing)
                    reply = self.ssocket_send(msg, node, REPLY)

                    if not reply is None:
                        code, node, leader = reply

                        if code == PUSH:
                            logger.debug(f'[{self.nodeID}]: Received PUSH response')

                            if not node is None:
                                temp = self.getConnectionByID(node.nodeID)
                                if not temp is None:
                                    self.connections.append(node)

                            if leader != self.leaderID:
                                self.manageELECTION(self.nodeID)

            time.sleep((len(self.connections) + 1) * 3)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--portin', type=int, default=5000, required=False,
                        help='Port for incoming communications on node')
    parser.add_argument('--address', type=str, required=False,
                        help='Address of node to connect to')
    args = parser.parse_args()

    port1 = args.portin
    if args.address:
        host, port = args.address.split(':')
        address = (host, int(port))
        node = Node(port1, address)
    else:
        node = Node(port1)


if __name__ == '__main__':
    main()