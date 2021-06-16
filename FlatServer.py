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



class Node:

    def __init__(self, portin=5000, serveraddress=None):
        # My Address
        hostname = socket.gethostname()
        self.host = socket.gethostbyname(hostname)

        # Socket to listen requests from other nodes
        self.context = zmq.Context()

        self.lsock = self.context.socket(zmq.ROUTER)
        self.listen_address = f'tcp://{self.host}:{portin}'
        self.lsock.bind(self.listen_address)
        logger.warning(f'socket binded to {self.listen_address}')

        self.worker_address = f'inproc://workers{portin}'
        self.wsock = self.context.socket(zmq.DEALER)
        self.wsock.bind(self.worker_address)
        for i in range(THREADS):
            threading.Thread(target=self.worker, args=(self.worker_address,)).start()

        # Nodes Info
        self.leaderID = 0
        self.nodeID = 0
        self.connections = []

        # Ping-Pong sockets
        self.sping = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sping.settimeout(1)
        self.spong = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.spong.bind((self.host, 0))
        self.udp_address = self.spong.getsockname()
        print(f"UDP address {self.udp_address}")

        logger.warning('starting flat server')

        if serveraddress:
            address = f'tcp://{serveraddress[0]}:{serveraddress[1]}'
            self.join(address)
        else:
            self.connections = [conn(self.nodeID, self.listen_address, self.udp_address)]
            self.leaderID = self.nodeID

        self.electionID = -1
        self.new_node_queue = []

        threading.Thread(target=self.pong, name='PONG').start()
        threading.Thread(target=self.pingingDaemon, name='Pinging').start()
        threading.Thread(target=zmq.device, args=(zmq.QUEUE, self.lsock, self.wsock,), name='Device').start()


    @property
    def leader(self):
        return self.getConnectionByID(self.leaderID)

    def getConnectionByAddress(self, address):
        arr = [i for i in self.connections if i.address == address]
        if len(arr) == 0: return None
        return arr[0]

    def getConnectionByID(self, ID):
        arr = [i for i in self.connections if i.nodeID == ID]
        if len(arr) == 0: return None
        return arr[0]

    def send(self, msg, address):
        msg = pickle.dumps(msg)
        sock = self.context.socket(zmq.DEALER)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.connect(address)
        sock.send(msg)
        reply = None
        try:
            reply = pickle.loads(sock.recv())
        except Exception as e:
            pass
        sock.disconnect(address)
        return reply

    def ssocket_send(self, msg, node, WaitForReply=True):
        msg = pickle.dumps(msg)
        sock = self.context.socket(zmq.DEALER)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.connect(node.address)
        sock.send(msg)
        reply = None
        while WaitForReply:
            try:
                sock.send(msg)
                reply = pickle.loads(sock.recv())
            except Exception as e:
                pass

            if reply is None and self.ping(node.udp_address):
                continue
            else:
                break
        sock.disconnect(node.address)
        return reply

    def pong(self):
        while True:
            try:
                msg, addr = self.spong.recvfrom(1024)
            except Exception:
                continue
            if not msg: continue
            msg = pickle.loads(msg)
            if msg == PING:
                logger.warning(f'received PING from {addr}')
                reply = pickle.dumps(PONG)
                self.spong.sendto(reply, addr)

    def ping(self, address):
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
                return True
        logger.warning(f'pinging returned False')
        return False

    def join(self, address):
        # message to join a group
        msg = (JOIN, self.listen_address, self.udp_address)
        logger.warning(f'sending JOIN request to {address}')
        reply = self.send(msg, address)

        if reply is None:
            raise Exception('cannot reach node')
        code, *args = reply

        if code == ACK:
            logger.warning(f'received ACK reply')


    def worker(self, worker_address):
        sock = self.context.socket(zmq.ROUTER)
        sock.connect(worker_address)

        while True:
            ident1, ident2, data = sock.recv_multipart()

            def send_response(msg):
                data = pickle.dumps(msg)
                sock.send_multipart((ident1, ident2, data))

            data = pickle.loads(data)
            logger.warning(data)
            response = self.manage_request(send_response, data)

            bits = pickle.dumps(response)
            sock.send_multipart([ident1, ident2, bits])


    def manage_request(self, send_response, data):
        code, *args = data
        if code == JOIN:
            logger.warning(f'received JOIN request from {args[0]}')
            send_response((ACK, None))
            # Accept new node in the group
            address, udp_address = args
            self.manageJOIN(address, udp_address)

        if code == NEW_NODE:
            logger.warning(f'received NEW_NODE request for {args[0]}')
            send_response((ACK, None))
            address, udp_address = args
            self.manageNEW_NODE(address, udp_address)

        if code == ADD_NODE:
            logger.warning(f'received ADD_NODE request for {args}')
            id, address, udp_address = args
            item = self.getConnectionByID(id)
            if not item is None:
                logger.warning(f'node {id} has reconnected')
                item.active = True
            else:
                self.connections.append(conn(id, address, udp_address))

        if code == ELECTION:
            logger.warning(f'received ELECTION request from node {args[0]}')
            send_response((ACK, None))
            self.manageELECTION(args[0])

        if code == COORDINATOR:
            logger.warning(f'received COORDINATOR request from node {args[0]}')
            send_response((ACK, None))
            if args[0] < self.leaderID: return
            self.leaderID = args[0]
            self.electionID = -1

            while len(self.new_node_queue) > 0:
                address, udp_address = self.new_node_queue.pop()
                self.manageJOIN(address, udp_address)

        if code == ACCEPTED:
            logger.warning(f'received ACCEPTED reply')
            self.nodeID, data = args
            self.manage_ACCEPTED(data)

        if code == PULL:
            logger.warning(f'received PULL request from {args[0]}')
            ans = self.managePULL(args[0])
            msg = (PUSH, ans , self.leaderID)
            send_response(msg)


    def managePULL(self, node):
        item = self.getConnectionByID(node)
        return item

    # address is the listening address of the socket connecting
    def manageJOIN(self, address, udp_address):
        if self.electionID != -1:
            self.new_node_queue.append((address, udp_address))
            return

        # I am the coordinator
        if self.leaderID == self.nodeID:
            self.manageNEW_NODE(address, udp_address)
            return

        # Tell coordinator node the new node address
        new_node_message = (NEW_NODE, address, udp_address)
        node = self.leader

        logger.warning(f'sending NEW_NODE request to {node.address} for {address}')
        reply = self.ssocket_send(new_node_message, node)

        if reply is None:
            self.new_node_queue.append((address, udp_address))
            self.manageELECTION(self.nodeID)
        else:
            code, *args = reply
            if code == ACK:
                logger.warning(f'received ACK reply')


    def manageNEW_NODE(self, address, udp_address):
        newID = len(self.connections)
        if self.leaderID != self.nodeID:
            return

        node = self.getConnectionByAddress(address)
        if node is None:
            node = conn(newID, address, udp_address)
            self.connections.append(node)
        else:
            return

        logger.warning(f'sending ACCEPTED request to {address}')
        msg = (ACCEPTED, newID, self.get_data())
        self.ssocket_send(msg, node, False)

        msg = (ADD_NODE, newID, address, udp_address)
        self.broadcast(msg, exc=[address])

        logger.warning(f"New node received {newID}")

    def manage_ACCEPTED(self, data):
        self.connections, self.leaderID = data

    def get_data(self):
        return [self.connections, self.nodeID]

    def broadcast(self, msg, exc=None):
        if exc is None: exc = []
        exc.append(self.listen_address)

        for node in self.connections:
            if node.address not in exc:
                logger.warning(f'broadcast {msg} to {node.address}')
                self.ssocket_send(msg, node, False)

    def manageELECTION(self, electionID):
        '''
        Starts an election procedure
        :return: None
        '''
        if self.electionID != -1: return
        self.electionID = electionID
        self.leaderID = -1

        logger.warning(f'started election in node {self.nodeID}')
        msg = (ELECTION, self.electionID)
        lastBully = None
        for node in [i for i in self.connections if i.nodeID > self.nodeID]:
            reply = self.ssocket_send(msg, node)
            if not reply is None and (lastBully is None or lastBully.nodeID < node.nodeID):
                logger.warning(f'received bully response from {node.nodeID}')
                lastBully = node
                break

        if lastBully is None:
            logger.warning(f'stablishing as coordinator')
            self.leaderID = self.nodeID
            msg = (COORDINATOR, self.nodeID)
            for node in self.connections:
                if node.address != self.listen_address:
                    logger.warning(f'sending COORDINATOR request to {node.address}')
                    self.ssocket_send(msg, node, False)
            self.electionID = -1
            for (address, udp_address) in self.new_node_queue:
                self.manageNEW_NODE(address, udp_address)
        else:
            for (address, udp_address) in self.new_node_queue:
                self.ssocket_send((JOIN, address, udp_address), lastBully, False)


    def missing(self):
        self.connections.sort()
        #sort = sorted(self.connections)
        node = len(self.connections)
        for i, item in enumerate(self.connections):
            if i != item.nodeID:
                node = i
        return node

    def pingingDaemon(self):
        self.btime = time.time()
        while True:
            seq = [i for i in self.connections if i.nodeID != self.nodeID and i.active]
            if len(self.connections) > 1 and len(seq) == 0:
                logger.warning(f'Node is isolated')

                reconnect = False
                for i in range(TRIES):
                    for node in self.connections:
                        if node.address != self.listen_address:
                            try:
                                self.join(node.address)
                            except Exception as e:
                                continue
                            reconnect = True
                            break
                    if reconnect: break
                if not reconnect:
                    continue

            if len(seq) != 0:
                node = random.choice(seq)
                logger.warning(f'pinging node {node.udp_address}')

                if not self.ping(node.udp_address):
                    node.retransmits += 1
                    if node.retransmits > RETRANSMITS:
                        node.active = False
                        if node.nodeID == self.leaderID:
                            self.manageELECTION(self.nodeID)
                else:
                    node.active = True
                    node.retransmits = 0
                    logger.warning(f'received PONG response')

                    missing = self.missing()

                    msg = (PULL, missing)
                    reply = self.ssocket_send(msg, node)

                    if not reply is None:
                        code, node, leader = reply

                        if code == PUSH:
                            logger.warning(f'received PUSH response')

                            if not node is None:
                                temp = self.getConnectionByID(node.nodeID)
                                if not temp is None:
                                    self.connections.append(node)

                            if leader != self.leaderID:
                                self.manageELECTION(self.nodeID)

            time.sleep(len(self.connections) * 5)


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