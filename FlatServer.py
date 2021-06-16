import socket
import pickle
import threading
import time
import zmq
import logging
import random
from constants import *

log = logging.Logger(name='Flat Server')
logging.basicConfig(level=logging.DEBUG)

WAIT_TIMEOUT = 5
MESSAGE_TIMEOUT = 5000

TRIES = 3
RETRANSMITS = 5
THREADS = 5

class Conn:
    def __init__(self, address, nodeID, udp_address):
        self.nodeID = nodeID
        self.address = address
        self.retransmits = 0
        self.active = True
        self.udp_address = udp_address


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
        log.warning(f'socket binded to {self.listen_address}')

        self.worker_address = f'inproc://workers{portin}'
        self.wsock = self.context.socket(zmq.DEALER)
        self.wsock.bind(self.worker_address)
        for i in range(THREADS):
            threading.Thread(target=self.worker, args=(self.worker_address,)).start()

        #self.lsock.setsockopt(zmq.RCVTIMEO, MESSAGE_TIMEOUT)
        # self.lsock.setsockopt(zmq.LINGER, 50000)

        self.sem = threading.Semaphore()
        self.ssock = self.context.socket(zmq.DEALER)
        self.ssock.bind_to_random_port(f'tcp://{self.host}')
        self.ssock.setsockopt(zmq.RCVTIMEO, MESSAGE_TIMEOUT)
        # self.ssock.setsockopt(zmq.LINGER, 50000)

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

        log.warning('starting flat server')

        if serveraddress:
            address = f'tcp://{serveraddress[0]}:{serveraddress[1]}'
            self.join(address)
        else:
            self.connections = [Conn(self.listen_address, self.nodeID, self.udp_address)]
            self.leaderID = self.nodeID

        self.electionID = -1
        self.new_node_queue = []

        threading.Thread(target=self.pong).start()
        threading.Thread(target=self.pingingDaemon).start()
        threading.Thread(target=zmq.device, args=(zmq.QUEUE, self.lsock, self.wsock,)).start()


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

    def ssocket_send(self, msg, address, WaitForReply=True):
        msg = pickle.dumps(msg)
        self.sem.acquire()
        self.ssock.connect(address)
        self.ssock.send(msg)
        if WaitForReply:
            try:
                reply = pickle.loads(self.ssock.recv())
            except Exception as e:
                log.error("TIMEOUT ERROR")
                reply = None
        else:
            reply = None
        self.ssock.disconnect(address)
        self.sem.release()
        return reply

    def pong(self):
        while True:
            msg, addr = self.spong.recvfrom(1024)
            if not msg: continue
            msg = pickle.loads(msg)
            if msg == PING:
                log.warning(f'received PING from {addr}')
                reply = pickle.dumps((PONG, self.leaderID, len(self.connections)))
                self.spong.sendto(reply, addr)

    def ping(self, node):
        if node == self.udp_address: return PONG, None, None
        log.warning(f'pinging {node}')
        msg = pickle.dumps(PING)
        for i in range(5):
            self.sping.sendto(msg, node)
            try:
                reply, addr = self.sping.recvfrom(1024)
            except socket.timeout:
                continue
            reply = pickle.loads(reply)
            if reply[0] == PONG: return reply
        log.warning(f'pinging returned False')
        return None

    def join(self, address):
        # message to join a group
        message = (JOIN, self.listen_address, self.udp_address)
        btime = time.time()

        while True:
            log.warning(f'sending JOIN request to {address}')
            reply = self.ssocket_send(message, address)
            if reply is None:
                if time.time() - btime > WAIT_TIMEOUT:
                    raise Exception('cannot reach node')
                continue
            code, *args = reply

            if code == ACK:
                log.warning(f'received ACK reply')
                break

    def send_response(self, socket, ident1, ident2, msg):
        data = pickle.dumps(msg)
        socket.send_multipart((ident1,ident2,data))


    def worker(self, worker_address):
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
        code, *args = data
        if code == JOIN:
            log.warning(f'received JOIN request from {args[0]}')
            send_response((ACK, None))
            # Accept new node in the group
            address, udp_address = args
            self.manageJOIN(address, udp_address)

        if code == NEW_NODE:
            log.warning(f'received NEW_NODE request for {args[0]}')
            send_response((ACK, None))
            address, udp_address = args
            self.manageNEW_NODE(address, udp_address)

        if code == ADD_NODE:
            log.warning(f'received ADD_NODE request for {args}')
            id, address, udp_address = args
            conn = self.getConnectionByID(id)
            if not conn is None:
                log.warning(f'node {id} has reconnected')
                conn.active = True
            else:
                self.connections.append(Conn(address, id, udp_address))

        if code == ELECTION:
            log.warning(f'received ELECTION request from node {args[0]}')
            send_response((ACK, None))
            self.manageELECTION(args[0])

        if code == COORDINATOR:
            log.warning(f'received COORDINATOR request from node {args[0]}')
            send_response((ACK, None))
            if args[0] < self.leaderID: return
            self.leaderID = args[0]
            self.electionID = -1

            while len(self.new_node_queue) > 0:
                address, udp_address = self.new_node_queue.pop()
                self.manageJOIN(address, udp_address)

        if code == ACCEPTED:
            log.warning(f'received ACCEPTED reply')
            self.nodeID, self.connections, self.leaderID = args

        if code == PULL:
            log.warning(f'received PULL request from {args[0]}')
            msg = (ACK, self.connections)
            send_response(msg)
            conn = self.getConnectionByID(args[0])
            self.ssocket_send(msg, conn.address, False)

        if code == PUSH:
            log.warning(f'received PUSH response')
            arr = args[0]
            for conn in arr:
                temp = self.getConnectionByID(conn.nodeID)
                if temp is None:
                    self.connections.append(conn)

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
        conn = self.leader
        while True:
            if conn.retransmits > RETRANSMITS:
                conn.retransmits = 0
                conn.active = False
                self.new_node_queue.append((address, udp_address))
                self.manageELECTION(self.nodeID)
                break

            conn.retransmits += 1
            # Sending message of new node to the leader
            log.warning(f'sending NEW_NODE request to {conn.address} for {address}')
            reply = self.ssocket_send(new_node_message, conn.address)

            if reply is None: continue
            code, *args = reply
            if code == ACK:
                log.warning(f'received ACK reply')
                break

    def manageNEW_NODE(self, address, udp_address):
        newID = len(self.connections)
        if self.leaderID != self.nodeID:
            return

        conn = self.getConnectionByAddress(address)
        if conn is None:
            self.connections.append(Conn(address, newID, udp_address))##################
        else:
            return

        log.warning(f'sending ACCEPTED request to {address}')
        msg = (ACCEPTED, newID, self.connections, self.nodeID)
        self.ssocket_send(msg, address, False)

        msg = (ADD_NODE, newID, address, udp_address)
        self.broadcast(msg, exc=[address])

        log.warning(f"New node received {newID}")

    def broadcast(self, msg, exc=None):
        if exc is None: exc = []
        exc.append(self.listen_address)

        for conn in self.connections:
            if conn.address not in exc:
                log.warning(f'broadcast {msg} to {conn.address}')
                self.ssocket_send(msg, conn.address, False)

    def manageELECTION(self, electionID):
        '''
        Starts an election procedure
        :return: None
        '''
        if self.electionID != -1: return
        self.electionID = electionID
        self.leaderID = -1

        log.warning(f'started election in node {self.nodeID}')
        msg = (ELECTION, self.electionID)
        lastBully = None
        for conn in [i for i in self.connections if i.nodeID > self.nodeID]:
            reply = self.ssocket_send(msg, conn.address)
            if not reply is None and (lastBully is None or lastBully.nodeID < conn.nodeID):
                log.warning(f'received bully response from {conn.nodeID}')
                lastBully = conn
                break

        if lastBully is None:
            log.warning(f'stablishing as coordinator')
            self.leaderID = self.nodeID
            msg = (COORDINATOR, self.nodeID)
            for conn in self.connections:
                if conn.address != self.listen_address:
                    log.warning(f'sending COORDINATOR request to {conn.address}')
                    self.ssocket_send(msg, conn.address, False)
            self.electionID = -1
            for (address, udp_address) in self.new_node_queue:
                self.manageNEW_NODE(address, udp_address)
        else:
            for (address, udp_address) in self.new_node_queue:
                self.ssocket_send((JOIN, address, udp_address), lastBully.address, False)

    '''
    def ping(self, ID):
        conn = self.getConnectionByID(ID)
        msg = (PING, self.nodeID, self.listen_address)
        reply = self.ssocket_send(msg, conn.address)
        return reply
    '''

    def pingingDaemon(self):
        self.btime = time.time()
        while True:
            seq = [i for i in self.connections if i.nodeID != self.nodeID and i.active]
            if len(self.connections) > 1 and len(seq) == 0:
                log.warning(f'Node is isolated')

                reconnect = False
                for i in range(TRIES):
                    for conn in self.connections:
                        if conn.address != self.listen_address:
                            try:
                                self.join(conn.address)
                            except Exception as e:
                                continue
                            reconnect = True
                            break
                    if reconnect: break
                if not reconnect:
                    continue

            if len(seq) != 0:
                conn = random.choice(seq)
                log.warning(f'pinging node {conn.udp_address}')
                reply = self.ping(conn.udp_address)

                if reply is None:
                    conn.retransmits += 1
                    if conn.retransmits > RETRANSMITS:
                        conn.active = False
                        if conn.nodeID == self.leaderID:
                            self.manageELECTION(self.nodeID)
                else:
                    conn.active = True
                    conn.retransmits = 0
                    log.warning(f'received PONG response')
                    code, leaderID, l = reply

                    if l != len(self.connections):
                        msg = (PULL, self.nodeID)
                        self.ssocket_send(msg, conn.address, False)

                    if leaderID != self.leaderID:
                        self.manageELECTION(self.nodeID)

            time.sleep(len(self.connections) * 5)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--portin', type=int, default=5000, required=False,
                        help='Port for incoming communications on node')
    #parser.add_argument('--portout', type=int, default=5001, required=False,
    #                   help='Port for outgoing communications on node')
    parser.add_argument('--address', type=str, required=False,
                        help='Address of node to connect to')
    args = parser.parse_args()

    port1 = args.portin
    #port2 = args.portout
    if args.address:
        host, port = args.address.split(':')
        address = (host, int(port))
        node = Node(port1, address)
    else:
        node = Node(port1)


if __name__ == '__main__':
    main()