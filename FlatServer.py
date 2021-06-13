import socket
import pickle
import threading
import time
import zmq
import logging
import random

log = logging.Logger(name='Flat Server')
logging.basicConfig(level=logging.DEBUG)

# Joining constants
JOIN = 1
NEW_NODE = 2
ADD_NODE = 3
ACCEPTED = 4
REFUSED = 5

# Election constants
ELECTION = 6
COORDINATOR = 7

ACK = 8
PING = 9
PONG = 10
PULL = 11
PUSH = 12

WAIT_TIMEOUT = 5
MESSAGE_TIMEOUT = 1000

TRIES = 3
RETRANSMITS = 5

class Conn:
    def __init__(self, address, nodeID):
        self.nodeID = nodeID
        self.address = address
        self.retransmits = 0
        self.active = True

class Node:

    def __init__(self, portin=5000, portout=5001, serveraddress=None):
        # My Address
        hostname = socket.gethostname()
        self.host = socket.gethostbyname(hostname)

        # Socket to listen requests from other nodes
        self.context = zmq.Context()
        self.lsock = self.context.socket(zmq.ROUTER)
        self.listen_address = f'tcp://{self.host}:{portin}'
        self.lsock.bind(self.listen_address)
        log.warning(f'socket binded to {self.listen_address}')
        self.lsock.setsockopt(zmq.RCVTIMEO, MESSAGE_TIMEOUT)
        #self.lsock.setsockopt(zmq.LINGER, 50000)

        self.sem = threading.Semaphore()
        self.ssock = self.context.socket(zmq.DEALER)
        #self.ssock.bind(f'tcp://{self.host}:{portout}')
        self.ssock.setsockopt(zmq.RCVTIMEO, MESSAGE_TIMEOUT)
        #self.ssock.setsockopt(zmq.LINGER, 50000)

        # Nodes Info
        self.leaderID = 0
        self.nodeID = 0
        self.connections = []

        log.warning('starting flat server')


        if serveraddress:
            address = f'tcp://{serveraddress[0]}:{serveraddress[1]}'
            self.join(address)
        else:
            self.connections = [Conn(self.listen_address, self.nodeID)]
            self.leaderID = self.nodeID

        self.election_boolean = False

        log.warning('listening for connections')
        threading.Thread(target=self.manageConnections).start()
        log.warning('starting pinging daemon')
        threading.Thread(target=self.pingingDaemon).start()

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

    def ssocket_send(self, msg, address):
        msg = pickle.dumps(msg)

        self.sem.acquire()
        self.ssock.connect(address)
        self.ssock.send(msg)

        try:
            reply = pickle.loads(self.ssock.recv())
        except Exception as e:
            reply = None
        self.ssock.disconnect(address)
        self.sem.release()

        return reply


    def lsocket_recv(self):
        try:
            ident, reply = self.lsock.recv_multipart()
            reply = pickle.loads(reply)
        except Exception as e:
            #print(e)
            ident, reply = None, None

        return ident, reply


    def lsocket_send(self, ident, msg=(ACK, None)):
        msg = pickle.dumps(msg)
        self.lsock.send_multipart([ident, msg])


    def join(self, address):
        # message to join a group
        message = (JOIN, self.listen_address)
        btime = time.time()

        while True:
            log.warning(f'sending JOIN request from {self.listen_address}')
            reply = self.ssocket_send(message, address)
            if reply is None: continue
            code, *args = reply

            if code == ACK:
                log.warning(f'received ACK reply')
                break

            if time.time() - btime > WAIT_TIMEOUT:
                raise Exception('cannot reach node')

    def manageConnections(self):
        while True:
            ident, data = self.lsocket_recv()
            if data is None: continue
            self.manageRequest(ident, data)

    def manageRequest(self, ident, data):
        code, *args = data
        if code == JOIN:
            log.warning(f'received JOIN request from {args[0]}')
            self.lsocket_send(ident)
            # Accept new node in the group
            self.manageJOIN(args[0])

        if code == NEW_NODE:
            log.warning(f'received NEW_NODE request for {args[0]}')
            self.lsocket_send(ident)
            self.manageNEW_NODE(args[0])

        if code == ADD_NODE:
            log.warning(f'received ADD_NODE request for {args}')
            id, address = args
            conn = self.getConnectionByID(id)
            if not conn is None:
                log.warning(f'node {id} has reconnected')
                conn.active = True
            else:
                self.connections.append(Conn(address, id))

        if code == ELECTION:
            log.warning(f'received ELECTION request from node {args[0]}')
            msg = (ACK, )
            self.lsocket_send(ident, msg)
            self.manageELECTION()

        if code == COORDINATOR:
            log.warning(f'received COORDINATOR request from node {args[0]}')
            self.lsocket_send(ident)
            if args[0] < self.leaderID: return
            self.leaderID = args[0]
            self.election_boolean = False

        if code == ACCEPTED:
            log.warning(f'received ACCEPTED reply')
            self.nodeID, self.connections, self.leaderID = args

        if code == PING:
            log.warning(f'received ping request')
            message = (PONG, self.leaderID, len(self.connections))
            conn = self.getConnectionByID(args[0])
            if not conn is None:
                conn.retransmits = 0
                conn.active = True
            else:
                id, address = args
                self.connections.append(Conn(address, id))
            self.lsocket_send(ident, message)
            log.warning(f'sended PONG response')

        if code == PULL:
            log.warning(f'received PULL request from {args[0]}')
            msg = (ACK, self.connections)
            conn = self.getConnectionByID(args[0])
            self.ssocket_send(msg, conn.address)

        if code == PUSH:
            log.warning(f'received PUSH response')
            arr = args[0]
            for conn in arr:
                temp = self.getConnectionByID(conn.nodeID)
                if temp is None:
                    self.connections.append(conn)


    # address is the listening address of the socket connecting
    def manageJOIN(self, address):
        #I am the coordinator
        if self.leaderID == self.nodeID:
            self.manageNEW_NODE(address)
            return

        # Tell coordinator node the new node address
        new_node_message = (NEW_NODE, address)
        conn = self.leader
        while True:
            if conn.retransmits > RETRANSMITS:
                conn.retransmits = 0
                conn.active = False
                self.manageELECTION()
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

    def manageNEW_NODE(self, address):
        newID = len(self.connections)
        if self.leaderID != self.nodeID:
            return

        conn = self.getConnectionByAddress(address)
        if conn is None: self.connections.append(Conn(address, newID))
        else: return

        log.warning(f'sending ACCEPTED request to {address}')
        msg = (ACCEPTED, newID, self.connections, self.nodeID)
        self.ssocket_send(msg, address)

        msg = (ADD_NODE, newID, address)
        self.broadcast(msg, exc=[address])

        log.warning(f"New node received {newID}")


    def broadcast(self, msg, exc=None):
        if exc is None: exc = []
        exc.append(self.listen_address)
        
        for conn in self.connections:
            if not conn.address in exc:
                log.warning(f'broadcast {msg} to {conn.address}')
                self.ssocket_send(msg, conn.address)


    def manageELECTION(self):
        '''
        Starts an election procedure
        :return: None
        '''
        if self.election_boolean: return
        self.election_boolean = True
        self.leaderID = -1

        log.warning(f'started election in node {self.nodeID}')
        msg = (ELECTION, self.nodeID)
        for conn in [i for i in self.connections if i.nodeID > self.nodeID]:
            reply = self.ssocket_send(msg, conn.address)
            if not reply is None:
                log.warning(f'received bully response from {conn.nodeID}')
                break
        else:
            log.warning(f'stablishing as coordinator')
            self.leaderID = self.nodeID
            msg = (COORDINATOR, self.nodeID)
            for conn in self.connections:
                if conn.address != self.listen_address:
                    log.warning(f'sending COORDINATOR request to {conn.address}')
                    self.ssocket_send(msg, conn.address)
            self.election_boolean = False

    def ping(self, ID):
        conn = self.getConnectionByID(ID)
        msg = (PING, self.nodeID, self.listen_address)
        reply = self.ssocket_send(msg, conn.address)
        return reply

    def pingingDaemon(self):
        self.btime = time.time()
        while True:
            seq = [i for i in self.connections if i.nodeID != self.nodeID and i.active]
            if len(self.connections) > 1 and len(seq) == 0:
                log.warning(f'node is isolated')

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
                    raise Exception('conection has been lost')

            if len(seq) != 0:
                conn = random.choice(seq)
                log.warning(f'pinging node {conn.nodeID}')
                reply = self.ping(conn.nodeID)

                if reply is None or reply[0] != PONG:
                    conn.retransmits += 1
                    if conn.retransmits > RETRANSMITS:
                        conn.active = False
                        if conn.nodeID == self.leaderID:
                            self.manageELECTION()
                else:
                    conn.active = True
                    conn.retransmits = 0
                    log.warning(f'received PONG response')
                    code, leaderID, l = reply

                    if l != len(self.connections):
                        msg = (PULL, self.nodeID)
                        reply = self.ssocket_send(msg, conn.address)

                    if leaderID != self.leaderID:
                        self.manageELECTION()

            time.sleep(len(self.connections))


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--portin', type=int, default=5000, required=False,
                        help='Port for incoming communications on node')
    parser.add_argument('--portout', type=int, default=5001, required=False,
                        help='Port for outgoing communications on node')
    parser.add_argument('--address', type=str, required=False,
                        help='Address of node to connect to')
    args = parser.parse_args()

    port1 = args.portin
    port2 = args.portout
    if args.address:
        host, port = args.address.split(':')
        address = (host, int(port))
        node = Node(port1, port2, address)
    else:
        node = Node(port1, port2)


if __name__ == '__main__':
    main()