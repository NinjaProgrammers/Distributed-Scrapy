import FlatServer
import chordServer
import logging

log = logging.Logger(name='Flat Server')
logging.basicConfig(level=logging.DEBUG)

JOIN_GROUP = 101
RANDOM_NODE = 102
ADD_GROUP = 103

class Broker(chordServer.Node, FlatServer.Node):
    def __init__(self, portin=5000, portout=5001, serveraddress=None, nbits=30):
        FlatServer.Node.__init__(self, portin=portin, portout=portout, serveraddress=serveraddress)
        chordServer.Node.__init__(self, nbits=nbits)


    def manageRequest(self, ident, data):
        super().manageRequest(ident, data)

        code, *args = data
        if code == JOIN_GROUP:
            log.warning(f'received JOINGROUP request')
            role, name = args
            id = self.registerNode(role, name)
            self.lsocket_send(ident, (id, self.NBits))
            msg = (ADD_GROUP, id, role, name)
            self.broadcast(msg)

        if code == RANDOM_NODE:
            log.warning(f'received RANDOMNODE request')
            role, exceptions = args
            node = self.getRandomNode(role, exceptions)
            if node is None:
                self.lsocket_send(ident, (None, None))
            else:
                self.lsocket_send(ident, (node.key, node.address))

        if code == ADD_GROUP:
            id, role, name = args
            self.addToGroup(id, role, name)



def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--portin', type=int, default=5000, required=False,
                        help='Port for incoming communications on node')
    parser.add_argument('--portout', type=int, default=5001, required=False,
                        help='Port for outgoing communications on node')
    parser.add_argument('--address', type=str, required=False,
                        help='Address of node to connect to')
    parser.add_argument('--nbits', type=int, default=5, required=False,
                        help='Number of bits of the chord model')
    args = parser.parse_args()

    port1 = args.portin
    port2 = args.portout
    nbits = args.nbits
    if args.address:
        host, port = args.address.split(':')
        address = (host, int(port))
        node = Broker(port1, port2, address, nbits=nbits)
    else:
        node = Broker(port1, port2, nbits=nbits)


if __name__ == '__main__':
    main()