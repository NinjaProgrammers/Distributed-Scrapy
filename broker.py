import FlatServer
import chordServer

NBits = 100
JOIN_GROUP = 101
RANDOM_NODE = 102

class Broker(chordServer.Node, FlatServer.Node):
    def __init__(self, port, serveraddress=None, nbits=30):
        FlatServer.Node.__init__(self, port=port, serveraddress=serveraddress)
        chordServer.Node.__init__(self, nbits=nbits)


    def manageRequest(self, ident, data):
        super().manageRequest(ident, data)

        code, *args = data
        if code == NBits:
            msg = self.NBits
            self.lsocket_send(ident, msg)

        if code == JOIN_GROUP:
            role, name = args
            self.registerNode(role, name)
            msg = (JOIN_GROUP, role, name)
            self.broadcast(msg)

        if code == RANDOM_NODE:
            role, id = args
            node = self.getRandomNode(role, id)
            msg = node
            self.lsocket_send(ident, msg)


