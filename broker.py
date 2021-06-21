import FlatServer
import chordServer
import random
from constants import *
from socketserver import ThreadingMixIn
from logFormatter import logger


class Broker(chordServer.node, FlatServer.Node,ThreadingMixIn):
    def __init__(self, portin=5000, serveraddress=None, nbits=30):
        chordServer.node.__init__(self, nbits=nbits)
        FlatServer.Node.__init__(self, portin=portin, serveraddress=serveraddress)

    def manage_request(self, send_response, data):
        super().manage_request(send_response,data)

        code, *args = data
        if code == JOIN_GROUP:
            logger.debug(f'Received JOIN_GROUP request')
            address, udp_address = args
            id = self.registerNode(address, udp_address)
            send_response((id, self.NBits))
            msg = (ADD_GROUP, id, address, udp_address)
            self.broadcast(msg)

        if code == RANDOM_NODE:
            logger.debug(f'Received RANDOM_NODE request')
            exceptions = args[0]
            node = self.getRandomNode(exceptions)
            if node is None:
                send_response((None, None, None))
            else:
                send_response((node.nodeID, node.address, node.udp_address))

        if code == ADD_GROUP:
            id, address, udp_address = args
            logger.debug(f'Received ADD_GROUP request for {id}')
            self.addToGroup(id, address, udp_address)

    def get_data(self):
        data = super().get_data()
        data.append(self.nodes)
        return data

    def manage_ACCEPTED(self, data):
        self.connections, self.leaderID, self.nodes = data

    def __get_cache_data__(self, key):
        '''
        Get data corresponding to key from cache if it is stored
        :param key: Key of the data
        '''
        arr = [i for i in self.nodes]
        while len(arr) > 0:
            c = random.choice(arr)
            arr.remove(c)

            logger.debug("Sending GET_DATA for " + key + " to node " + c.address)
            reply = self.ssocket_send((GET_DATA, key), c, FlatServer.REPLY | FlatServer.RESEND)
            if reply is None:
                c.active = False
                continue
            return reply
        return "Empty"

    def __save_cache_data__(self, key, data):
        '''
        Stores data in cache with its corresponding key
        :param key: Key corresponding to data
        :param data: Data to store
        '''
        arr = [i for i in self.nodes]
        while len(arr) > 0:
            c = random.choice(arr)
            arr.remove(c)

            logger.debug(f"Sending SAVE_DATA for {key} to node {c.address}" )
            reply = self.ssocket_send((SAVE_DATA, key, data), c, FlatServer.REPLY)
            if reply is None or reply != ACK:
                c.active = False
                continue
            break





