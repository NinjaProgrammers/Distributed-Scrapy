


class conn:
    def __init__(self, nodeID, address, udp_address):
        self.nodeID = nodeID
        self.address = address
        self.udp_address = udp_address
        self.retransmits = 0
        self.active = True

    def __eq__(self, other):
        return not other is None and self.nodeID == other.nodeID \
               and self.address == other.address \
               and self.udp_address == other.udp_address

    def __str__(self):
        return str(self.nodeID)

    def __repr__(self):
        return self.__str__()

    def __le__(self, other):
        return self.nodeID <= other.nodeID

    def __lt__(self, other):
        return self.nodeID < other.nodeID