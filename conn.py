


class conn:
    def __init__(self, nodeID=None, address=None, udp_address=None):
        self.nodeID = nodeID
        self.address = address
        self.udp_address = udp_address
        self.active = True

    @property
    def is_valid(self):
        return not self.nodeID is None

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