from collections import OrderedDict
K = 20
class KBucket:
    def __init__(self):
        self.peers = OrderedDict()

    def add_peer(self, peer_id):

        if peer_id in self.peers:
            self.peers.move_to_end(peer_id)
            return None

        if len(self.peers) < K:
            self.peers[peer_id] = True
            return None

        return next(iter(self.peers))
    def remove_peer(self, peer_id):
        self.peers.pop(peer_id, None)

    def get_peers(self):
        return list(self.peers.keys())
    def size(self):
        return len(self.peers)
if __name__ == "__main__":
    bucket = KBucket()
    print("===== MeshWeaver K-Bucket Demo =====")
    for i in range(1, 6):
        bucket.add_peer(i)
        print("Added Peer:", i)
    print("\nAll Peers:", bucket.get_peers())
    print("Total Peers:", bucket.size())
    bucket.add_peer(3)
    print("\nAfter updating Peer 3:", bucket.get_peers())
    bucket.remove_peer(2)
    print("\nAfter removing Peer 2:", bucket.get_peers())

    print("Final Bucket Size:", bucket.size())

