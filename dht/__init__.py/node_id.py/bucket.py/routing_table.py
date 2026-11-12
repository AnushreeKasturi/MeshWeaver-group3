

from .bucket import KBucket
from .node_id import get_bucket_index

ID_BITS = 160


class RoutingTable:

    def __init__(self, local_node_id):
        self.local_node_id = local_node_id
        self.buckets = [
            KBucket() for _ in range(ID_BITS)
        ]

    def add_peer(self, peer_id):
        """Add a peer to the correct bucket."""

        if peer_id == self.local_node_id:
            return False

        index = get_bucket_index(
            self.local_node_id,
            peer_id
        )

        if index == -1:
            return False

        result = self.buckets[index].add_peer(peer_id)

        # None means added or already present.
        # A peer ID means the bucket is full.
        return result is None

    def remove_peer(self, peer_id):
        """Remove a peer from the correct bucket."""

        index = get_bucket_index(
            self.local_node_id,
            peer_id
        )

        if index == -1:
            return False

        bucket = self.buckets[index]

        if peer_id not in bucket.get_peers():
            return False

        bucket.remove_peer(peer_id)
        return True

    def get_bucket(self, index):
        """Return a bucket by its index."""

        if not 0 <= index < ID_BITS:
            raise IndexError("Invalid bucket index")

        return self.buckets[index]

    def get_all_peers(self):
        """Return all peers in the routing table."""

        peers = []

        for bucket in self.buckets:
            peers.extend(bucket.get_peers())

        return peers

    def get_peer_count(self):
        """Return total number of peers."""

        return len(self.get_all_peers())


if __name__ == "__main__":

    print("========== MeshWeaver Routing Table ==========")

    table = RoutingTable(100)

    print("Local Node ID:", table.local_node_id)

    print("\nAdding Peers...")

    peers = [200, 300, 400, 500]

    for peer in peers:
        result = table.add_peer(peer)
        print(f"Peer {peer} added: {result}")

    print("\nAll Connected Peers:")

    for peer in table.get_all_peers():
        print(peer)

    print("\nTotal Peers:", table.get_peer_count())

    print("\nRemoving Peer 200...")

    result = table.remove_peer(200)
    print("Removed:", result)

    print("\nUpdated Peer List:")
    print(table.get_all_peers())

    print("\nTotal Peers:", table.get_peer_count())

    print("\n========== Execution Completed ==========")
