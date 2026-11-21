import time
from dataclasses import dataclass


STALE_TIMEOUT = 15


@dataclass
class PeerLoad:

    node_id: str
    cpu_usage: float
    free_ram_gb: float
    timestamp: float
    received_at: float

    @property
    def is_stale(self):

        return (
            time.monotonic() - self.received_at
            > STALE_TIMEOUT
        )


class PeerLoadStore:

    def __init__(self):

        self.peers = {}

    def update_peer(self, message):

        if message.get("type") != "GOSSIP_LOAD":
            raise ValueError("Invalid gossip message")

        node_id = message.get("sender_id")

        payload = message.get("payload", {})

        if not node_id:
            raise ValueError("Missing sender node ID")

        cpu_usage = float(payload["cpu_usage"])
        free_ram_gb = float(payload["free_ram_gb"])
        timestamp = float(payload["timestamp"])

        self.peers[node_id] = PeerLoad(
            node_id=node_id,
            cpu_usage=cpu_usage,
            free_ram_gb=free_ram_gb,
            timestamp=timestamp,
            received_at=time.monotonic()
        )

        print("Updated peer load:", node_id)

    def get_peer(self, node_id):

        return self.peers.get(node_id)

    def get_fresh_peers(self):

        return {
            node_id: peer
            for node_id, peer in self.peers.items()
            if not peer.is_stale
        }

    def get_stale_peers(self):

        return {
            node_id: peer
            for node_id, peer in self.peers.items()
            if peer.is_stale
        }

    def remove_stale_peers(self):

        stale_ids = [
            node_id
            for node_id, peer in self.peers.items()
            if peer.is_stale
        ]

        for node_id in stale_ids:
            del self.peers[node_id]

        return stale_ids
if __name__ == "__main__":

    store = PeerLoadStore()

    test_message = {
        "type": "GOSSIP_LOAD",
        "sender_id": "node_001",
        "payload": {
            "cpu_usage": 35.5,
            "free_ram_gb": 4.5,
            "timestamp": time.time()
        }
    }

    store.update_peer(test_message)

    print("\nStored peer information:")
    print(store.get_peer("node_001"))

    print("\nFresh peers:")
    print(store.get_fresh_peers())
