"""Store and manage load information received from other peers."""

import time
from dataclasses import dataclass


STALE_TIMEOUT = 15.0


@dataclass
class PeerLoad:
    """Load information belonging to one remote peer."""

    node_id: str
    cpu_usage: float
    free_ram_gb: float
    timestamp: float
    received_at: float
    stale_after: float = STALE_TIMEOUT

    @property
    def is_stale(self):
        """Check whether this peer's load information is stale."""

        return (
            time.monotonic() - self.received_at
            > self.stale_after
        )


class PeerLoadStore:
    """Maintain load reports received from other nodes."""

    def __init__(self, stale_after=STALE_TIMEOUT):
        self.peers = {}
        self.stale_after = stale_after

    def update_peer(self, message):
        """Add or update a peer using a received MeshWeaver message."""

        if hasattr(message, "to_dict"):
            message = message.to_dict()

        if message.get("type") != "GOSSIP_LOAD":
            raise ValueError("Invalid message type")

        node_id = message.get("sender_id")
        payload = message.get("payload", {})

        if not node_id:
            raise ValueError("Missing sender_id")

        if not isinstance(payload, dict):
            raise ValueError("Invalid payload")

        if payload.get("node_id") != node_id:
            raise ValueError(
                "Sender ID does not match LoadReport node ID"
            )

        cpu_usage = float(payload["cpu_usage"])
        free_ram_gb = float(payload["free_ram_gb"])
        timestamp = float(payload.get("timestamp", time.time()))

        if not 0 <= cpu_usage <= 100:
            raise ValueError("CPU usage must be between 0 and 100")

        if free_ram_gb < 0:
            raise ValueError("Free RAM cannot be negative")

        peer = PeerLoad(
            node_id=node_id,
            cpu_usage=cpu_usage,
            free_ram_gb=free_ram_gb,
            timestamp=timestamp,
            received_at=time.monotonic(),
            stale_after=self.stale_after,
        )

        self.peers[node_id] = peer

        return peer

    def get_peer(self, node_id):
        """Return one peer's load information."""

        return self.peers.get(node_id)

    def get_fresh_peers(self):
        """Return only peers with fresh load information."""

        return {
            node_id: peer
            for node_id, peer in self.peers.items()
            if not peer.is_stale
        }

    def get_stale_peers(self):
        """Return peers whose load information has expired."""

        return {
            node_id: peer
            for node_id, peer in self.peers.items()
            if peer.is_stale
        }

    def remove_stale_peers(self):
        """Remove expired peer load entries."""

        stale_ids = [
            node_id
            for node_id, peer in self.peers.items()
            if peer.is_stale
        ]

        for node_id in stale_ids:
            del self.peers[node_id]

        return stale_ids
