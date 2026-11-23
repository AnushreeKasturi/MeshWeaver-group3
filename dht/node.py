
"""MeshWeaver DHT module.

Uses Member A's shared Node, Module and protocol architecture.
"""

from __future__ import annotations

from typing import Any

from ..node import Module
from ..protocol import (
    MessageType,
    PeerInfo,
    FindNode,
    FindNodeReply,
    FindValue,
    FindValueReply,
    Store,
    StoreReply,
    node_id_to_hex,
    node_id_from_hex,
)

from .routing_table import RoutingTable
from .node_id import generate_node_id


class DHT(Module):
    """Distributed Hash Table module."""

    name = "dht"

    def __init__(self):
        super().__init__()

        self.routing_table = None

        # Maps integer node IDs to (host, port).
        self.peer_addresses: dict[int, tuple[str, int]] = {}

        # Locally stored key-value data.
        self.data_store: dict[str, Any] = {}

    def handlers(self):
        """Register DHT request handlers."""
        return {
            MessageType.FIND_NODE: self.on_find_node,
            MessageType.STORE: self.on_store,
            MessageType.FIND_VALUE: self.on_find_value,
        }

    async def start(self):
        """Initialize the routing table after the module is attached."""
        self.routing_table = RoutingTable(self.node.node_id)

    async def stop(self):
        """Stop DHT background work if added later."""
        pass

    # -------------------------------------------------
    # PEER MANAGEMENT
    # -------------------------------------------------

    def add_peer(self, peer: PeerInfo) -> bool:
        """Add a peer using Member A's PeerInfo format."""

        peer_id = node_id_from_hex(peer.node_id)

        if peer_id == self.node.node_id:
            return False

        added = self.routing_table.add_peer(peer_id)

        if added:
            self.peer_addresses[peer_id] = (
                peer.host,
                peer.port,
            )

        return added

    def get_nearest_peer_infos(
        self,
        target_id: int,
        count: int | None = None,
    ) -> list[PeerInfo]:
        """Return the closest known peers to a target ID."""

        if count is None:
            count = self.node.config.k

        peer_ids = self.routing_table.get_all_peers()

        nearest = sorted(
            peer_ids,
            key=lambda peer_id: peer_id ^ target_id,
        )[:count]

        result = []

        for peer_id in nearest:
            address = self.peer_addresses.get(peer_id)

            if address is None:
                continue

            host, port = address

            result.append(
                PeerInfo(
                    node_id=node_id_to_hex(peer_id),
                    host=host,
                    port=port,
                )
            )

        return result

    # -------------------------------------------------
    # FIND_NODE
    # -------------------------------------------------

    async def on_find_node(self, msg, addr):
        """Respond with the closest known peers."""

        request = msg.parsed()
        target_id = node_id_from_hex(request.target_id)

        peers = self.get_nearest_peer_infos(target_id)

        return msg.reply(
            MessageType.FIND_NODE_REPLY,
            self.node.id_hex,
            FindNodeReply.from_peers(peers),
        )

    # -------------------------------------------------
    # STORE
    # -------------------------------------------------

    async def on_store(self, msg, addr):
        """Store a key-value pair locally."""

        request = msg.parsed()

        self.data_store[request.key] = request.value

        return msg.reply(
            MessageType.STORE_REPLY,
            self.node.id_hex,
            StoreReply(ok=True),
        )

    # -------------------------------------------------
    # FIND_VALUE
    # -------------------------------------------------

    async def on_find_value(self, msg, addr):
        """Return a value or nearby peers."""

        request = msg.parsed()

        if request.key in self.data_store:
            response = FindValueReply(
                found=True,
                value=self.data_store[request.key],
            )
        else:
            target_id = generate_node_id(request.key)

            response = FindValueReply(
                found=False,
                peers=self.get_nearest_peer_infos(target_id),
            )

        return msg.reply(
            MessageType.FIND_VALUE_REPLY,
            self.node.id_hex,
            response,
        )


        

        

             
