
"""MeshWeaver Distributed Hash Table module."""

from __future__ import annotations

from typing import Any

from ..node import Module

from ..protocol import (
    MessageType,
    PeerInfo,
    FindNodeReply,
    FindValueReply,
    StoreReply,
    node_id_to_hex,
    node_id_from_hex,
)

from .routing_table import RoutingTable
from .node_id import generate_node_id
from .lookup import iterative_find_node
from .rpc import request_store, request_find_value
from .bootstrap import bootstrap, multicast_discovery, refresh_loop


class DHT(Module):
    """Distributed Hash Table module."""

    name = "dht"

    def __init__(self):
        super().__init__()

        self.routing_table = None

        # Integer peer ID -> (host, port)
        self.peer_addresses: dict[int, tuple[str, int]] = {}

        # Locally stored key-value data
        self.data_store: dict[str, Any] = {}

        # Separate UDP socket used for multicast discovery
        self._multicast_socket = None

    def handlers(self):
        """Register DHT message handlers."""

        return {
            MessageType.PING: self.on_ping,
            MessageType.FIND_NODE: self.on_find_node,
            MessageType.STORE: self.on_store,
            MessageType.FIND_VALUE: self.on_find_value,
        }

    async def start(self):
        """Initialize DHT and start background discovery tasks."""

        self.routing_table = RoutingTable(self.node.node_id)

        # Existing seed bootstrap
        self.spawn(
            bootstrap(self),
            name="dht-bootstrap",
        )

        # Existing periodic peer refresh
        self.spawn(
            refresh_loop(self),
            name="dht-refresh",
        )

        # Optional UDP multicast discovery
        if self.node.config.multicast_enabled:
            self.spawn(
                multicast_discovery(self),
                name="dht-multicast",
            )

            self.log.info("DHT multicast discovery enabled")

        else:
            self.log.info("DHT multicast discovery disabled")

    async def stop(self):
        """Close the multicast socket during shutdown."""

        multicast_socket = self._multicast_socket

        if multicast_socket is not None:
            multicast_socket.close()
            self._multicast_socket = None

            self.log.info("DHT multicast socket closed")

    # -----------------------------------------
    # PEER MANAGEMENT
    # -----------------------------------------

    def add_peer(self, peer: PeerInfo) -> bool:
        """Add a peer to the routing table."""

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

    def remove_peer(self, peer_id: int) -> bool:
        """Remove a peer and its network address."""

        removed = self.routing_table.remove_peer(peer_id)

        if removed:
            self.peer_addresses.pop(peer_id, None)

        return removed

    def peer_list(self):
        """Return peers in a format suitable for gossip integration."""

        peers = []

        for peer_id, address in self.peer_addresses.items():
            host, port = address

            peers.append(
                {
                    "node_id": node_id_to_hex(peer_id),
                    "address": (host, port),
                }
            )

        return peers

    def get_nearest_peer_infos(
        self,
        target_id: int,
        count: int | None = None,
    ) -> list[PeerInfo]:
        """Return peers ordered by XOR distance."""

        if count is None:
            count = self.node.config.k

        peer_ids = self.routing_table.get_all_peers()

        peer_ids.sort(
            key=lambda peer_id: peer_id ^ target_id
        )

        result = []

        for peer_id in peer_ids[:count]:
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

    # -----------------------------------------
    # PING HANDLER
    # -----------------------------------------

    async def on_ping(self, msg, addr):
        """Handle PING requests and reply with PONG."""

        self.add_peer(
            PeerInfo(
                msg.sender_id,
                addr[0],
                addr[1],
            )
        )

        return msg.reply(
            MessageType.PONG,
            self.node.id_hex,
            None,
        )

    # -----------------------------------------
    # FIND_NODE HANDLER
    # -----------------------------------------

    async def on_find_node(self, msg, addr):
        """Return the closest known peers."""

        # Learn about the requesting peer.
        self.add_peer(
            PeerInfo(
                msg.sender_id,
                addr[0],
                addr[1],
            )
        )

        request = msg.parsed()

        target_id = node_id_from_hex(request.target_id)

        peers = self.get_nearest_peer_infos(target_id)

        return msg.reply(
            MessageType.FIND_NODE_REPLY,
            self.node.id_hex,
            FindNodeReply.from_peers(peers),
        )

    # -----------------------------------------
    # STORE HANDLER
    # -----------------------------------------

    async def on_store(self, msg, addr):
        """Store a key-value pair locally."""

        # Learn about the requesting peer.
        self.add_peer(
            PeerInfo(
                msg.sender_id,
                addr[0],
                addr[1],
            )
        )

        request = msg.parsed()

        self.data_store[request.key] = request.value

        return msg.reply(
            MessageType.STORE_REPLY,
            self.node.id_hex,
            StoreReply(ok=True),
        )

    # -----------------------------------------
    # FIND_VALUE HANDLER
    # -----------------------------------------

    async def on_find_value(self, msg, addr):
        """Return a stored value or nearby peers."""

        # Learn about the requesting peer.
        self.add_peer(
            PeerInfo(
                msg.sender_id,
                addr[0],
                addr[1],
            )
        )

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

    # -----------------------------------------
    # PUBLIC DHT OPERATIONS
    # -----------------------------------------

    async def find_node(self, target_id: int):
        """Find the closest peers to a target ID."""

        return await iterative_find_node(
            self,
            target_id,
        )

    async def store_value(self, key: str, value: Any):
        """Store locally and replicate to closest peers."""

        self.data_store[key] = value

        target_id = generate_node_id(key)

        peers = await self.find_node(target_id)

        results = []

        for peer_id in peers:
            if peer_id not in self.peer_addresses:
                continue

            try:
                stored = await request_store(
                    self,
                    peer_id,
                    key,
                    value,
                )

                results.append(stored)

            except Exception as error:
                self.log.warning(
                    "STORE failed for peer %s: %s",
                    peer_id,
                    error,
                )

                results.append(False)

        return all(results) if results else True

    async def find_value(self, key: str):
        """Find a value locally or on remote peers."""

        if key in self.data_store:
            return self.data_store[key]

        target_id = generate_node_id(key)

        peers = await self.find_node(target_id)

        for peer_id in peers:
            try:
                value = await request_find_value(
                    self,
                    peer_id,
                    key,
                )

                if value is not None:
                    self.data_store[key] = value
                    return value

            except Exception as error:
                self.log.warning(
                    "FIND_VALUE failed for peer %s: %s",
                    peer_id,
                    error,
                )

        return None




  

   
    
       


     
             
