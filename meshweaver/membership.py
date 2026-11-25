"""Basic peer discovery so the runtime and launcher work end to end.

This is the placeholder for member C's Kademlia DHT. It uses the same
message types C's module will handle (PING / PONG and FIND_NODE /
FIND_NODE_REPLY), so swapping it for the real DHT changes no other code.

How a node joins:

1. Every ``peer_refresh_interval`` seconds the node PINGs each seed and each
   known peer. A PONG marks that peer alive and logs ``peer.join`` the first
   time it is seen.
2. It sends FIND_NODE for its own ID to one live peer (or a seed). The reply
   lists the ``k`` peers closest to it by XOR distance; any new ones are pinged
   and join the table once they answer.
3. A peer that misses ``peer_max_misses`` pings in a row is dropped and logged
   as ``peer.leave``.

Only PING senders are added as peers, so a client that only sends FIND_NODE
(like the launcher's monitor) never shows up in anyone's peer table.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Optional

from .node import Module
from .protocol import (
    FindNode,
    FindNodeReply,
    Message,
    MessageType,
    PeerInfo,
    Ping,
    node_id_from_hex,
)
from .transport import Address, TransportError


@dataclass
class Peer:
    node_id: str
    host: str
    port: int
    last_seen: float
    misses: int = 0

    @property
    def address(self) -> Address:
        return (self.host, self.port)

    def info(self) -> PeerInfo:
        return PeerInfo(self.node_id, self.host, self.port)


class Membership(Module):
    name = "membership"

    def __init__(self) -> None:
        super().__init__()
        self.peers: dict[str, Peer] = {}

    # -- module API ----------------------------------------------------------

    def handlers(self):
        return {
            MessageType.PING: self.on_ping,
            MessageType.FIND_NODE: self.on_find_node,
        }

    async def start(self) -> None:
        self.spawn(self._refresh_loop(), name="refresh")

    def status(self) -> dict:
        return {"peers": len(self.peers)}

    def peer_list(self) -> list[Peer]:
        return list(self.peers.values())

    # -- handlers ------------------------------------------------------------

    def on_ping(self, msg: Message, addr: Address) -> Message:
        self._seen(msg.sender_id, addr)
        return msg.reply(MessageType.PONG, self.node.id_hex, Ping(self.node.address[1]))

    def on_find_node(self, msg: Message, addr: Address) -> Message:
        target = node_id_from_hex(msg.parsed().target_id)
        closest = sorted(
            (p for p in self.peers.values() if p.node_id != msg.sender_id),
            key=lambda p: node_id_from_hex(p.node_id) ^ target,
        )[: self.node.config.k]
        return msg.reply(MessageType.FIND_NODE_REPLY, self.node.id_hex,
                         FindNodeReply.from_peers([p.info() for p in closest]))

    # -- background work -----------------------------------------------------

    async def _refresh_loop(self) -> None:
        cfg = self.node.config
        while True:
            await self.refresh()
            # Jitter so 10 nodes started together do not all fire at once.
            await asyncio.sleep(cfg.peer_refresh_interval * random.uniform(0.8, 1.2))

    async def refresh(self) -> None:
        """One round: ping seeds and peers, then look up our own neighbourhood."""
        own = self.node.address
        known = {p.address for p in self.peers.values()}
        targets = [s for s in self.node.config.seeds if s not in known and s != own]
        await asyncio.gather(*(self._ping_new(a) for a in targets),
                             *(self._ping_peer(p) for p in list(self.peers.values())))
        await self._lookup_self()

    async def _ping_new(self, addr: Address) -> None:
        try:
            reply = await self.node.request(addr, MessageType.PING,
                                            Ping(self.node.address[1]))
        except TransportError:
            return
        if reply.type is MessageType.PONG:
            self._seen(reply.sender_id, addr)

    async def _ping_peer(self, peer: Peer) -> None:
        try:
            reply = await self.node.request(peer.address, MessageType.PING,
                                            Ping(self.node.address[1]))
        except TransportError:
            peer.misses += 1
            if peer.misses >= self.node.config.peer_max_misses:
                self._drop(peer, reason="no reply")
            return
        if reply.type is MessageType.PONG:
            self._seen(reply.sender_id, peer.address)

    async def _lookup_self(self) -> None:
        live = [p.address for p in self.peers.values() if p.misses == 0]
        candidates = live or [s for s in self.node.config.seeds if s != self.node.address]
        if not candidates:
            return
        addr = random.choice(candidates)
        try:
            reply = await self.node.request(addr, MessageType.FIND_NODE,
                                            FindNode(self.node.id_hex))
        except TransportError:
            return
        if reply.type is not MessageType.FIND_NODE_REPLY:
            return
        new = [p for p in reply.parsed().peer_infos()
               if p.node_id != self.node.id_hex and p.node_id not in self.peers]
        await asyncio.gather(*(self._ping_new((p.host, p.port)) for p in new))

    # -- peer table ----------------------------------------------------------

    def _seen(self, node_id: str, addr: Address) -> Optional[Peer]:
        if node_id == self.node.id_hex:
            return None
        now = time.time()
        peer = self.peers.get(node_id)
        if peer is None:
            peer = Peer(node_id, addr[0], addr[1], now)
            self.peers[node_id] = peer
            self.log.event("peer.join", peer=node_id, host=addr[0], port=addr[1],
                           total=len(self.peers))
        else:
            peer.last_seen, peer.misses = now, 0
            peer.host, peer.port = addr
        return peer

    def _drop(self, peer: Peer, reason: str) -> None:
        if self.peers.pop(peer.node_id, None) is not None:
            self.log.event("peer.leave", peer=peer.node_id, host=peer.host,
                           port=peer.port, reason=reason, total=len(self.peers))
