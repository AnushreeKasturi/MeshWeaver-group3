
"""Iterative Kademlia node lookup."""

import asyncio

from .rpc import request_find_node
from ..protocol import node_id_from_hex


async def iterative_find_node(dht, target_id: int):
    """Find the k closest known peers to a target ID."""

    k = dht.node.config.k
    alpha = min(3, k)

    queried = set()

    while True:
        candidates = dht.routing_table.get_all_peers()

        candidates.sort(
            key=lambda peer_id: peer_id ^ target_id
        )

        closest = candidates[:k]

        unqueried = [
            peer_id
            for peer_id in closest
            if peer_id not in queried
        ]

        if not unqueried:
            break

        batch = unqueried[:alpha]
        queried.update(batch)

        results = await asyncio.gather(
            *[
                request_find_node(dht, peer_id, target_id)
                for peer_id in batch
            ],
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, Exception):
                continue

            for peer in result:
                try:
                    peer_id = node_id_from_hex(peer.node_id)
                    dht.add_peer(peer)
                except (ValueError, TypeError, AttributeError):
                    continue

    candidates = dht.routing_table.get_all_peers()

    candidates.sort(
        key=lambda peer_id: peer_id ^ target_id
    )

    return candidates[:k]
