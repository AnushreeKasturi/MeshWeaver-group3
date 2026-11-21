import asyncio
import hashlib

from dht.rpc import find_node, find_value
from dht.routing import xor_distance


ALPHA = 3
K = 20
MAX_ROUNDS = 20


class IterativeLookup:

    def __init__(self, dht_node):

        self.dht_node = dht_node
        self.local_node = dht_node.node_info

    async def find_closest_nodes(self, target_id):

        routing = self.dht_node.routing_table

        shortlist = routing.get_nearest_peers(
            target_id=target_id,
            count=K
        )

        contacted = set()

        for round_number in range(MAX_ROUNDS):

            shortlist.sort(
                key=lambda peer: xor_distance(
                    target_id,
                    peer["node_id"]
                )
            )

            candidates = [
                peer for peer in shortlist
                if peer["node_id"] not in contacted
            ][:ALPHA]

            if not candidates:
                break

            for peer in candidates:
                contacted.add(peer["node_id"])

            results = await asyncio.gather(
                *[
                    find_node(
                        peer,
                        self.local_node,
                        target_id
                    )
                    for peer in candidates
                ],
                return_exceptions=True
            )

            previous_closest = [
                peer["node_id"] for peer in shortlist[:K]
            ]

            for result in results:

                if isinstance(result, Exception):
                    continue

                if result.get("type") != "FIND_NODE_RESPONSE":
                    continue

                new_peers = result.get(
                    "payload", {}
                ).get("nodes", [])

                for peer in new_peers:

                    if peer["node_id"] == self.dht_node.node_id:
                        continue

                    self.dht_node.add_peer(peer)

                    if peer["node_id"] not in {
                        item["node_id"] for item in shortlist
                    }:
                        shortlist.append(peer)

            shortlist.sort(
                key=lambda peer: xor_distance(
                    target_id,
                    peer["node_id"]
                )
            )

            shortlist = shortlist[:K]

            current_closest = [
                peer["node_id"] for peer in shortlist
            ]

            # Stop if the closest known set has not changed
            if current_closest == previous_closest:
                if not any(
                    peer["node_id"] not in contacted
                    for peer in shortlist
                ):
                    break

        return shortlist

    async def find_value_iterative(self, key):

        target_id = hashlib.sha1(
            key.encode("utf-8")
        ).hexdigest()

        routing = self.dht_node.routing_table

        shortlist = routing.get_nearest_peers(
            target_id=target_id,
            count=K
        )

        contacted = set()

        for _ in range(MAX_ROUNDS):

            shortlist.sort(
                key=lambda peer: xor_distance(
                    target_id,
                    peer["node_id"]
                )
            )

            candidates = [
                peer for peer in shortlist
                if peer["node_id"] not in contacted
            ][:ALPHA]

            if not candidates:
                break

            for peer in candidates:
                contacted.add(peer["node_id"])

            results = await asyncio.gather(
                *[
                    find_value(
                        peer,
                        self.local_node,
                        key
                    )
                    for peer in candidates
                ],
                return_exceptions=True
            )

            for result in results:

                if isinstance(result, Exception):
                    continue

                if result.get("type") != "FIND_VALUE_RESPONSE":
                    continue

                payload = result.get("payload", {})

                if payload.get("found"):
                    return payload.get("value")

                for peer in payload.get("nodes", []):

                    if peer["node_id"] == self.dht_node.node_id:
                        continue

                    self.dht_node.add_peer(peer)

                    if peer["node_id"] not in {
                        item["node_id"] for item in shortlist
                    }:
                        shortlist.append(peer)

            shortlist.sort(
                key=lambda peer: xor_distance(
                    target_id,
                    peer["node_id"]
                )
            )

            shortlist = shortlist[:K]

        return None
