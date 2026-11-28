
"""Select the nearest connected peers using Kademlia XOR distance."""

from meshweaver.protocol import node_id_from_hex
from meshweaver.dht.node_id import xor_distance


def get_nearest_neighbours(local_node_id, peers, count=3, log=None):
    """Return the closest peers to the local node.

    Args:
        local_node_id: Local node ID as a 160-bit integer.
        peers: Connected Peer objects from DHT.
        count: Maximum number of neighbours to return.
        log: MeshWeaver logger used to report skipped peers.
    """

    if isinstance(local_node_id, str):
        local_node_id = node_id_from_hex(local_node_id)

    valid_peers = []

    for peer in peers:
        try:
            peer_id = node_id_from_hex(peer.node_id)

            if peer_id != local_node_id:
                valid_peers.append(peer)

        except AttributeError:
            if log:
                log.event(
                    "gossip.warning",
                    detail="Skipping peer: peer object has no node_id attribute",
                )

        except (ValueError, TypeError) as exc:
            if log:
                log.event(
                    "gossip.warning",
                    detail=f"Skipping peer due to invalid node ID: {exc}",
                )

    nearest = sorted(
        valid_peers,
        key=lambda peer: xor_distance(
            local_node_id,
            node_id_from_hex(peer.node_id),
        ),
    )

    return nearest[:count]
