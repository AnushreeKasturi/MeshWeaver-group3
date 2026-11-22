"""Select the nearest connected peers using Kademlia XOR distance."""

from meshweaver.protocol import node_id_from_hex
from meshweaver.dht.node_id import xor_distance


def get_nearest_neighbours(local_node_id, peers, count=3):
    """Return the closest peers to the local node.

    Args:
        local_node_id: Local node ID as a 160-bit integer.
        peers: Connected Peer objects from Membership.
        count: Maximum number of neighbours to return.
    """

    if isinstance(local_node_id, str):
        local_node_id = node_id_from_hex(local_node_id)

    valid_peers = []

    for peer in peers:
        try:
            peer_id = node_id_from_hex(peer.node_id)

            if peer_id != local_node_id:
                valid_peers.append(peer)

        except (ValueError, AttributeError):
            continue

    nearest = sorted(
        valid_peers,
        key=lambda peer: xor_distance(
            local_node_id,
            node_id_from_hex(peer.node_id),
        ),
    )

    return nearest[:count]
