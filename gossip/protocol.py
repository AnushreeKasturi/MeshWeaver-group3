"""Gossip payload helpers.

The shared wire protocol is defined in meshweaver.protocol.
"""

from meshweaver.protocol import LoadReport


# Compatibility alias for the earlier Week 1 implementation.
GossipMessage = LoadReport


def create_gossip_message(node_id, load_info):
    """Create a shared MeshWeaver LoadReport."""

    return LoadReport(
        node_id=node_id,
        cpu_usage=load_info["cpu_usage"],
        free_ram_gb=load_info["free_ram_gb"],
    )
