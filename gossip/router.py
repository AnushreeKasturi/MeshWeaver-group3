
"""Select the best peer for task execution based on system load."""

import time


def select_best_peer(peer_loads, stale_after=15.0, now=None):
    """Select the least-loaded peer, skipping stale or invalid records.

    Selection rules:
    1. Skip stale or invalid peer records.
    2. Select the peer with the lowest CPU usage.
    3. Use higher free RAM as the tie-breaker.

    Args:
        peer_loads: Iterable of peer load records.
        stale_after: Maximum age of a load report in seconds.
        now: Current timestamp, useful for testing.

    Returns:
        The selected peer record, or None if no eligible peer exists.
    """

    if now is None:
        now = time.time()

    eligible_peers = []

    for peer in peer_loads:
        try:
            if isinstance(peer, dict):
                node_id = peer["node_id"]
                cpu_usage = float(peer["cpu_usage"])
                free_ram_gb = float(peer["free_ram_gb"])
                timestamp = float(peer["timestamp"])
            else:
                node_id = peer.node_id
                cpu_usage = float(peer.cpu_usage)
                free_ram_gb = float(peer.free_ram_gb)
                timestamp = float(peer.timestamp)

            age = now - timestamp

            if age < 0 or age > stale_after:
                continue

            if not node_id:
                continue

            if not 0 <= cpu_usage <= 100:
                continue

            if free_ram_gb < 0:
                continue

            eligible_peers.append(
                (cpu_usage, -free_ram_gb, peer)
            )

        except (KeyError, AttributeError, TypeError, ValueError):
            continue

    if not eligible_peers:
        return None

    eligible_peers.sort(key=lambda item: (item[0], item[1]))

    return eligible_peers[0][2]


def select_best_peer_from_store(peer_store):
    """Select the best peer using the existing PeerLoadStore."""

    fresh_peers = peer_store.get_fresh_peers()

    return select_best_peer(fresh_peers)
