"""Kademlia DHT and peer discovery (member C).

Node IDs are 160-bit ints internally; on the wire use
``protocol.node_id_to_hex`` / ``node_id_from_hex``. Handles PING, FIND_NODE,
FIND_VALUE and STORE through the dispatcher.
"""
