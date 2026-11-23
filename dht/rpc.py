
"""Remote procedure calls for the MeshWeaver DHT."""

from ..protocol import (
    MessageType,
    FindNode,
    FindNodeReply,
    FindValue,
    FindValueReply,
    Store,
    StoreReply,
    node_id_to_hex,
)


async def request_find_node(dht, peer_id: int, target_id: int):
    """Request the closest peers to a target node ID."""

    address = dht.peer_addresses.get(peer_id)

    if address is None:
        return []

    response = await dht.node.request(
        address,
        MessageType.FIND_NODE,
        FindNode(target_id=node_id_to_hex(target_id)),
        timeout=dht.node.config.rpc_timeout,
    )

    result = response.parsed()

    if not isinstance(result, FindNodeReply):
        return []

    return result.peer_infos()


async def request_store(dht, peer_id: int, key: str, value):
    """Store a key-value pair on a remote peer."""

    address = dht.peer_addresses.get(peer_id)

    if address is None:
        return False

    response = await dht.node.request(
        address,
        MessageType.STORE,
        Store(key=key, value=value),
        timeout=dht.node.config.rpc_timeout,
    )

    result = response.parsed()

    return isinstance(result, StoreReply) and result.ok


async def request_find_value(dht, peer_id: int, key: str):
    """Request a value from a remote peer."""

    address = dht.peer_addresses.get(peer_id)

    if address is None:
        return None

    response = await dht.node.request(
        address,
        MessageType.FIND_VALUE,
        FindValue(key=key),
        timeout=dht.node.config.rpc_timeout,
    )

    result = response.parsed()

    if not isinstance(result, FindValueReply):
        return None

    if result.found:
        return result.value

    return None
