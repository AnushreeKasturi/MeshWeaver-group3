from net.message import create_message
from net.transport import send_rpc


def create_rpc_payload(node_info, extra=None):

    payload = {
        "sender": {
            "node_id": node_info["node_id"],
            "ip_address": node_info["ip_address"],
            "port": node_info["port"]
        }
    }

    if extra:
        payload.update(extra)

    return payload


async def ping(peer, local_node):

    message = create_message(
        "PING",
        local_node["node_id"],
        create_rpc_payload(local_node)
    )

    return await send_rpc(peer, message)


async def store(peer, local_node, key, value):

    message = create_message(
        "STORE",
        local_node["node_id"],
        create_rpc_payload(
            local_node,
            {
                "key": key,
                "value": value
            }
        )
    )

    return await send_rpc(peer, message)


async def find_node(peer, local_node, target_node_id):

    message = create_message(
        "FIND_NODE",
        local_node["node_id"],
        create_rpc_payload(
            local_node,
            {
                "target_node_id": target_node_id
            }
        )
    )

    return await send_rpc(peer, message)


async def find_value(peer, local_node, key):

    message = create_message(
        "FIND_VALUE",
        local_node["node_id"],
        create_rpc_payload(
            local_node,
            {
                "key": key
            }
        )
    )

    return await send_rpc(peer, message)
