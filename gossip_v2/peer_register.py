
def xor_distance(node_id1, node_id2):
    return int(node_id1, 16) ^ int(node_id2, 16)


def get_nearest_neighbours(
    local_node_id,
    routing_table,
    peer_directory,
    count=3
):

    peer_ids = routing_table.get_all_peers()

    available_peers = []

    for peer_id in peer_ids:

        if peer_id == local_node_id:
            continue

        peer = peer_directory.get(peer_id)

        if peer is not None:
            available_peers.append(peer)

    
    nearest = sorted(
        available_peers,
        key=lambda peer: xor_distance(
            local_node_id,
            peer["node_id"]
        )
    )

    return nearest[:count]
    if __name__ == "__main__":

    node_ids = [
        "1111111111111111111111111111111111111111",
        "2222222222222222222222222222222222222222",
        "3333333333333333333333333333333333333333"
    ]

    local_node_id = node_ids[0]

    routing_table = type("RoutingTable", (), {
        "get_all_peers": lambda self: node_ids[1:]
    })()

    peer_directory = {
        node_id: {
            "node_id": node_id,
            "ip_address": "127.0.0.1",
            "port": 8000 + i
        }
        for i, node_id in enumerate(node_ids[1:], start=2)
    }

    neighbours = get_nearest_neighbours(
        local_node_id,
        routing_table,
        peer_directory,
        count=2
    )

    print("Nearest neighbours:")
    for peer in neighbours:
        print(peer)
