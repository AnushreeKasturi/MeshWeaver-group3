
import hashlib
def generate_node_id(node_name):
    """Generate a 160-bit node ID using SHA-1."""
    return hashlib.sha1(node_name.encode()).hexdigest()


def create_sample_nodes():

    nodes = [
        {
            "node_id": generate_node_id("node_1"),
            "ip_address": "127.0.0.1",
            "port": 8001
        },
        {
            "node_id": generate_node_id("node_2"),
            "ip_address": "127.0.0.1",
            "port": 8002
        },
        {
            "node_id": generate_node_id("node_3"),
            "ip_address": "127.0.0.1",
            "port": 8003
        },
        {
            "node_id": generate_node_id("node_4"),
            "ip_address": "127.0.0.1",
            "port": 8004
        }
    ]

    return nodes


def xor_distance(node_id1, node_id2):
    return int(node_id1, 16) ^ int(node_id2, 16)


def get_nearest_neighbours(local_node_id, nodes, count=3):

    other_nodes = [
        node for node in nodes
        if node["node_id"] != local_node_id
    ]

    nearest = sorted(
        other_nodes,
        key=lambda node: xor_distance(
            local_node_id, node["node_id"]
        )
    )

    return nearest[:count]


if __name__ == "__main__":

    sample_nodes = create_sample_nodes()
    local_node = sample_nodes[0]

    neighbours = get_nearest_neighbours(
        local_node["node_id"],
        sample_nodes
    )

    print("Local Node ID:", local_node["node_id"])
    print("\nThree Nearest Neighbours:")

    for peer in neighbours:
        print(peer)
