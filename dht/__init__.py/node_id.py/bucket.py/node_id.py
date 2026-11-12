import hashlib

ID_BITS = 160


def generate_node_id(value):
    """Generate a 160-bit node ID using SHA-1."""
    data = str(value).encode("utf-8")
    hash_value = hashlib.sha1(data).hexdigest()
    return int(hash_value, 16)


def xor_distance(node_id1, node_id2):
    """Calculate XOR distance between two nodes."""
    return node_id1 ^ node_id2


def get_bucket_index(local_id, remote_id):
    """Calculate the Kademlia bucket index."""
    distance = xor_distance(local_id, remote_id)

    if distance == 0:
        return -1

    return distance.bit_length() - 1


# Main program to display output
if __name__ == "__main__":

    print("========== MeshWeaver Node ID ==========")

    node1 = generate_node_id("Sahana")
    node2 = generate_node_id("MeshWeaver")

    print("\nNode 1 Name: Sahana")
    print("Node 1 ID:", node1)

    print("\nNode 2 Name: MeshWeaver")
    print("Node 2 ID:", node2)

    distance = xor_distance(node1, node2)

    print("\nXOR Distance:", distance)

    bucket_index = get_bucket_index(node1, node2)

    print("Bucket Index:", bucket_index)

    print("\n========== Execution Completed ==========")

