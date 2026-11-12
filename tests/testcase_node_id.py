from dht.node_id import (
    generate_node_id,
    xor_distance,
    get_bucket_index,
    ID_BITS
)


def test_generate_node_id():
    node_id = generate_node_id("Sahana")

    assert isinstance(node_id, int)
    assert 0 <= node_id < 2 ** ID_BITS


def test_node_id_is_160_bits():
    node_id = generate_node_id("MeshWeaver")

    assert node_id.bit_length() <= 160


def test_same_input_same_id():
    id1 = generate_node_id("Sahana")
    id2 = generate_node_id("Sahana")

    assert id1 == id2


def test_different_input_different_id():
    id1 = generate_node_id("Sahana")
    id2 = generate_node_id("MeshWeaver")

    assert id1 != id2


def test_xor_distance():
    assert xor_distance(10, 20) == 30


def test_xor_distance_same_nodes():
    assert xor_distance(100, 100) == 0


def test_bucket_index():
    assert get_bucket_index(0, 1) == 0
    assert get_bucket_index(0, 2) == 1
    assert get_bucket_index(0, 8) == 3


def test_same_node_bucket_index():
    assert get_bucket_index(100, 100) == -1

