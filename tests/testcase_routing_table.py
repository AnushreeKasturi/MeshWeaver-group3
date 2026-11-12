import pytest

from dht.routing_table import RoutingTable


def test_routing_table_creation():
    table = RoutingTable(100)
    assert len(table.buckets) == 160


def test_add_peer():
    table = RoutingTable(100)
    assert table.add_peer(200) is True
    assert 200 in table.get_all_peers()


def test_add_multiple_peers():
    table = RoutingTable(100)

    table.add_peer(200)
    table.add_peer(300)
    table.add_peer(400)

    assert table.get_peer_count() == 3


def test_ignore_local_node():
    table = RoutingTable(100)

    assert table.add_peer(100) is False
    assert table.get_all_peers() == []


def test_get_peer_count():
    table = RoutingTable(100)

    table.add_peer(200)
    table.add_peer(300)

    assert table.get_peer_count() == 2


def test_remove_peer():
    table = RoutingTable(100)

    table.add_peer(200)

    assert table.remove_peer(200) is True
    assert 200 not in table.get_all_peers()


def test_remove_missing_peer():
    table = RoutingTable(100)

    assert table.remove_peer(200) is False


def test_get_bucket():
    table = RoutingTable(100)

    assert table.get_bucket(0) is not None


def test_invalid_bucket():
    table = RoutingTable(100)

    with pytest.raises(IndexError):
        table.get_bucket(160)


def test_duplicate_peer():
    table = RoutingTable(100)

    table.add_peer(200)
    table.add_peer(200)

    assert table.get_peer_count() == 1

