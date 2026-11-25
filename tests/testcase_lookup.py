
import asyncio
from types import SimpleNamespace

from meshweaver.dht.node import DHT
from meshweaver.dht.routing_table import RoutingTable
from meshweaver.protocol import PeerInfo, node_id_to_hex
import meshweaver.dht.lookup as lookup_module


def make_dht():
    dht = DHT()

    dht.node = SimpleNamespace(
        node_id=100,
        id_hex=node_id_to_hex(100),
        config=SimpleNamespace(k=5, rpc_timeout=2.0),
    )

    dht.routing_table = RoutingTable(100)

    return dht


def add_peer(dht, peer_id):
    peer = PeerInfo(
        node_id=node_id_to_hex(peer_id),
        host="127.0.0.1",
        port=9000 + peer_id,
    )

    dht.add_peer(peer)


def test_lookup_returns_closest_peers(monkeypatch):
    dht = make_dht()

    add_peer(dht, 200)
    add_peer(dht, 150)
    add_peer(dht, 300)

    async def fake_request(dht_instance, peer_id, target_id):
        return []

    monkeypatch.setattr(
        lookup_module,
        "request_find_node",
        fake_request,
    )

    result = asyncio.run(
        lookup_module.iterative_find_node(dht, 180)
    )

    expected = sorted(
        [200, 150, 300],
        key=lambda peer_id: peer_id ^ 180,
    )

    assert result == expected


def test_lookup_discovers_new_peer(monkeypatch):
    dht = make_dht()

    add_peer(dht, 200)

    discovered_peer = PeerInfo(
        node_id=node_id_to_hex(150),
        host="127.0.0.1",
        port=9150,
    )

    async def fake_request(dht_instance, peer_id, target_id):
        if peer_id == 200:
            return [discovered_peer]

        return []

    monkeypatch.setattr(
        lookup_module,
        "request_find_node",
        fake_request,
    )

    result = asyncio.run(
        lookup_module.iterative_find_node(dht, 180)
    )

    assert 150 in result
    assert 150 in dht.peer_addresses


def test_lookup_handles_peer_failure(monkeypatch):
    dht = make_dht()
    add_peer(dht, 200)

    async def fake_request(dht_instance, peer_id, target_id):
        raise TimeoutError("Peer unavailable")

    monkeypatch.setattr(
        lookup_module,
        "request_find_node",
        fake_request,
    )

    result = asyncio.run(
        lookup_module.iterative_find_node(dht, 180)
    )

    assert result == [200]


def test_lookup_returns_empty_when_no_peers():
    dht = make_dht()

    result = asyncio.run(
        lookup_module.iterative_find_node(dht, 180)
    )

    assert result == []
