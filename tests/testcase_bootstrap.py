
import asyncio
from types import SimpleNamespace

from meshweaver.dht import bootstrap as bootstrap_module
from meshweaver.dht.node import DHT
from meshweaver.dht.routing_table import RoutingTable
from meshweaver.protocol import node_id_to_hex


def make_dht():
    dht = DHT()

    dht.node = SimpleNamespace(
        node_id=100,
        id_hex=node_id_to_hex(100),
        config=SimpleNamespace(
            seeds=[("127.0.0.1", 9001)],
            port=9000,
            rpc_timeout=2.0,
            peer_refresh_interval=2.0,
            k=20,
        ),
        request=None,
    )

    dht.routing_table = RoutingTable(100)

    dht.log = SimpleNamespace(
        info=lambda *args: None,
        warning=lambda *args: None,
    )

    return dht


def test_bootstrap_connects_to_seed(monkeypatch):
    dht = make_dht()

    async def fake_request(*args, **kwargs):
        return SimpleNamespace(
            sender_id=node_id_to_hex(200)
        )

    dht.node.request = fake_request

    async def fake_lookup(dht_instance, target_id):
        return []

    monkeypatch.setattr(
        bootstrap_module,
        "iterative_find_node",
        fake_lookup,
    )

    asyncio.run(bootstrap_module.bootstrap(dht))

    assert 200 in dht.routing_table.get_all_peers()
    assert dht.peer_addresses[200] == ("127.0.0.1", 9001)


def test_bootstrap_handles_seed_failure(monkeypatch):
    dht = make_dht()

    async def fake_request(*args, **kwargs):
        raise TimeoutError("Seed unavailable")

    dht.node.request = fake_request

    async def fake_lookup(dht_instance, target_id):
        return []

    monkeypatch.setattr(
        bootstrap_module,
        "iterative_find_node",
        fake_lookup,
    )

    asyncio.run(bootstrap_module.bootstrap(dht))

    assert dht.routing_table.get_peer_count() == 0


def test_bootstrap_uses_configured_seed():
    dht = make_dht()

    assert dht.node.config.seeds == [
        ("127.0.0.1", 9001)
    ]


def test_refresh_loop_uses_configured_interval(monkeypatch):
    dht = make_dht()

    calls = []

    async def fake_lookup(dht_instance, target_id):
        calls.append(target_id)
        raise asyncio.CancelledError

    async def fake_sleep(interval):
        assert interval == 2.0

    monkeypatch.setattr(
        bootstrap_module,
        "iterative_find_node",
        fake_lookup,
    )

    monkeypatch.setattr(
        bootstrap_module.asyncio,
        "sleep",
        fake_sleep,
    )

    try:
        asyncio.run(bootstrap_module.refresh_loop(dht))
    except asyncio.CancelledError:
        pass

    assert calls == [100]
