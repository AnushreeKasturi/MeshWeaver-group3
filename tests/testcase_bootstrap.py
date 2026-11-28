import asyncio
import json
import socket
from types import SimpleNamespace

import pytest

from meshweaver.dht import bootstrap as bootstrap_module
from meshweaver.dht.node import DHT
from meshweaver.dht.routing_table import RoutingTable
from meshweaver.protocol import node_id_to_hex


TEST_MULTICAST_GROUP = "239.255.42.99"
TEST_MULTICAST_PORT = 10099


def make_dht():
    dht = DHT()

    dht.node = SimpleNamespace(
        node_id=100,
        id_hex=node_id_to_hex(100),
        address=("127.0.0.1", 9000),
        config=SimpleNamespace(
            seeds=[("127.0.0.1", 9001)],
            port=9000,
            rpc_timeout=2.0,
            peer_refresh_interval=2.0,
            multicast_enabled=True,
            multicast_group=TEST_MULTICAST_GROUP,
            multicast_port=TEST_MULTICAST_PORT,
            multicast_interval=0.2,
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


def make_multicast_dht(node_id, port):
    dht = DHT()

    dht.node = SimpleNamespace(
        node_id=node_id,
        id_hex=node_id_to_hex(node_id),
        address=("127.0.0.1", port),
        config=SimpleNamespace(
            seeds=[],
            port=port,
            rpc_timeout=2.0,
            peer_refresh_interval=2.0,
            multicast_enabled=True,
            multicast_group=TEST_MULTICAST_GROUP,
            multicast_port=TEST_MULTICAST_PORT,
            multicast_interval=0.2,
            k=20,
        ),
        request=None,
    )

    dht.routing_table = RoutingTable(node_id)

    dht.log = SimpleNamespace(
        info=lambda *args: None,
        warning=lambda *args: None,
    )

    return dht


async def wait_for_peers(dhts, expected_count, timeout=5.0):
    """Wait until every DHT discovers the expected number of peers."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        if all(
            dht.routing_table.get_peer_count() >= expected_count
            for dht in dhts
        ):
            return

        await asyncio.sleep(0.1)

    counts = [
        dht.routing_table.get_peer_count()
        for dht in dhts
    ]

    raise AssertionError(
        f"Expected {expected_count} peers per node, got {counts}"
    )


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


def test_multicast_two_nodes_discover_each_other(monkeypatch):
    """Two nodes with no seeds discover one another using multicast."""

    dht1 = make_multicast_dht(101, 9101)
    dht2 = make_multicast_dht(102, 9102)

    async def fake_lookup(dht_instance, target_id):
        return []

    monkeypatch.setattr(
        bootstrap_module,
        "iterative_find_node",
        fake_lookup,
    )

    async def run_test():
        tasks = [
            asyncio.create_task(
                bootstrap_module.multicast_discovery(dht1)
            ),
            asyncio.create_task(
                bootstrap_module.multicast_discovery(dht2)
            ),
        ]

        try:
            await wait_for_peers([dht1, dht2], 1)

            assert 102 in dht1.routing_table.get_all_peers()
            assert 101 in dht2.routing_table.get_all_peers()

        finally:
            for dht in [dht1, dht2]:
                await dht.stop()

            for task in tasks:
                task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

    asyncio.run(run_test())


def test_multicast_ignores_own_announcement(monkeypatch):
    """A node must not add itself after receiving its own announcement."""

    dht = make_multicast_dht(103, 9103)

    async def fake_lookup(dht_instance, target_id):
        return []

    monkeypatch.setattr(
        bootstrap_module,
        "iterative_find_node",
        fake_lookup,
    )

    async def run_test():
        task = asyncio.create_task(
            bootstrap_module.multicast_discovery(dht)
        )

        try:
            await asyncio.sleep(0.5)

            assert dht.routing_table.get_peer_count() == 0

        finally:
            await dht.stop()
            task.cancel()

            await asyncio.gather(
                task,
                return_exceptions=True,
            )

    asyncio.run(run_test())


@pytest.mark.parametrize(
    "announcement",
    [
        b"not-json",
        json.dumps(
            {"host": "127.0.0.1", "port": 9104}
        ).encode(),
    ],
)
def test_multicast_ignores_invalid_announcement(
    monkeypatch,
    announcement,
):
    """Malformed announcements must not crash multicast discovery."""

    dht = make_multicast_dht(104, 9104)

    async def fake_lookup(dht_instance, target_id):
        return []

    monkeypatch.setattr(
        bootstrap_module,
        "iterative_find_node",
        fake_lookup,
    )

    async def run_test():
        task = asyncio.create_task(
            bootstrap_module.multicast_discovery(dht)
        )

        sender = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        )

        try:
            await asyncio.sleep(0.3)

            sender.sendto(
                announcement,
                (
                    TEST_MULTICAST_GROUP,
                    TEST_MULTICAST_PORT,
                ),
            )

            await asyncio.sleep(0.5)

            assert not task.done()
            assert dht.routing_table.get_peer_count() == 0

        finally:
            sender.close()
            await dht.stop()
            task.cancel()

            await asyncio.gather(
                task,
                return_exceptions=True,
            )

    asyncio.run(run_test())


def test_multicast_socket_closed_after_stop(monkeypatch):
    """Stopping DHT closes its multicast socket and clears the reference."""

    dht = make_multicast_dht(105, 9105)

    async def fake_lookup(dht_instance, target_id):
        return []

    monkeypatch.setattr(
        bootstrap_module,
        "iterative_find_node",
        fake_lookup,
    )

    async def run_test():
        task = asyncio.create_task(
            bootstrap_module.multicast_discovery(dht)
        )

        try:
            for _ in range(30):
                if dht._multicast_socket is not None:
                    break

                await asyncio.sleep(0.1)

            assert dht._multicast_socket is not None

            await dht.stop()

            assert dht._multicast_socket is None

        finally:
            await dht.stop()
            task.cancel()

            await asyncio.gather(
                task,
                return_exceptions=True,
            )

    asyncio.run(run_test())


def test_peer_list_returns_peer_objects():
    """Peer list must expose node_id and address as object attributes."""

    dht = DHT()

    dht.peer_addresses[200] = ("127.0.0.1", 9001)

    peers = dht.peer_list()

    assert len(peers) == 1

    peer = peers[0]

    assert peer.node_id == node_id_to_hex(200)
    assert peer.address == ("127.0.0.1", 9001)


def test_refresh_loop_retries_seed_when_seed_starts_late(monkeypatch):
    """A node should discover its seed when the seed starts later."""

    dht = make_dht()
    seed_ready = False

    async def fake_request(*args, **kwargs):
        if not seed_ready:
            raise ConnectionError("Seed is not running yet")

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

    async def run_test():
        nonlocal seed_ready

        # First, bootstrap while the seed is offline.
        await bootstrap_module.bootstrap(dht)

        assert dht.routing_table.get_peer_count() == 0

        # The seed starts after the node's initial bootstrap attempt.
        seed_ready = True

        sleep_calls = 0

        async def fake_sleep(interval):
            nonlocal sleep_calls

            assert interval == 2.0
            sleep_calls += 1

            # Allow one refresh cycle to run.
            if sleep_calls > 1:
                raise asyncio.CancelledError

        monkeypatch.setattr(
            bootstrap_module.asyncio,
            "sleep",
            fake_sleep,
        )

        # Run refresh loop until the fake sleep cancels it.
        try:
            await bootstrap_module.refresh_loop(dht)
        except asyncio.CancelledError:
            pass

        # The refresh loop should have discovered the late seed.
        assert 200 in dht.routing_table.get_all_peers()
        assert dht.peer_addresses[200] == ("127.0.0.1", 9001)

    asyncio.run(run_test())



             
