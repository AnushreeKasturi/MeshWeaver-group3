"""Integration tests: real nodes running member C's DHT and member D's gossip
together on localhost UDP, through the node runtime's default modules."""

import asyncio
import subprocess
import sys
import time
from pathlib import Path

import pytest

from meshweaver.config import NodeConfig
from meshweaver.node import Node
from meshweaver.protocol import Message, MessageType, Status

ROOT = Path(__file__).resolve().parents[2]

FAST = dict(port=0, rpc_timeout=0.3, peer_refresh_interval=0.2,
            gossip_interval=0.3, stale_after=1.0)


async def wait_until(cond, timeout, step=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        await asyncio.sleep(step)
    return cond()


def peer_ids(node):
    return {p.node_id for p in node.peers()}


def loads(node):
    return node.module("gossip").get_peer_loads()


def full_mesh(nodes):
    ids = {n.id_hex for n in nodes}
    return all(peer_ids(n) == ids - {n.id_hex} for n in nodes)


async def start_mesh(n, **extra):
    """Start node 0, then the rest seeded from it (default DHT + gossip modules)."""
    seed = Node(NodeConfig(name="node-0", **FAST, **extra))
    await seed.start()
    nodes = [seed]
    for i in range(1, n):
        node = Node(NodeConfig(name=f"node-{i}", seeds=[seed.address], **FAST, **extra))
        await node.start()
        nodes.append(node)
    return nodes


async def stop_all(nodes):
    await asyncio.gather(*(n.stop() for n in nodes))


def test_default_modules_are_dht_then_gossip():
    node = Node(NodeConfig(port=0))
    assert [m.name for m in node.modules] == ["dht", "gossip"]


def test_ten_nodes_form_a_full_mesh_through_the_dht():
    async def go():
        nodes = await start_mesh(10)
        try:
            assert await wait_until(lambda: full_mesh(nodes), timeout=10), \
                [len(peer_ids(n)) for n in nodes]
            # Every peer is stored with the address it actually listens on.
            by_id = {n.id_hex: n.address for n in nodes}
            for n in nodes:
                for p in n.peers():
                    assert tuple(p.address) == by_id[p.node_id]
        finally:
            await stop_all(nodes)

    asyncio.run(go())


def test_gossip_load_reports_flow_over_dht_peers():
    async def go():
        nodes = await start_mesh(10)
        try:
            assert await wait_until(lambda: full_mesh(nodes), timeout=10)
            # Each node sends to gossip_fanout (3) neighbours, so 10 nodes
            # deliver 30 reports per round between them.
            assert await wait_until(
                lambda: sum(len(loads(n)) for n in nodes) >= 30, timeout=10), \
                [len(loads(n)) for n in nodes]
            ids = {n.id_hex for n in nodes}
            for n in nodes:
                for node_id, report in loads(n).items():
                    assert node_id in ids - {n.id_hex}
                    assert 0 <= report.cpu_usage <= 100
                    assert report.free_ram_gb > 0
        finally:
            await stop_all(nodes)

    asyncio.run(go())


def test_stopped_nodes_load_goes_stale_everywhere():
    async def go():
        nodes = await start_mesh(6)
        try:
            assert await wait_until(lambda: full_mesh(nodes), timeout=10)
            victim = nodes[-1]
            assert await wait_until(
                lambda: any(victim.id_hex in loads(n) for n in nodes[:-1]), timeout=10)
            nodes.pop()
            await victim.stop()
            assert await wait_until(
                lambda: all(victim.id_hex not in loads(n) for n in nodes), timeout=10)
        finally:
            await stop_all(nodes)

    asyncio.run(go())


@pytest.mark.xfail(reason="member C's Week 3 task: drop peers that stop replying",
                   strict=False)
def test_stopped_node_is_removed_from_peer_tables():
    async def go():
        nodes = await start_mesh(5)
        try:
            assert await wait_until(lambda: full_mesh(nodes), timeout=10)
            victim = nodes.pop(2)
            await victim.stop()
            assert await wait_until(
                lambda: all(victim.id_hex not in peer_ids(n) for n in nodes), timeout=5)
        finally:
            await stop_all(nodes)

    asyncio.run(go())


def test_late_joiner_through_a_non_seed_node_learns_everyone():
    async def go():
        nodes = await start_mesh(4)
        try:
            assert await wait_until(lambda: full_mesh(nodes), timeout=10)
            late = Node(NodeConfig(name="late", seeds=[nodes[3].address], **FAST))
            await late.start()
            nodes.append(late)
            assert await wait_until(lambda: full_mesh(nodes), timeout=10)
        finally:
            await stop_all(nodes)

    asyncio.run(go())


def test_multicast_only_mesh_forms_without_seeds():
    async def go():
        extra = dict(multicast_enabled=True, multicast_port=19981, multicast_interval=0.3)
        nodes = []
        try:
            for i in range(5):
                node = Node(NodeConfig(name=f"mc-{i}", **FAST, **extra))
                await node.start()
                nodes.append(node)
            assert await wait_until(lambda: full_mesh(nodes), timeout=10), \
                [len(peer_ids(n)) for n in nodes]
        finally:
            await stop_all(nodes)

    asyncio.run(go())


def test_status_reports_peers_and_does_not_add_the_asker_as_a_peer():
    async def go():
        nodes = await start_mesh(3)
        outsider = Node(NodeConfig(name="outsider", **FAST), modules=[])
        await outsider.start()
        try:
            assert await wait_until(lambda: full_mesh(nodes), timeout=10)
            reply = await outsider.request(nodes[0].address, MessageType.STATUS, Status())
            assert reply.type is MessageType.STATUS_REPLY
            st = reply.parsed()
            assert st.name == "node-0" and st.state == "running" and st.peers == 2
            assert set(st.modules) == {"dht", "gossip"}
            await asyncio.sleep(0.5)
            assert outsider.id_hex not in peer_ids(nodes[0])
        finally:
            await stop_all(nodes + [outsider])

    asyncio.run(go())


@pytest.mark.parametrize("extra_args", [[], ["--multicast"]], ids=["seed", "multicast"])
def test_launcher_script_forms_mesh(tmp_path, extra_args):
    base = 19400 if not extra_args else 19450
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "launch_mesh.py"),
         "--nodes", "4", "--base-port", str(base), "--log-dir", str(tmp_path),
         "--timeout", "30", "--exit-when-formed", *extra_args],
        cwd=ROOT, capture_output=True, text=True, timeout=90)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Mesh formed" in proc.stdout
    logs = sorted(tmp_path.glob("node-*.log"))
    assert len(logs) == 4
    text = logs[1].read_text(encoding="utf-8")
    assert "module.start" in text and "module=dht" in text and "module=gossip" in text
