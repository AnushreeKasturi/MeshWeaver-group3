"""Integration tests: many real nodes on localhost UDP forming a mesh."""

import asyncio
import subprocess
import sys
import time
from pathlib import Path

from meshweaver.config import NodeConfig
from meshweaver.node import Node

ROOT = Path(__file__).resolve().parents[2]

FAST = dict(port=0, rpc_timeout=0.3, peer_refresh_interval=0.2, peer_max_misses=3)


async def wait_until(cond, timeout, step=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        await asyncio.sleep(step)
    return cond()


def peer_ids(node):
    return set(node.module("membership").peers)


async def start_mesh(n):
    seed = Node(NodeConfig(name="node-0", **FAST))
    await seed.start()
    nodes = [seed]
    for i in range(1, n):
        node = Node(NodeConfig(name=f"node-{i}", seeds=[seed.address], **FAST))
        await node.start()
        nodes.append(node)
    return nodes


def test_ten_nodes_form_a_full_mesh():
    async def go():
        nodes = await start_mesh(10)
        try:
            ids = {n.id_hex for n in nodes}
            formed = await wait_until(
                lambda: all(peer_ids(n) == ids - {n.id_hex} for n in nodes), timeout=10)
            assert formed, [len(peer_ids(n)) for n in nodes]
            # Peers are stored with the address they actually listen on.
            by_id = {n.id_hex: n.address for n in nodes}
            for n in nodes:
                for p in n.module("membership").peer_list():
                    assert p.address == by_id[p.node_id]
        finally:
            await asyncio.gather(*(n.stop() for n in nodes))

    asyncio.run(go())


def test_dead_node_is_dropped_and_rest_keep_working():
    async def go():
        nodes = await start_mesh(5)
        try:
            ids = {n.id_hex for n in nodes}
            assert await wait_until(
                lambda: all(peer_ids(n) == ids - {n.id_hex} for n in nodes), timeout=10)
            victim = nodes.pop(2)
            await victim.stop()
            dropped = await wait_until(
                lambda: all(victim.id_hex not in peer_ids(n) for n in nodes), timeout=10)
            assert dropped
            # Survivors still see each other.
            alive = ids - {victim.id_hex}
            assert all(peer_ids(n) == alive - {n.id_hex} for n in nodes)
        finally:
            await asyncio.gather(*(n.stop() for n in nodes))

    asyncio.run(go())


def test_late_joiner_learns_peers_it_never_pinged():
    async def go():
        nodes = await start_mesh(4)
        try:
            ids = {n.id_hex for n in nodes}
            assert await wait_until(
                lambda: all(peer_ids(n) == ids - {n.id_hex} for n in nodes), timeout=10)
            # Joins through node-3 only, not the original seed.
            late = Node(NodeConfig(name="late", seeds=[nodes[3].address], **FAST))
            await late.start()
            nodes.append(late)
            assert await wait_until(lambda: peer_ids(late) == ids, timeout=10)
            assert await wait_until(
                lambda: all(late.id_hex in peer_ids(n) for n in nodes[:-1]), timeout=10)
        finally:
            await asyncio.gather(*(n.stop() for n in nodes))

    asyncio.run(go())


def test_launcher_script_forms_mesh(tmp_path):
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "launch_mesh.py"),
         "--nodes", "4", "--base-port", "19400", "--log-dir", str(tmp_path),
         "--timeout", "30", "--exit-when-formed"],
        cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Mesh formed" in proc.stdout
    logs = sorted(tmp_path.glob("node-*.log"))
    assert len(logs) == 4
    assert "peer.join" in logs[1].read_text(encoding="utf-8")
