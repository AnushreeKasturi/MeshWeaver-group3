"""Start a MeshWeaver mesh of N nodes on one machine and watch it form.

    python scripts/launch_mesh.py                 # 10 nodes on ports 9000-9009
    python scripts/launch_mesh.py --nodes 20 --base-port 9100
    python scripts/launch_mesh.py --exit-when-formed   # stop once all are connected

Node 0 is the seed; every other node starts with ``--seed`` pointing at it.
Each node runs as its own process (so any one can be killed to test failure)
and writes its log to ``logs/node-<i>.log``.

While the mesh runs, the launcher asks every node for its peer list once a
second (a FIND_NODE request) and prints a table. When every node knows all
the others it prints how long the mesh took to form. Ctrl+C stops all nodes.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from meshweaver.logs import NodeLogger, setup_logging  # noqa: E402
from meshweaver.protocol import (  # noqa: E402
    Dispatcher, FindNode, Message, MessageType, node_id_to_hex,
)
from meshweaver.transport import TransportError, UdpTransport  # noqa: E402

MONITOR_ID = node_id_to_hex(0)  # all-zero ID marks the launcher's monitor


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Launch a local MeshWeaver mesh.")
    p.add_argument("--nodes", type=int, default=10, help="number of nodes (default 10)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--base-port", type=int, default=9000)
    p.add_argument("--log-dir", default=str(ROOT / "logs"))
    p.add_argument("--log-json", action="store_true", help="nodes log JSON lines")
    p.add_argument("--timeout", type=float, default=60.0,
                   help="seconds to wait for the mesh to form (default 60)")
    p.add_argument("--exit-when-formed", action="store_true",
                   help="stop all nodes as soon as the mesh has formed")
    args = p.parse_args(argv)
    if args.nodes < 2:
        p.error("--nodes must be at least 2")
    return args


def start_nodes(args: argparse.Namespace) -> list[subprocess.Popen]:
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    seed = f"{args.host}:{args.base_port}"
    procs = []
    for i in range(args.nodes):
        cmd = [sys.executable, "-m", "meshweaver.node",
               "--name", f"node-{i}", "--host", args.host,
               "--port", str(args.base_port + i),
               "--log-file", str(log_dir / f"node-{i}.log")]
        if i > 0:
            cmd += ["--seed", seed]
        if args.log_json:
            cmd.append("--log-json")
        procs.append(subprocess.Popen(cmd, cwd=ROOT, env=env,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    return procs


def stop_nodes(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            p.terminate()
    deadline = time.time() + 5
    for p in procs:
        try:
            p.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            p.kill()


async def peer_counts(monitor: UdpTransport, addrs: list[tuple[str, int]]) -> list[int | None]:
    """Ask every node for its peers; ``None`` means the node did not answer."""

    async def ask(addr):
        msg = Message.create(MessageType.FIND_NODE, MONITOR_ID, FindNode(MONITOR_ID))
        try:
            reply = await monitor.request(addr, msg, timeout=1.0)
        except TransportError:
            return None
        if reply.type is not MessageType.FIND_NODE_REPLY:
            return None
        return len(reply.parsed().peers)

    return list(await asyncio.gather(*(ask(a) for a in addrs)))


def render(counts: list[int | None], args: argparse.Namespace, elapsed: float) -> str:
    want = args.nodes - 1
    lines = [f"MeshWeaver: {args.nodes} nodes, {elapsed:5.1f}s", "",
             f"{'node':<9}{'port':<7}{'peers':<8}status"]
    for i, c in enumerate(counts):
        if c is None:
            status, shown = "starting / down", "-"
        else:
            status, shown = ("connected" if c >= want else "joining"), f"{c}/{want}"
        lines.append(f"node-{i:<4}{args.base_port + i:<7}{shown:<8}{status}")
    return "\n".join(lines)


async def watch(args: argparse.Namespace, procs: list[subprocess.Popen]) -> bool:
    setup_logging("WARNING")
    monitor = UdpTransport(Dispatcher(MONITOR_ID), NodeLogger("launcher"))
    await monitor.start(args.host, 0)
    addrs = [(args.host, args.base_port + i) for i in range(args.nodes)]
    want = args.nodes - 1
    start = time.monotonic()
    formed_at = None
    interactive = sys.stdout.isatty()
    try:
        while True:
            counts = await peer_counts(monitor, addrs)
            elapsed = time.monotonic() - start
            table = render(counts, args, elapsed)
            print(("\x1b[2J\x1b[H" if interactive else "\n") + table, flush=True)
            dead = [i for i, p in enumerate(procs) if p.poll() is not None]
            if dead and formed_at is None:
                print(f"\nnode(s) exited during startup: {dead}; see {args.log_dir}")
                return False
            if formed_at is None and all(c is not None and c >= want for c in counts):
                formed_at = elapsed
            if formed_at is not None:
                print(f"\nMesh formed in {formed_at:.1f}s: every node knows all {want} others.")
                if args.exit_when_formed:
                    return True
                print("Running. Kill a node process to watch the others drop it. Ctrl+C to stop.")
            elif elapsed > args.timeout:
                print(f"\nMesh did not form within {args.timeout:.0f}s; see {args.log_dir}")
                return False
            await asyncio.sleep(1.0)
    finally:
        await monitor.close()


def main(argv=None) -> int:
    args = parse_args(argv)
    procs = start_nodes(args)
    try:
        ok = asyncio.run(watch(args, procs))
    except KeyboardInterrupt:
        ok = True
    finally:
        print("\nStopping nodes...")
        stop_nodes(procs)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
