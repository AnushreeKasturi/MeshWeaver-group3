"""Start a MeshWeaver mesh of N nodes on one machine and watch it form.

    python scripts/launch_mesh.py                 # 10 nodes on ports 9000-9009
    python scripts/launch_mesh.py --nodes 20 --base-port 9100
    python scripts/launch_mesh.py --exit-when-formed   # stop once all are connected
    python scripts/launch_mesh.py --multicast          # no seeds: find peers by multicast

Node 0 is the seed; every other node starts with ``--seed`` pointing at it.
With ``--multicast`` no node gets a seed and they find each other by UDP
multicast instead. Each node runs as its own process (so any one can be
killed to test failure) and writes its log to ``logs/node-<i>.log``.

Once a second the launcher sends every node a STATUS request and prints a
table of its peers (from the DHT) and the fresh load reports it holds (from
gossip). STATUS is answered by the node runtime itself, so the launcher never
becomes anyone's peer. When every node knows all the others it prints how
long the mesh took to form. Ctrl+C stops all nodes.
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
    Dispatcher, Message, MessageType, Status, StatusReply, node_id_to_hex,
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
    p.add_argument("--multicast", action="store_true",
                   help="start nodes with no seeds; they find each other by multicast")
    p.add_argument("--timeout", type=float, default=60.0,
                   help="seconds to wait for the mesh to form (default 60)")
    p.add_argument("--exit-when-formed", action="store_true",
                   help="stop all nodes as soon as the mesh has formed")
    args = p.parse_args(argv)
    if args.nodes < 2:
        p.error("--nodes must be at least 2")
    return args


def start_nodes(args: argparse.Namespace, indexes: range) -> list[subprocess.Popen]:
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    seed = f"{args.host}:{args.base_port}"
    procs = []
    for i in indexes:
        cmd = [sys.executable, "-m", "meshweaver.node",
               "--name", f"node-{i}", "--host", args.host,
               "--port", str(args.base_port + i),
               "--log-file", str(log_dir / f"node-{i}.log")]
        if args.multicast:
            cmd.append("--multicast")
        elif i > 0:
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


async def node_statuses(monitor: UdpTransport,
                        addrs: list[tuple[str, int]]) -> list[StatusReply | None]:
    """Ask every node for its status; ``None`` means the node did not answer."""

    async def ask(addr):
        msg = Message.create(MessageType.STATUS, MONITOR_ID, Status())
        try:
            reply = await monitor.request(addr, msg, timeout=1.0)
        except TransportError:
            return None
        if reply.type is not MessageType.STATUS_REPLY:
            return None
        return reply.parsed()

    return list(await asyncio.gather(*(ask(a) for a in addrs)))


def load_count(st: StatusReply) -> str:
    """Fresh gossip load reports the node holds, or "-" without a gossip module."""
    n = (st.modules.get("gossip") or {}).get("known_load_reports")
    return "-" if n is None else str(n)


def render(statuses: list[StatusReply | None], args: argparse.Namespace,
           elapsed: float) -> str:
    want = args.nodes - 1
    mode = "multicast" if args.multicast else f"seed {args.host}:{args.base_port}"
    lines = [f"MeshWeaver: {args.nodes} nodes ({mode}), {elapsed:5.1f}s", "",
             f"{'node':<9}{'port':<7}{'peers':<8}{'loads':<7}status"]
    for i, st in enumerate(statuses):
        port = args.base_port + i
        if st is None:
            lines.append(f"node-{i:<4}{port:<7}{'-':<8}{'-':<7}starting / down")
            continue
        status = "connected" if st.peers >= want else "joining"
        lines.append(f"node-{i:<4}{port:<7}{f'{st.peers}/{want}':<8}{load_count(st):<7}{status}")
    lines += ["", "peers = nodes known through the DHT; loads = fresh gossip load reports held"]
    return "\n".join(lines)


async def wait_until_answering(monitor: UdpTransport, addr: tuple[str, int],
                               timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (await node_statuses(monitor, [addr]))[0] is not None:
            return True
        await asyncio.sleep(0.1)
    return False


async def watch(args: argparse.Namespace, procs: list[subprocess.Popen]) -> bool:
    setup_logging("WARNING")
    monitor = UdpTransport(Dispatcher(MONITOR_ID), NodeLogger("launcher"))
    await monitor.start(args.host, 0)
    addrs = [(args.host, args.base_port + i) for i in range(args.nodes)]
    # Start the seed first and wait until it answers, so the other nodes'
    # first PING to it is not lost while it is still starting up.
    procs += start_nodes(args, range(0, 1))
    if not await wait_until_answering(monitor, addrs[0], timeout=15.0):
        print(f"seed node-0 did not start; see {args.log_dir}")
        await monitor.close()
        return False
    procs += start_nodes(args, range(1, args.nodes))
    want = args.nodes - 1
    start = time.monotonic()
    formed_at = None
    interactive = sys.stdout.isatty()
    try:
        while True:
            statuses = await node_statuses(monitor, addrs)
            counts = [None if st is None else st.peers for st in statuses]
            elapsed = time.monotonic() - start
            table = render(statuses, args, elapsed)
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
                print("Running. Ctrl+C to stop.")
            elif elapsed > args.timeout:
                print(f"\nMesh did not form within {args.timeout:.0f}s; see {args.log_dir}")
                return False
            await asyncio.sleep(1.0)
    finally:
        await monitor.close()


def main(argv=None) -> int:
    args = parse_args(argv)
    procs: list[subprocess.Popen] = []   # watch() starts the nodes and fills this in
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
