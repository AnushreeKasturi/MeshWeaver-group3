"""Node runtime: starts every module on one asyncio event loop.

A MeshWeaver node is one process that runs:

    transport  ->  membership/DHT  ->  gossip  ->  scheduler  ->  tasks

all on a single asyncio event loop. Each part is a :class:`Module`. The runtime:

1. builds the node ID, the shared :class:`~meshweaver.protocol.Dispatcher` and
   the transport, then starts the transport;
2. starts each module in order, registering its message handlers on the
   dispatcher just before it starts;
3. shuts down in reverse order: each module's ``stop()`` runs, then every
   background task the module started with :meth:`Module.spawn` is cancelled,
   and the transport closes last.

Run one node from a terminal::

    python -m meshweaver.node --name node-0 --port 9000
    python -m meshweaver.node --name node-1 --port 9001 --seed 127.0.0.1:9000
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import time
from typing import Any, Awaitable, Callable, Iterable, Optional, Sequence

from .config import NodeConfig, load_config, parse_address
from .logs import NodeLogger, setup_logging
from .protocol import Dispatcher, Handler, Message, MessageType, node_id_to_hex
from .transport import Address, Transport, UdpTransport

NODE_ID_BITS = 160


def make_node_id(name: Optional[str] = None) -> int:
    """160-bit node ID. Same SHA-1 scheme as ``dht.node_id.generate_node_id``.

    A name gives a stable ID (handy for demos); no name gives a random one.
    """
    seed = name.encode("utf-8") if name else os.urandom(32)
    return int(hashlib.sha1(seed).hexdigest(), 16)


class Module:
    """Base class for every part of a node (DHT, gossip, scheduler, tasks...).

    Subclasses set ``name``, return their handlers from :meth:`handlers`, and
    put long-running loops in :meth:`spawn` so the runtime can cancel them.
    """

    name: str = "module"

    def __init__(self) -> None:
        self.node: Optional["Node"] = None
        self.log: Optional[NodeLogger] = None

    def attach(self, node: "Node") -> None:
        self.node = node
        self.log = node.log.child(self.name)

    def handlers(self) -> dict[MessageType, Handler]:
        """Message types this module handles. Registered before :meth:`start`."""
        return {}

    async def start(self) -> None:
        """Start background work. Must return quickly; use :meth:`spawn` for loops."""

    async def stop(self) -> None:
        """Flush state before the runtime cancels this module's tasks."""

    def spawn(self, coro: Awaitable[Any], name: str = "") -> asyncio.Task:
        assert self.node is not None, "module not attached to a node"
        return self.node.spawn(coro, owner=self.name, name=name)

    def status(self) -> dict:
        """Summary for the dashboard. Override to add module-specific fields."""
        return {}


class Node:
    """One MeshWeaver node: config, identity, dispatcher, transport, modules."""

    def __init__(
        self,
        config: NodeConfig,
        modules: Optional[Sequence[Module]] = None,
        transport_factory: Optional[Callable[["Node"], Transport]] = None,
    ) -> None:
        self.config = config
        self.node_id = make_node_id(config.name or None)
        self.id_hex = node_id_to_hex(self.node_id)
        self.name = config.name or self.id_hex[:8]
        self.log = NodeLogger(self.name)
        self.dispatcher = Dispatcher(self.id_hex)
        self.transport: Transport = (transport_factory or _default_transport)(self)
        self.modules: list[Module] = (
            list(modules) if modules is not None else default_modules(config))
        names = [m.name for m in self.modules]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate module names: {names}")
        self._tasks: dict[str, set[asyncio.Task]] = {}
        self._started: list[Module] = []
        self._stopped = asyncio.Event()
        self.state = "created"
        self.started_at: Optional[float] = None

    # -- lookup -------------------------------------------------------------

    @property
    def address(self) -> Optional[Address]:
        return self.transport.address

    def module(self, name: str) -> Module:
        for m in self.modules:
            if m.name == name:
                return m
        raise KeyError(name)

    # -- messaging helpers for modules ---------------------------------------

    def message(self, msg_type: MessageType, payload: Any = None) -> Message:
        return Message.create(msg_type, self.id_hex, payload)

    def send(self, addr: Address, msg_type: MessageType, payload: Any = None) -> None:
        self.transport.send(addr, self.message(msg_type, payload))

    async def request(self, addr: Address, msg_type: MessageType, payload: Any = None,
                      timeout: Optional[float] = None) -> Message:
        return await self.transport.request(addr, self.message(msg_type, payload), timeout)

    # -- background tasks ----------------------------------------------------

    def spawn(self, coro: Awaitable[Any], owner: str = "node", name: str = "") -> asyncio.Task:
        """Run ``coro`` in the background; it is cancelled when ``owner`` stops."""
        task = asyncio.get_running_loop().create_task(
            coro, name=f"{self.name}:{owner}:{name or 'task'}")
        bucket = self._tasks.setdefault(owner, set())
        bucket.add(task)
        task.add_done_callback(lambda t: self._task_done(owner, t))
        return task

    def _task_done(self, owner: str, task: asyncio.Task) -> None:
        self._tasks.get(owner, set()).discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.log.error("background task %s crashed", task.get_name(),
                           exc_info=task.exception())

    async def _cancel_tasks(self, owner: str) -> None:
        tasks = self._tasks.pop(owner, set())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=self.config.shutdown_timeout)

    # -- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        if self.state != "created":
            raise RuntimeError(f"cannot start a node that is {self.state}")
        self.state = "starting"
        t0 = time.monotonic()
        self.log.event("node.start", id=self.id_hex, modules=",".join(m.name for m in self.modules))
        try:
            host, port = await self.transport.start(self.config.host, self.config.port)
            self.log.event("module.start", module="transport", host=host, port=port)
            for module in self.modules:
                module.attach(self)
                for msg_type, handler in module.handlers().items():
                    self.dispatcher.register(msg_type, handler)
                await module.start()
                self._started.append(module)
                self.log.event("module.start", module=module.name)
        except BaseException:
            await self.stop()
            raise
        missing = self.dispatcher.missing_handlers()
        if missing:
            self.log.event("node.unhandled", level=20,
                           types=",".join(t.value for t in missing))
        self.state = "running"
        self.started_at = time.time()
        self.log.event("node.ready", host=host, port=port,
                       ms=round((time.monotonic() - t0) * 1000, 1))

    async def stop(self) -> None:
        if self.state in ("stopping", "stopped"):
            await self._stopped.wait()
            return
        self.state = "stopping"
        for module in reversed(self._started):
            try:
                await asyncio.wait_for(module.stop(), self.config.shutdown_timeout)
            except Exception:  # noqa: BLE001 - keep shutting the rest down
                self.log.exception("module %s failed to stop cleanly", module.name)
            await self._cancel_tasks(module.name)
            for msg_type in module.handlers():
                self.dispatcher.unregister(msg_type)
            self.log.event("module.stop", module=module.name)
        self._started.clear()
        for owner in list(self._tasks):
            await self._cancel_tasks(owner)
        await self.transport.close()
        self.log.event("module.stop", module="transport")
        self.state = "stopped"
        self.log.event("node.stop")
        self._stopped.set()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    async def __aenter__(self) -> "Node":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    def status(self) -> dict:
        return {
            "name": self.name,
            "id": self.id_hex,
            "address": self.address,
            "state": self.state,
            "uptime": round(time.time() - self.started_at, 1) if self.started_at else 0.0,
            "modules": {m.name: m.status() for m in self.modules},
        }


def _default_transport(node: Node) -> Transport:
    return UdpTransport(node.dispatcher, node.log.child("transport"),
                        max_message_size=node.config.max_message_size,
                        default_timeout=node.config.rpc_timeout)


def default_modules(config: NodeConfig) -> list[Module]:
    """The modules a node runs, in start order.

    Teammates' modules are added here as their branches are merged:
    DHT (C) replaces Membership, then gossip and scheduler (D), then tasks (E).
    """
    from .membership import Membership

    return [Membership()]


# ---------------------------------------------------------------------------
# Command line: python -m meshweaver.node
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m meshweaver.node",
                                description="Run one MeshWeaver node.")
    p.add_argument("--name", help="node name (also seeds a stable node ID)")
    p.add_argument("--host", help="interface to bind (default 127.0.0.1)")
    p.add_argument("--port", type=int, help="UDP port (default 9000, 0 = any free port)")
    p.add_argument("--seed", action="append", default=None, metavar="HOST:PORT",
                   help="existing node to join through (repeatable)")
    p.add_argument("--multicast", action="store_true", default=None,
                   help="also discover peers by UDP multicast on the local network")
    p.add_argument("--config", help="JSON config file")
    p.add_argument("--log-level", help="DEBUG, INFO, WARNING or ERROR")
    p.add_argument("--log-json", action="store_true", default=None,
                   help="log JSON lines instead of readable text")
    p.add_argument("--log-file", help="also write logs to this file")
    return p


def config_from_args(argv: Optional[Iterable[str]] = None) -> NodeConfig:
    args = build_arg_parser().parse_args(list(argv) if argv is not None else None)
    seeds = [parse_address(s) for s in args.seed] if args.seed else None
    return load_config(
        args.config, name=args.name, host=args.host, port=args.port, seeds=seeds,
        multicast_enabled=args.multicast,
        log_level=args.log_level, log_json=args.log_json, log_file=args.log_file,
    )


async def run_node(config: NodeConfig) -> None:
    node = Node(config)
    await node.start()
    try:
        await node.wait_stopped()
    finally:
        await node.stop()


def main(argv: Optional[Iterable[str]] = None) -> None:
    config = config_from_args(argv)
    setup_logging(config.log_level, config.log_json, config.log_file)
    try:
        asyncio.run(run_node(config))
    except KeyboardInterrupt:
        pass  # asyncio.run cancels run_node, whose finally block stops the node


if __name__ == "__main__":
    main()
