import asyncio

import pytest

from meshweaver.config import NodeConfig
from meshweaver.node import Module, Node, config_from_args, make_node_id
from meshweaver.protocol import Message, MessageType, Ping, Status, TaskSubmit
from meshweaver.transport import RequestTimeout, TransportError


def cfg(**kw):
    kw.setdefault("port", 0)
    kw.setdefault("rpc_timeout", 0.3)
    return NodeConfig(**kw)


def run(coro):
    return asyncio.run(coro)


class Recorder(Module):
    """Test module that records lifecycle calls into a shared list."""

    def __init__(self, name, events, fail_start=False):
        super().__init__()
        self.name = name
        self.events = events
        self.fail_start = fail_start
        self.task = None

    def handlers(self):
        if self.name == "pinger":
            return {MessageType.PING: lambda m, a: m.reply(
                MessageType.PONG, self.node.id_hex, Ping(42))}
        return {}

    async def start(self):
        self.events.append(f"start:{self.name}")
        if self.fail_start:
            raise RuntimeError("boom")
        self.task = self.spawn(self._loop(), name="loop")

    async def _loop(self):
        await asyncio.Event().wait()

    @property
    def loop_cancelled(self):
        return self.task is not None and self.task.cancelled()

    async def stop(self):
        self.events.append(f"stop:{self.name}")


def test_node_id_is_stable_for_a_name_and_random_otherwise():
    assert make_node_id("node-1") == make_node_id("node-1")
    assert make_node_id("node-1") != make_node_id("node-2")
    assert make_node_id() != make_node_id()
    assert make_node_id("x").bit_length() <= 160


def test_modules_start_in_order_and_stop_in_reverse():
    events = []
    mods = [Recorder(n, events) for n in ("dht", "gossip", "tasks")]

    async def go():
        node = Node(cfg(name="n"), modules=mods)
        await node.start()
        assert node.state == "running"
        assert node.address[1] > 0
        await node.stop()
        assert node.state == "stopped"

    run(go())
    assert events == ["start:dht", "start:gossip", "start:tasks",
                      "stop:tasks", "stop:gossip", "stop:dht"]
    assert all(m.loop_cancelled for m in mods)


def test_failed_start_stops_what_already_started():
    events = []
    good = Recorder("dht", events)
    bad = Recorder("gossip", events, fail_start=True)

    async def go():
        node = Node(cfg(), modules=[good, bad, Recorder("tasks", events)])
        with pytest.raises(RuntimeError, match="boom"):
            await node.start()
        assert node.state == "stopped"

    run(go())
    assert events == ["start:dht", "start:gossip", "stop:dht"]
    assert good.loop_cancelled


def test_duplicate_module_names_rejected():
    with pytest.raises(ValueError):
        Node(cfg(), modules=[Recorder("a", []), Recorder("a", [])])


def test_stop_is_idempotent_and_context_manager_works():
    async def go():
        async with Node(cfg(), modules=[]) as node:
            assert node.state == "running"
        await node.stop()
        return node.state

    assert run(go()) == "stopped"


def test_cannot_start_twice():
    async def go():
        node = Node(cfg(), modules=[])
        await node.start()
        try:
            with pytest.raises(RuntimeError):
                await node.start()
        finally:
            await node.stop()

    run(go())


def test_request_reply_between_two_nodes_over_udp():
    async def go():
        a = Node(cfg(name="a"), modules=[Recorder("pinger", [])])
        b = Node(cfg(name="b"), modules=[])
        await a.start()
        await b.start()
        try:
            reply = await b.request(a.address, MessageType.PING, Ping(1))
            assert reply.type is MessageType.PONG
            assert reply.sender_id == a.id_hex
            assert reply.parsed().port == 42
            # A type nobody handles comes back as an ERROR reply, not silence.
            err = await b.request(a.address, MessageType.TASK_SUBMIT, TaskSubmit("t", b"x"))
            assert err.type is MessageType.ERROR and err.parsed().code == "NO_HANDLER"
        finally:
            await a.stop()
            await b.stop()

    run(go())


def test_request_to_dead_address_times_out_and_socket_keeps_working():
    async def go():
        a = Node(cfg(name="a"), modules=[Recorder("pinger", [])])
        b = Node(cfg(name="b"), modules=[])
        await a.start()
        await b.start()
        dead_port = a.address[1]
        try:
            c = Node(cfg(name="c"), modules=[])
            await c.start()
            dead = c.address
            await c.stop()
            # Several sends to a closed port at once used to make Windows
            # stop delivering datagrams to this socket.
            results = await asyncio.gather(
                *(b.request(dead, MessageType.PING, Ping(1), timeout=0.2) for _ in range(5)),
                return_exceptions=True)
            assert all(isinstance(r, RequestTimeout) for r in results)
            reply = await b.request(("127.0.0.1", dead_port), MessageType.PING, Ping(1))
            assert reply.type is MessageType.PONG
        finally:
            await a.stop()
            await b.stop()

    run(go())


def test_oversized_message_rejected():
    async def go():
        async with Node(cfg(max_message_size=1000), modules=[]) as node:
            with pytest.raises(TransportError, match="over the"):
                node.send(("127.0.0.1", 9), MessageType.TASK_SUBMIT,
                          TaskSubmit("t", b"x" * 5000))

    run(go())


def test_pending_requests_fail_when_node_stops():
    async def go():
        node = Node(cfg(rpc_timeout=5.0), modules=[])
        await node.start()
        silent = Node(cfg(), modules=[])
        await silent.start()
        silent.dispatcher.register(MessageType.PING, lambda m, a: None)
        pending = asyncio.ensure_future(node.request(silent.address, MessageType.PING, Ping(1)))
        await asyncio.sleep(0.05)
        await node.stop()
        with pytest.raises(TransportError):
            await pending
        await silent.stop()

    run(go())


def test_crashing_background_task_is_logged_not_fatal(caplog):
    async def go():
        async with Node(cfg(), modules=[]) as node:
            async def crash():
                raise ValueError("bad loop")
            node.spawn(crash(), owner="x")
            await asyncio.sleep(0.05)
            assert node.state == "running"

    run(go())


def test_status_reports_modules():
    async def go():
        async with Node(cfg(name="s"), modules=[Recorder("dht", [])]) as node:
            return node.status()

    st = run(go())
    assert st["name"] == "s" and st["state"] == "running" and "dht" in st["modules"]


def test_cli_args_build_config():
    c = config_from_args(["--name", "node-2", "--port", "9002",
                          "--seed", "127.0.0.1:9000", "--seed", "9001", "--log-json"])
    assert c.name == "node-2" and c.port == 9002 and c.log_json is True
    assert c.seeds == [("127.0.0.1", 9000), ("127.0.0.1", 9001)]


def test_cli_multicast_flag():
    assert config_from_args(["--multicast"]).multicast_enabled is True
    assert config_from_args([]).multicast_enabled is False


def test_status_request_is_answered_by_the_runtime():
    async def go():
        async with Node(cfg(name="st"), modules=[Recorder("dht", [])]) as a, \
                   Node(cfg(name="asker"), modules=[]) as b:
            reply = await b.request(a.address, MessageType.STATUS, Status())
            return reply

    reply = run(go())
    assert reply.type is MessageType.STATUS_REPLY
    st = reply.parsed()
    assert st.name == "st" and st.state == "running" and st.peers == 0
    assert "dht" in st.modules


def test_peers_come_from_the_module_with_peer_list():
    class FakeDht(Module):
        name = "dht"

        def peer_list(self):
            return ["p1", "p2"]

    node = Node(cfg(), modules=[Recorder("x", []), FakeDht()])
    assert node.peers() == ["p1", "p2"]
    assert Node(cfg(), modules=[]).peers() == []
