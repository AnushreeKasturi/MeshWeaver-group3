import asyncio
import json

import pytest

from meshweaver.protocol import (
    PAYLOAD_OF,
    Dispatcher,
    FindNodeReply,
    LoadReport,
    Message,
    MessageType,
    PeerInfo,
    Ping,
    ProtocolError,
    TaskResult,
    TaskSubmit,
    node_id_from_hex,
    node_id_to_hex,
)

SENDER = "a" * 40


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------- envelope

def test_every_type_has_a_payload_class():
    assert set(PAYLOAD_OF) == set(MessageType)


def test_round_trip_keeps_all_fields():
    msg = Message.create(MessageType.PING, SENDER, Ping(port=9000))
    back = Message.decode(msg.encode())
    assert back == msg
    assert back.parsed() == Ping(port=9000)


def test_type_accepts_string():
    msg = Message.create("GOSSIP_LOAD", SENDER, LoadReport(SENDER, 12.5, 3.0, 1.0))
    assert msg.type is MessageType.GOSSIP_LOAD


def test_bytes_payload_survives_json():
    blob = bytes(range(256))
    msg = Message.create(MessageType.TASK_SUBMIT, SENDER, TaskSubmit("t1", blob, timeout=5))
    wire = msg.encode()
    json.loads(wire)  # must be plain JSON
    parsed = Message.decode(wire).parsed()
    assert parsed.blob == blob
    assert parsed.signature is None


def test_reply_links_to_request():
    req = Message.create(MessageType.PING, SENDER)
    rep = req.reply(MessageType.PONG, "b" * 40, Ping(1))
    assert rep.reply_to == req.msg_id
    assert rep.msg_id != req.msg_id


def test_nested_payload_dataclasses():
    peers = [PeerInfo("c" * 40, "127.0.0.1", 9001), PeerInfo("d" * 40, "10.0.0.2", 9002)]
    msg = Message.create(MessageType.FIND_NODE_REPLY, SENDER, FindNodeReply.from_peers(peers))
    assert Message.decode(msg.encode()).parsed().peer_infos() == peers


@pytest.mark.parametrize(
    "data",
    [
        b"not json",
        b"\xff\xfe",
        b"[1, 2]",
        b'{"type": "PING", "msg_id": "x"}',
        b'{"type": "NOPE", "msg_id": "x", "sender_id": "y"}',
        b'{"type": "PING", "msg_id": "", "sender_id": "y"}',
        b'{"type": "PING", "msg_id": "x", "sender_id": "y", "version": 99}',
        b'{"type": "PING", "msg_id": "x", "sender_id": "y", "payload": [1]}',
    ],
)
def test_decode_rejects_bad_input(data):
    with pytest.raises(ProtocolError):
        Message.decode(data)


def test_missing_payload_field_is_protocol_error():
    msg = Message(MessageType.TASK_RESULT, "m1", SENDER, payload={"status": "done"})
    with pytest.raises(ProtocolError):
        msg.parsed()


def test_unknown_payload_keys_are_ignored():
    msg = Message(MessageType.PING, "m1", SENDER, payload={"port": 1, "future_field": True})
    assert msg.parsed() == Ping(1)


def test_load_report_matches_gossip_message_fields():
    # gossip.protocol.GossipMessage uses exactly these four fields.
    assert set(LoadReport(SENDER, 1.0, 2.0).to_dict()) == {
        "node_id", "cpu_usage", "free_ram_gb", "timestamp"
    }


def test_node_id_hex_round_trip():
    big = (1 << 160) - 1
    assert node_id_from_hex(node_id_to_hex(big)) == big
    assert len(node_id_to_hex(5)) == 40
    with pytest.raises(ValueError):
        node_id_to_hex(1 << 160)
    with pytest.raises(ValueError):
        node_id_from_hex("abc")


# -------------------------------------------------------------- dispatcher

def test_dispatch_to_sync_and_async_handlers():
    d = Dispatcher(SENDER)
    d.register(MessageType.PING, lambda m, a: m.reply(MessageType.PONG, SENDER, Ping(7)))

    @d.on(MessageType.GOSSIP_LOAD)
    async def on_load(msg, addr):
        await asyncio.sleep(0)
        return None

    pong = run(d.dispatch(Message.create(MessageType.PING, "b" * 40)))
    assert pong.type is MessageType.PONG and pong.parsed().port == 7
    load = Message.create(MessageType.GOSSIP_LOAD, "b" * 40, LoadReport("b" * 40, 1, 1))
    assert run(d.dispatch(load)) is None


def test_handler_receives_address():
    d = Dispatcher(SENDER)
    seen = []
    d.register(MessageType.PING, lambda m, a: seen.append(a))
    run(d.dispatch(Message.create(MessageType.PING, "b" * 40), ("1.2.3.4", 5)))
    assert seen == [("1.2.3.4", 5)]


def test_duplicate_registration_is_rejected():
    d = Dispatcher(SENDER)
    d.register(MessageType.PING, lambda m, a: None)
    with pytest.raises(ValueError):
        d.register(MessageType.PING, lambda m, a: None)
    d.register(MessageType.PING, lambda m, a: None, replace=True)


def test_unknown_type_gets_error_reply():
    d = Dispatcher(SENDER)
    req = Message.create(MessageType.FIND_NODE, "b" * 40, {"target_id": "0" * 40})
    rep = run(d.dispatch(req))
    assert rep.type is MessageType.ERROR
    assert rep.reply_to == req.msg_id
    assert rep.parsed().code == "NO_HANDLER"


def test_handler_exception_becomes_error_not_crash():
    d = Dispatcher(SENDER)

    def boom(m, a):
        raise RuntimeError("bug")

    d.register(MessageType.TASK_SUBMIT, boom)
    req = Message.create(MessageType.TASK_SUBMIT, "b" * 40, TaskSubmit("t", b"x"))
    rep = run(d.dispatch(req))
    assert rep.parsed().code == "HANDLER_FAILED"
    assert d.stats["errors"] == 1


def test_bad_payload_in_handler_reports_bad_payload():
    d = Dispatcher(SENDER)
    d.register(MessageType.TASK_RESULT, lambda m, a: m.parsed())
    rep = run(d.dispatch(Message(MessageType.TASK_RESULT, "m", "b" * 40, payload={})))
    assert rep.parsed().code == "BAD_PAYLOAD"


def test_handler_returning_non_message_is_error():
    d = Dispatcher(SENDER)
    d.register(MessageType.PING, lambda m, a: "oops")
    rep = run(d.dispatch(Message.create(MessageType.PING, "b" * 40)))
    assert rep.type is MessageType.ERROR


def test_reply_hook_claims_replies():
    d = Dispatcher(SENDER)
    claimed = []
    d.set_reply_hook(lambda m: claimed.append(m.reply_to) or True)
    req = Message.create(MessageType.PING, SENDER)
    pong = req.reply(MessageType.PONG, "b" * 40)
    assert run(d.dispatch(pong)) is None
    assert claimed == [req.msg_id]


def test_unclaimed_reply_is_dropped_without_error_reply():
    d = Dispatcher(SENDER)
    pong = Message.create(MessageType.PING, SENDER).reply(MessageType.PONG, "b" * 40)
    assert run(d.dispatch(pong)) is None
    err = Message.create(MessageType.ERROR, "b" * 40, {"code": "X"})
    assert run(d.dispatch(err)) is None
    assert d.stats["dropped"] == 2


def test_dispatch_bytes_end_to_end():
    d = Dispatcher(SENDER)
    d.register(MessageType.TASK_SUBMIT, lambda m, a: m.reply(
        MessageType.TASK_RESULT, SENDER, TaskResult(m.parsed().task_id, "done", b"42")))
    req = Message.create(MessageType.TASK_SUBMIT, "b" * 40, TaskSubmit("t9", b"code"))
    out = run(d.dispatch_bytes(req.encode(), ("127.0.0.1", 1)))
    res = Message.decode(out)
    assert res.reply_to == req.msg_id
    assert res.parsed() == TaskResult("t9", "done", b"42")


def test_dispatch_bytes_drops_garbage():
    d = Dispatcher(SENDER)
    assert run(d.dispatch_bytes(b"\x00garbage")) is None
    assert d.stats["dropped"] == 1


def test_missing_handlers_lists_request_types_only():
    d = Dispatcher(SENDER)
    missing = set(d.missing_handlers())
    assert MessageType.PING in missing and MessageType.TASK_SUBMIT in missing
    assert MessageType.PONG not in missing and MessageType.FIND_NODE_REPLY not in missing
    for t in list(missing):
        d.register(t, lambda m, a: None)
    assert d.missing_handlers() == []
