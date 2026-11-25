 
import asyncio
from types import SimpleNamespace

from meshweaver.dht.node import DHT
from meshweaver.dht.routing_table import RoutingTable
from meshweaver.protocol import (
    PeerInfo,
    FindNode,
    Store,
    FindValue,
    MessageType,
    node_id_to_hex,
)


def make_dht(local_id=100):
    dht = DHT()

    dht.node = SimpleNamespace(
        node_id=local_id,
        id_hex=node_id_to_hex(local_id),
        config=SimpleNamespace(k=20),
    )

    dht.routing_table = RoutingTable(local_id)

    return dht


def test_dht_initialization():
    dht = DHT()

    assert dht.name == "dht"
    assert dht.data_store == {}
    assert dht.peer_addresses == {}


def test_handlers_registration():
    dht = DHT()
    handlers = dht.handlers()

    assert MessageType.PING in handlers
    assert MessageType.FIND_NODE in handlers
    assert MessageType.STORE in handlers
    assert MessageType.FIND_VALUE in handlers


def test_add_peer():
    dht = make_dht()

    peer = PeerInfo(
        node_id=node_id_to_hex(200),
        host="127.0.0.1",
        port=9001,
    )

    assert dht.add_peer(peer) is True
    assert 200 in dht.routing_table.get_all_peers()
    assert dht.peer_addresses[200] == ("127.0.0.1", 9001)


def test_reject_own_node():
    dht = make_dht(100)

    peer = PeerInfo(
        node_id=node_id_to_hex(100),
        host="127.0.0.1",
        port=9000,
    )

    assert dht.add_peer(peer) is False


def test_remove_peer():
    dht = make_dht()

    peer = PeerInfo(
        node_id=node_id_to_hex(200),
        host="127.0.0.1",
        port=9001,
    )

    dht.add_peer(peer)

    assert dht.remove_peer(200) is True
    assert 200 not in dht.peer_addresses


def test_nearest_peers():
    dht = make_dht()

    for peer_id in [200, 150, 300]:
        dht.add_peer(
            PeerInfo(
                node_id=node_id_to_hex(peer_id),
                host="127.0.0.1",
                port=9000 + peer_id,
            )
        )

    result = dht.get_nearest_peer_infos(180)

    distances = [
        int(peer.node_id, 16) ^ 180
        for peer in result
    ]

    assert distances == sorted(distances)


class FakeMessage:
    def __init__(self, payload, sender_id=None):
        self.payload = payload
        self.sender_id = sender_id or node_id_to_hex(999)
        self.reply_type = None
        self.reply_sender = None
        self.reply_payload = None

    def parsed(self):
        return self.payload

    def reply(self, message_type, sender_id, payload):
        self.reply_type = message_type
        self.reply_sender = sender_id
        self.reply_payload = payload
        return self


TEST_ADDR = ("127.0.0.1", 9005)


def test_ping_handler():
    dht = make_dht()

    message = FakeMessage(
        payload=None,
        sender_id=node_id_to_hex(999),
    )

    asyncio.run(dht.on_ping(message, TEST_ADDR))

    assert 999 in dht.routing_table.get_all_peers()
    assert dht.peer_addresses[999] == TEST_ADDR
    assert message.reply_type == MessageType.PONG


def test_store_handler():
    dht = make_dht()

    message = FakeMessage(
        Store(key="student", value="Sahana"),
    )

    asyncio.run(dht.on_store(message, TEST_ADDR))

    assert dht.data_store["student"] == "Sahana"
    assert message.reply_payload.ok is True
    assert 999 in dht.routing_table.get_all_peers()


def test_find_value_handler_when_value_exists():
    dht = make_dht()
    dht.data_store["student"] = "Sahana"

    message = FakeMessage(
        FindValue(key="student"),
    )

    asyncio.run(dht.on_find_value(message, TEST_ADDR))

    assert message.reply_payload.found is True
    assert message.reply_payload.value == "Sahana"
    assert 999 in dht.routing_table.get_all_peers()


def test_find_value_handler_when_value_missing():
    dht = make_dht()

    message = FakeMessage(
        FindValue(key="unknown"),
    )

    asyncio.run(dht.on_find_value(message, TEST_ADDR))

    assert message.reply_payload.found is False
    assert 999 in dht.routing_table.get_all_peers()


def test_find_node_handler():
    dht = make_dht()

    dht.add_peer(
        PeerInfo(
            node_id=node_id_to_hex(200),
            host="127.0.0.1",
            port=9001,
        )
    )

    message = FakeMessage(
        FindNode(target_id=node_id_to_hex(200)),
    )

    asyncio.run(dht.on_find_node(message, TEST_ADDR))

    assert message.reply_payload is not None

    returned_peer_ids = [
        int(peer.node_id, 16)
        for peer in message.reply_payload.peer_infos()
    ]

    assert 200 in returned_peer_ids
    assert 999 in dht.routing_table.get_all_peers()


def test_peer_list():
    dht = make_dht()

    dht.add_peer(
        PeerInfo(
            node_id=node_id_to_hex(200),
            host="127.0.0.1",
            port=9001,
        )
    )

    peers = dht.peer_list()

    assert len(peers) == 1
    assert peers[0]["node_id"] == node_id_to_hex(200)
    assert peers[0]["address"] == ("127.0.0.1", 9001)
