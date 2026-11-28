
import asyncio
from types import SimpleNamespace

from meshweaver.dht.rpc import (
    request_find_node,
    request_store,
    request_find_value,
)

from meshweaver.protocol import (
    FindNodeReply,
    FindValueReply,
    StoreReply,
    PeerInfo,
    node_id_to_hex,
)


class FakeResponse:
    def __init__(self, result):
        self.result = result

    def parsed(self):
        return self.result


class FakeNode:
    def __init__(self, response):
        self.response = response
        self.calls = []

        self.config = SimpleNamespace(
            rpc_timeout=2.0
        )

    async def request(
        self,
        address,
        message_type,
        payload,
        timeout=None,
    ):
        self.calls.append(
            (address, message_type, payload, timeout)
        )

        return self.response


def make_dht(response):
    dht = SimpleNamespace()

    dht.peer_addresses = {
        200: ("127.0.0.1", 9001)
    }

    dht.node = FakeNode(response)

    return dht


def test_request_find_node():
    peer = PeerInfo(
        node_id=node_id_to_hex(300),
        host="127.0.0.1",
        port=9002,
    )

    dht = make_dht(
        FakeResponse(
            FindNodeReply.from_peers([peer])
        )
    )

    result = asyncio.run(
        request_find_node(dht, 200, 300)
    )

    assert len(result) == 1
    assert result[0].node_id == node_id_to_hex(300)
    assert len(dht.node.calls) == 1


def test_request_find_node_unknown_peer():
    dht = make_dht(
        FakeResponse(FindNodeReply.from_peers([]))
    )

    result = asyncio.run(
        request_find_node(dht, 999, 300)
    )

    assert result == []
    assert dht.node.calls == []


def test_request_store_success():
    dht = make_dht(
        FakeResponse(StoreReply(ok=True))
    )

    result = asyncio.run(
        request_store(dht, 200, "name", "Sahana")
    )

    assert result is True
    assert len(dht.node.calls) == 1


def test_request_store_failure():
    dht = make_dht(
        FakeResponse(StoreReply(ok=False))
    )

    result = asyncio.run(
        request_store(dht, 200, "name", "Sahana")
    )

    assert result is False


def test_request_store_unknown_peer():
    dht = make_dht(
        FakeResponse(StoreReply(ok=True))
    )

    result = asyncio.run(
        request_store(dht, 999, "name", "Sahana")
    )

    assert result is False
    assert dht.node.calls == []


def test_request_find_value_success():
    dht = make_dht(
        FakeResponse(
            FindValueReply(
                found=True,
                value="Sahana",
            )
        )
    )

    result = asyncio.run(
        request_find_value(dht, 200, "name")
    )

    assert result == "Sahana"


def test_request_find_value_missing():
    dht = make_dht(
        FakeResponse(
            FindValueReply(found=False)
        )
    )

    result = asyncio.run(
        request_find_value(dht, 200, "unknown")
    )

    assert result is None


def test_request_find_value_unknown_peer():
    dht = make_dht(
        FakeResponse(
            FindValueReply(found=True, value="Sahana")
        )
    )

    result = asyncio.run(
        request_find_value(dht, 999, "name")
    )

    assert result is None
    assert dht.node.calls == []
