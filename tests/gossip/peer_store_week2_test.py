import time
import pytest

from gossip.peer_store import PeerLoadStore


def create_test_message():
    return {
        "type": "GOSSIP_LOAD",
        "sender_id": "a" * 40,
        "payload": {
            "node_id": "a" * 40,
            "cpu_usage": 25.5,
            "free_ram_gb": 3.0,
            "timestamp": time.time(),
        },
    }


def test_peer_store_fresh_peer():
    store = PeerLoadStore(stale_after=15)

    store.update_peer(create_test_message())

    assert len(store.get_fresh_peers()) == 1
    assert len(store.get_stale_peers()) == 0


def test_peer_store_stale_peer():
    store = PeerLoadStore(stale_after=1)

    peer = store.update_peer(create_test_message())
    peer.received_at = time.monotonic() - 5

    assert peer.is_stale


def test_peer_store_remove_stale_peer():
    store = PeerLoadStore(stale_after=1)

    peer = store.update_peer(create_test_message())
    peer.received_at = time.monotonic() - 5

    removed = store.remove_stale_peers()

    assert len(removed) == 1
    assert len(store.peers) == 0


def test_peer_store_rejects_invalid_message_type():
    store = PeerLoadStore()
    message = create_test_message()
    message["type"] = "INVALID"

    with pytest.raises(ValueError):
        store.update_peer(message)


def test_peer_store_rejects_sender_mismatch():
    store = PeerLoadStore()
    message = create_test_message()
    message["payload"]["node_id"] = "b" * 40

    with pytest.raises(ValueError):
        store.update_peer(message)


def test_peer_store_rejects_invalid_cpu():
    store = PeerLoadStore()
    message = create_test_message()
    message["payload"]["cpu_usage"] = 150

    with pytest.raises(ValueError):
        store.update_peer(message)


def test_peer_store_rejects_negative_ram():
    store = PeerLoadStore()
    message = create_test_message()
    message["payload"]["free_ram_gb"] = -2

    with pytest.raises(ValueError):
        store.update_peer(message)
