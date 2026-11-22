
from types import SimpleNamespace

from meshweaver.gossip.peer_register import (
    get_nearest_neighbours,
)


def test_get_nearest_neighbours():
    local_id = 100

    peers = [
        SimpleNamespace(
            node_id=f"{101:040x}",
            address=("127.0.0.1", 8001)
        ),
        SimpleNamespace(
            node_id=f"{102:040x}",
            address=("127.0.0.1", 8002)
        ),
        SimpleNamespace(
            node_id=f"{103:040x}",
            address=("127.0.0.1", 8003)
        ),
        SimpleNamespace(
            node_id=f"{104:040x}",
            address=("127.0.0.1", 8004)
        ),
        SimpleNamespace(
            node_id=f"{100:040x}",
            address=("127.0.0.1", 8000)
        ),
    ]

    result = get_nearest_neighbours(
        local_id,
        peers,
        count=3
    )

    assert len(result) == 3

    result_ids = [int(peer.node_id, 16) for peer in result]

    assert 100 not in result_ids
    assert result_ids == [101, 102, 103]
