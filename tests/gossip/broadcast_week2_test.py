import asyncio
import time
from unittest.mock import Mock

from gossip.broadcast import GossipModule
from meshweaver.protocol import LoadReport


def test_gossip_module_receives_message():
    gossip = GossipModule()

    sender_id = "b" * 40

    report = LoadReport(
        node_id=sender_id,
        cpu_usage=30.0,
        free_ram_gb=2.5,
    )

    message_data = {
        "type": "GOSSIP_LOAD",
        "sender_id": sender_id,
        "payload": {
            "node_id": sender_id,
            "cpu_usage": 30.0,
            "free_ram_gb": 2.5,
            "timestamp": time.time(),
        },
    }

    fake_message = Mock()
    fake_message.parsed.return_value = report
    fake_message.to_dict.return_value = message_data

    gossip.log = Mock()

    asyncio.run(
        gossip.on_load(
            fake_message,
            ("127.0.0.1", 8001)
        )
    )

    received = gossip.get_peer_loads()

    assert sender_id in received
    assert received[sender_id].cpu_usage == 30.0
    assert received[sender_id].free_ram_gb == 2.5
