from gossip.protocol import (
    create_gossip_message,
    GossipMessage,
)


def test_create_gossip_message():
    load_info = {
        "cpu_usage": 25.5,
        "free_ram_gb": 3.0,
    }

    message = create_gossip_message(
        "a" * 40,
        load_info
    )

    assert isinstance(message, GossipMessage)
    assert message.node_id == "a" * 40
    assert message.cpu_usage == 25.5
    assert message.free_ram_gb == 3.0
    assert message.timestamp > 0
