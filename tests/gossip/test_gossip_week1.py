
import time

from meshweaver.gossip.collector import collect_system_load
from meshweaver.gossip.protocol import (
    create_gossip_message,
    GossipMessage,
)
from meshweaver.protocol import LoadReport


def test_collector_protocol_integration():
    # Step 1: Collect actual system load
    load_info = collect_system_load()

    # Step 2: Check collected information
    assert "cpu_usage" in load_info
    assert "total_ram_gb" in load_info
    assert "free_ram_gb" in load_info

    assert 0 <= load_info["cpu_usage"] <= 100
    assert load_info["total_ram_gb"] > 0
    assert load_info["free_ram_gb"] >= 0

    # Step 3: Create a Gossip message
    node_id = "a" * 40

    message = create_gossip_message(
        node_id,
        load_info
    )

    # Step 4: Verify message
    assert isinstance(message, LoadReport)
    assert isinstance(message, GossipMessage)

    assert message.node_id == node_id
    assert message.cpu_usage == load_info["cpu_usage"]
    assert message.free_ram_gb == load_info["free_ram_gb"]

    # Step 5: Verify timestamp
    assert message.timestamp > 0
    assert abs(message.timestamp - time.time()) < 5
