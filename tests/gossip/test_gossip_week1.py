
import time

from gossip.collector import collect_system_load
from gossip.protocol import (
    create_gossip_message,
    GossipMessage,
)


def test_collector_protocol_integration():
    # Step 1: Collect actual system load
    load_info = collect_system_load()

    # Step 2: Verify collected information
    assert "cpu_usage" in load_info
    assert "total_ram_gb" in load_info
    assert "free_ram_gb" in load_info

    assert 0 <= load_info["cpu_usage"] <= 100
    assert load_info["total_ram_gb"] > 0
    assert load_info["free_ram_gb"] >= 0

    # Step 3: Create Gossip message
    node_id = "a" * 40

    message = create_gossip_message(
        node_id,
        load_info
    )

    # Step 4: Verify message
    assert isinstance(message, GossipMessage)
    assert message.node_id == node_id
    assert message.cpu_usage == load_info["cpu_usage"]
    assert message.free_ram_gb == load_info["free_ram_gb"]

    # Step 5: Verify timestamp
    assert message.timestamp > 0
    assert abs(message.timestamp - time.time()) < 5


def test_collector_protocol_serialization():
    # Collect system load
    load_info = collect_system_load()

    # Create original Gossip message
    original = create_gossip_message(
        "b" * 40,
        load_info
    )

    # Convert LoadReport into dictionary
    data = original.to_dict()

    # Reconstruct LoadReport
    restored = type(original).from_dict(data)

    # Verify reconstructed message
    assert restored.node_id == original.node_id
    assert restored.cpu_usage == original.cpu_usage
    assert restored.free_ram_gb == original.free_ram_gb
    assert restored.timestamp == original.timestamp
