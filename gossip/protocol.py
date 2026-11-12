
from dataclasses import dataclass, asdict
import json
import time


@dataclass
class GossipMessage:
    """Message containing node load information."""

    node_id: str
    cpu_usage: float
    free_ram_gb: float
    timestamp: float

    def to_json(self):
        """Convert message into JSON."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, message):
        """Convert JSON into a GossipMessage object."""
        data = json.loads(message)
        return cls(**data)


def create_gossip_message(node_id, load_info):
    """Create a gossip message using system load."""

    return GossipMessage(
        node_id=node_id,
        cpu_usage=load_info["cpu_usage"],
        free_ram_gb=load_info["free_ram_gb"],
        timestamp=time.time()
    )

if __name__ == "__main__":

    load = {
        "cpu_usage": 25.5,
        "total_ram_gb": 8.0,
        "free_ram_gb": 4.0
    }
    message = create_gossip_message("node-a", load)

    print("Original Gossip Message:")
    print(message)
    json_message = message.to_json()

    print("\nJSON Message:")
    print(json_message)

    received_message = GossipMessage.from_json(json_message)

    print("\nReceived Gossip Message:")
    print(received_message)

