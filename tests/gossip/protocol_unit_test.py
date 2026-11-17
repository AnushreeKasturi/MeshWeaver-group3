
import unittest
import json

from gossip.protocol import GossipMessage, create_gossip_message
class TestGossipProtocol(unittest.TestCase):

    def setUp(self):
        self.load_info = {
            "cpu_usage": 30.5,
            "free_ram_gb": 4.0
        }

    def test_message_creation(self):
        message = create_gossip_message("node_1", self.load_info)

        self.assertEqual(message.node_id, "node_1")
        self.assertEqual(message.cpu_usage, 30.5)
        self.assertEqual(message.free_ram_gb, 4.0)
        self.assertIsInstance(message.timestamp, float) 
    def test_json_serialization(self):
        message = create_gossip_message("node_1", self.load_info)

        json_data = message.to_json()
        data = json.loads(json_data)

        self.assertEqual(data["node_id"], "node_1")
        self.assertEqual(data["cpu_usage"], 30.5)
        self.assertEqual(data["free_ram_gb"], 4.0)

    
    def test_json_deserialization(self):
        message = create_gossip_message("node_1", self.load_info)

        json_data = message.to_json()
        restored = GossipMessage.from_json(json_data)

        self.assertEqual(restored.node_id, message.node_id)
        self.assertEqual(restored.cpu_usage, message.cpu_usage)
        self.assertEqual(restored.free_ram_gb, message.free_ram_gb)
        self.assertEqual(restored.timestamp, message.timestamp)


if __name__ == "__main__":
    unittest.main()
