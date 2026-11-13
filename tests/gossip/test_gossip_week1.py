import unittest
import json
from collector import collect_system_load
from protocol import GossipMessage, create_gossip_message


class TestCollector(unittest.TestCase):

    def test_cpu_usage(self):
        load = collect_system_load()

        self.assertGreaterEqual(load["cpu_usage"], 0)
        self.assertLessEqual(load["cpu_usage"], 100)

    def test_total_ram(self):
        load = collect_system_load()

        self.assertGreater(load["total_ram_gb"], 0)

    def test_free_ram(self):
        load = collect_system_load()

        self.assertGreater(load["free_ram_gb"], 0)
        self.assertLessEqual(
            load["free_ram_gb"],
            load["total_ram_gb"]
        )


class TestGossipProtocol(unittest.TestCase):

    def setUp(self):
        self.load = {
            "cpu_usage": 25.5,
            "total_ram_gb": 8.0,
            "free_ram_gb": 4.0
        }

    def test_message_creation(self):
        message = create_gossip_message("node-a", self.load)

        self.assertEqual(message.node_id, "node-a")
        self.assertEqual(message.cpu_usage, 25.5)
        self.assertEqual(message.free_ram_gb, 4.0)

    def test_json_serialization(self):
        message = create_gossip_message("node-a", self.load)

        json_message = message.to_json()

        data = json.loads(json_message)

        self.assertEqual(data["node_id"], "node-a")
        self.assertEqual(data["cpu_usage"], 25.5)

    def test_json_deserialization(self):
        message = create_gossip_message("node-a", self.load)

        json_message = message.to_json()

        received_message = GossipMessage.from_json(json_message)

        self.assertEqual(received_message.node_id, "node-a")
        self.assertEqual(received_message.cpu_usage, 25.5)
        self.assertEqual(received_message.free_ram_gb, 4.0)
        self.assertEqual(received_message.timestamp, message.timestamp)


if __name__ == "__main__":
    unittest.main()
