import unittest
from unittest.mock import patch, MagicMock
from gossip.collector import collect_system_load
class TestCollector(unittest.TestCase):

    @patch("gossip.collector.psutil.virtual_memory")
    @patch("gossip.collector.psutil.cpu_percent")
  
    def test_cpu_usage(self, mock_cpu, mock_memory):
        mock_cpu.return_value = 25.5
        memory = MagicMock()
        memory.total = 8 * (1024 ** 3)
        memory.available = 4 * (1024 ** 3)
        mock_memory.return_value = memory
        result = collect_system_load()
        self.assertEqual(result["cpu_usage"], 25.5)
    @patch("gossip.collector.psutil.virtual_memory")
    @patch("gossip.collector.psutil.cpu_percent")
  
    def test_total_ram(self, mock_cpu, mock_memory):
        mock_cpu.return_value = 20.0

        memory = MagicMock()
        memory.total = 8 * (1024 ** 3)
        memory.available = 4 * (1024 ** 3)
        mock_memory.return_value = memory
        result = collect_system_load()
        self.assertEqual(result["total_ram_gb"], 8.0)

    @patch("gossip.collector.psutil.virtual_memory")
    @patch("gossip.collector.psutil.cpu_percent")
  
    def test_free_ram(self, mock_cpu, mock_memory):
        mock_cpu.return_value = 20.0

        memory = MagicMock()
        memory.total = 8 * (1024 ** 3)
        memory.available = 4 * (1024 ** 3)
        mock_memory.return_value = memory

        result = collect_system_load()

        self.assertEqual(result["free_ram_gb"], 4.0)


if __name__ == "__main__":
    unittest.main()
