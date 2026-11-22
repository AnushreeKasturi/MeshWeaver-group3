
from unittest.mock import patch, Mock

from meshweaver.gossip.collector import collect_system_load


def test_collect_system_load():
    fake_memory = Mock()
    fake_memory.total = 8 * (1024 ** 3)
    fake_memory.available = 3 * (1024 ** 3)

    with patch(
        "meshweaver.gossip.collector.psutil.cpu_percent",
        return_value=25.5
    ), patch(
        "meshweaver.gossip.collector.psutil.virtual_memory",
        return_value=fake_memory
    ):
        result = collect_system_load()

    assert result["cpu_usage"] == 25.5
    assert result["total_ram_gb"] == 8.0
    assert result["free_ram_gb"] == 3.0
