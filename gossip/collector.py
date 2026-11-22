"""Collect CPU and memory usage of the local node."""

import psutil


def collect_system_load():
    """Collect the current CPU usage and available RAM."""

    cpu_usage = psutil.cpu_percent(interval=0.1)

    memory = psutil.virtual_memory()

    return {
        "cpu_usage": round(cpu_usage, 2),
        "total_ram_gb": round(
            memory.total / (1024 ** 3), 2
        ),
        "free_ram_gb": round(
            memory.available / (1024 ** 3), 2
        ),
    }
