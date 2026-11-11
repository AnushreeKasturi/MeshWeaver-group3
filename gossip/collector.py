
import psutil


def collect_system_load():
    """Collect CPU and available RAM information."""

    cpu_usage = psutil.cpu_percent(interval=1)

    memory = psutil.virtual_memory()

    load_info = {
        "cpu_usage": round(cpu_usage, 2),
        "total_ram_gb": round(memory.total / (1024 ** 3), 2),
        "free_ram_gb": round(memory.available / (1024 ** 3), 2)
    }

    return load_info


if __name__ == "__main__":
    print(collect_system_load())
