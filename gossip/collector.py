import psutil
import time
BYTES_TO_GB = 1024 ** 3


def convert_bytes_to_gb(bytes_value):
   
    return round(bytes_value / BYTES_TO_GB, 2)
def collect_cpu_usage(interval=0.1):
    """Collect CPU utilization percentage."""
    if interval <= 0:
        raise ValueError("Interval must be greater than zero")

    cpu_usage = psutil.cpu_percent(interval=interval)

    return round(cpu_usage, 2)


def collect_memory_info():
    memory = psutil.virtual_memory()

    total_ram = convert_bytes_to_gb(memory.total)
    used_ram = convert_bytes_to_gb(memory.used)
    free_ram = convert_bytes_to_gb(memory.available)

    return {
        "total_ram_gb": total_ram,
        "used_ram_gb": used_ram,
        "free_ram_gb": free_ram
    }


def collect_system_load():
    
    start_time = time.time()

    cpu_usage = collect_cpu_usage()
    memory_info = collect_memory_info()

    load_info = {
        "cpu_usage": cpu_usage,
        "total_ram_gb": memory_info["total_ram_gb"],
        "used_ram_gb": memory_info["used_ram_gb"],
        "free_ram_gb": memory_info["free_ram_gb"]
    }

    collection_time = round(time.time() - start_time, 3)

    load_info["collection_time_seconds"] = collection_time

    return load_info


def display_system_load(load_info):

    print(f"CPU Usage       : {load_info['cpu_usage']}%")
    print(f"Total RAM       : {load_info['total_ram_gb']} GB")
    print(f"Used RAM        : {load_info['used_ram_gb']} GB")
    print(f"Available RAM   : {load_info['free_ram_gb']} GB")

    print(
        f"Collection Time : "
        f"{load_info['collection_time_seconds']} seconds"
    )


if __name__ == "__main__":

    system_load = collect_system_load()

    display_system_load(system_load)
