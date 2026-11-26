import cloudpickle
import base64


def serialize_task(func, args=(), kwargs=None):
    """Convert a Python function and its arguments into a string."""
    if kwargs is None:
        kwargs = {}

    task_data = {
        "func": func,
        "args": args,
        "kwargs": kwargs
    }

    serialized = cloudpickle.dumps(task_data)
    return base64.b64encode(serialized).decode("utf-8")


def deserialize_task(data):
    """Convert the string back into a Python function and its arguments."""
    serialized = base64.b64decode(data.encode("utf-8"))
    return cloudpickle.loads(serialized)