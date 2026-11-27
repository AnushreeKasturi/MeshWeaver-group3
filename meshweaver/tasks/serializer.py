import cloudpickle


def serialize_task(func, args=(), kwargs=None):
    if kwargs is None:
        kwargs = {}

    return cloudpickle.dumps((func, args, kwargs))


def deserialize_task(data):
    return cloudpickle.loads(data)