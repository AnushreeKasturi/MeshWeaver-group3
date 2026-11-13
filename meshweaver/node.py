"""Node runtime (member A, Week 2).

Will start every module on one asyncio event loop in this order:
transport -> DHT -> gossip -> scheduler -> tasks, register each module's
handlers on a shared ``protocol.Dispatcher``, and shut down by cancelling
their tasks in reverse order.
"""
