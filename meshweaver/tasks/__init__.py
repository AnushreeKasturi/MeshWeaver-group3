"""Task serialization, execution and fault tolerance (member E).

Handles TASK_SUBMIT (payload ``protocol.TaskSubmit``, cloudpickle ``blob``),
replies with TASK_RESULT, and sends HEARTBEAT every 2 seconds while running.
"""
