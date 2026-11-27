"""Task serialization, execution and fault tolerance (member E).

Handles TASK_SUBMIT (payload ``protocol.TaskSubmit``, cloudpickle ``blob``),
replies with TASK_RESULT, and sends HEARTBEAT every 2 seconds while running.
"""


from .executor import execute_task
from ..node import Module
from ..protocol import MessageType


class Tasks(Module):
    name = "tasks"

    def handlers(self):
        return {
            MessageType.TASK_SUBMIT: self.on_task_submit,
        }

    async def on_task_submit(self, msg, addr):
        task = msg.parsed()
        result = execute_task(task)

        return msg.reply(
            MessageType.TASK_RESULT,
            self.node.id_hex,
            result
        )