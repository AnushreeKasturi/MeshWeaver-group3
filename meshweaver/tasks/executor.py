import cloudpickle
from meshweaver.protocol import TaskSubmit, TaskResult


def execute_task(task: TaskSubmit) -> TaskResult:
    try:
        func, args, kwargs = cloudpickle.loads(task.blob)

        result = func(*args, **kwargs)

        return TaskResult(
            task_id=task.task_id,
            status="done",
            result=cloudpickle.dumps(result)
        )

    except Exception as e:
        return TaskResult(
            task_id=task.task_id,
            status="failed",
            error=str(e)
        )