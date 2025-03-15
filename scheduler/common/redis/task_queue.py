from common.redis.redis_connection import RedisConnection


class TaskQueue(RedisConnection):
    """
    Redis ordered task queue interaction class.
    """

    def __init__(self, queue_name: str) -> None:
        """
        Initialize the redis ordered task queue interaction class.
        """

        super().__init__()
        self.queue_name = queue_name

    async def get_all_tasks_from_queue(self) -> list[str]:
        """
        Get a list of all tasks in the queue.
        """

        return await self.redis_connect.zrange(self.queue_name, 0, -1)

    async def pop_next_task_from_queue(self) -> tuple:
        """
        Take the next task from the queue by priority.
        """

        next_task_data = await self.redis_connect.zpopmin(self.queue_name)
        if next_task_data:
            next_task_name, next_task_run = next_task_data[0]
        else:
            next_task_name, next_task_run = None, None

        return next_task_name, next_task_run

    async def add_task_to_queue(self, task_name: str, task_launch: float) -> None:
        """
        Add a task to the queue or update the execution time of an existing task in the queue.
        """

        task_data = {task_name: task_launch}
        await self.redis_connect.zadd(self.queue_name, task_data)
