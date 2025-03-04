from common.redis.redis_connection import RedisConnection


class TaskQueue(RedisConnection):
    """
    Класс взаимодействия с упорядоченной очередью задач в redis.
    """

    def __init__(self, queue_name: str) -> None:
        """
        Инициализация класса взаимодействия с упорядоченной очередью задач в redis.
        """

        super().__init__()
        self.queue_name = queue_name

    async def get_all_tasks_from_queue(self) -> list[str]:
        """
        Получения списка всех задач в очереди.
        """

        return await self.redis_connect.zrange(self.queue_name, 0, -1)

    async def pop_next_task_from_queue(self) -> tuple:
        """
        Взятие следующей задачи из очереди по приоритету.
        """

        next_task_data = await self.redis_connect.zpopmin(self.queue_name)
        if next_task_data:
            next_task_name, next_task_run = next_task_data[0]
        else:
            next_task_name, next_task_run = None, None

        return next_task_name, next_task_run

    async def add_task_to_queue(self, task_name: str, task_launch: float) -> None:
        """
        Добавление задачи в очередь или обновление времени выполнения существующей задачи в очереди.
        """

        task_data = {task_name: task_launch}
        await self.redis_connect.zadd(self.queue_name, task_data)
