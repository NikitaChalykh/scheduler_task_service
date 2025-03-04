import logging
import asyncio
import datetime
import traceback
from uuid import UUID

from sqlalchemy import select, delete, update, Row
from sqlalchemy.ext.asyncio import AsyncSession

from common import server_config
from common.redis.task_queue import TaskQueue
from common.models.models import LogType, Log, Task, TaskHistory, TaskHistoryStatus
from tasks import example_task_1, example_task_2, example_task_3

logger = logging.getLogger(__name__)

task_mapping = {
    'example_task_1': example_task_1,
    'example_task_2': example_task_2,
    'example_task_3': example_task_3,
}


async def create_log(session: AsyncSession, log_type_name: str, description: str) -> None:
    """
    Создание лога (основной сервис scheduler).
    """

    log_type = await LogType.get_unique(session=session, data={'name': log_type_name})

    new_log = Log(log_type_id=log_type.id, description=description)
    session.add(new_log)

    await session.commit()


async def add_task_to_queue(task: Row, task_queue: TaskQueue, task_launch: datetime.datetime) -> None:
    """
    Добавление общей задач в очередь redis (вспомогательный сервис scheduler).
    """

    formated_task_launch = task_launch.replace(tzinfo=datetime.timezone.utc).timestamp()

    await task_queue.add_task_to_queue(task_name=task.name, task_launch=formated_task_launch)
    logger.info(f"Добавлена задача в очередь redis {task.name, task_launch.isoformat()}")


async def add_task_to_history(session: AsyncSession, task: Row, task_launch: datetime.datetime) -> None:
    """
    Добавление общей или персональной задачи в историю задач (вспомогательный сервис scheduler).
    """

    planned_task_history_status = await TaskHistoryStatus.get_unique(session=session, data=dict(name="planned"))
    new_task_history = TaskHistory(
        start_date=task_launch,
        task_id=task.id,
        status_id=planned_task_history_status.id,
    )
    session.add(new_task_history)

    await session.commit()
    logger.info(f"Добавлена новая запись в историю задач со статусом 'planned' для задачи ({task.name})")


async def init_common_tasks(session: AsyncSession, task_queue: TaskQueue) -> None:
    """
    Инициализации общих задач в очереди redis и в БД (основной сервис scheduler).
    """

    tasks_in_db = await Task.get_all(session=session)
    logger.info(f"Получено задач из БД - {len(tasks_in_db)}")

    tasks_in_redis = await task_queue.get_all_tasks_from_queue()
    logger.info(f"Получено задач из redis - {len(tasks_in_redis)}\n")

    initiated_tasks = [task for task in tasks_in_db if task.name not in tasks_in_redis]
    for task in initiated_tasks:
        task_launch = datetime.datetime.utcnow() + datetime.timedelta(seconds=task.period)
        await add_task_to_queue(task=task, task_queue=task_queue, task_launch=task_launch)
        await add_task_to_history(session=session, task=task, task_launch=task_launch)
        logger.info(f"Общая задача ({task.name}) инициализирована\n")


async def get_planned_common_task_by_task_name(session: AsyncSession, task_name: str) -> Row | None:
    """
    Получение общей задачи с записью в истории в статусе "planned" (вспомогательный сервис scheduler).
    """

    stm = select(
        Task.id,
        Task.name,
        Task.period,
        TaskHistory.id.label("task_history_id"),
        TaskHistory.start_date,
    ).join(
        Task, Task.id == TaskHistory.task_id,
    ).join(
        TaskHistoryStatus, TaskHistoryStatus.id == TaskHistory.status_id,
    ).where(
        Task.name == task_name,
        TaskHistoryStatus.name == "planned",
    )
    result = await session.execute(stm)
    task_history_obj = result.first()

    return task_history_obj


async def get_actual_planned_common_task_or_delay(session: AsyncSession, task_queue: TaskQueue) -> tuple:
    """
    Получение общей задачи с записью в истории в статусе "planned" или задержки цикла scheduler
    (основной сервис scheduler).
    """

    base_delay = server_config["scheduler_base_delay"]

    task_name, task_time_launch = await task_queue.pop_next_task_from_queue()
    if not task_name or not task_time_launch:
        logger.info("Нет общих задач в очереди redis\n")
        return None, base_delay

    logger.info(
        f"Следующая общая задача в очереди redis - "
        f"{task_name, datetime.datetime.utcfromtimestamp(task_time_launch).isoformat()}"
    )
    current_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).timestamp()
    if task_time_launch > current_timestamp:
        delay = min(task_time_launch - current_timestamp, base_delay)
        await task_queue.add_task_to_queue(task_name=task_name, task_launch=task_time_launch)
        logger.info(f"Общую задачу ({task_name}) рано выполнять, задача возвращена обратно в redis\n")
        return None, delay

    planned_common_task = await get_planned_common_task_by_task_name(session=session, task_name=task_name)
    if not planned_common_task:
        logger.warning(
            f"Нет записи в истории задач со статусом 'planned' "
            f"для общей задачи ({task_name}), записан лог ошибки\n"
        )
        return None, None

    return planned_common_task, None


async def update_task_history_status(session: AsyncSession, task_history_id: UUID, new_status: str) -> None:
    """
    Изменение статуса в истории выполнения задач.
    """

    task_history_status = await TaskHistoryStatus.get_unique(session=session, data=dict(name=new_status))

    stm = (
        update(TaskHistory)
        .values(status_id=task_history_status.id)
        .where(TaskHistory.id == task_history_id)
    )
    await session.execute(stm)
    await session.commit()


async def run_task(session: AsyncSession, task: Row) -> None:
    """
    Выполнение задачи scheduler (основной сервис scheduler).
    """

    await update_task_history_status(
        session=session,
        task_history_id=task.task_history_id,
        new_status='in_progress',
    )
    logger.info(f"Обновлена запись в истории задач со статусом 'planned' на 'in_progress' для задачи ({task.name})")

    task_func = task_mapping.get(task.name, False)
    if not task_func:
        await update_task_history_status(
            session=session,
            task_history_id=task.task_history_id,
            new_status='error',
        )
        await create_log(
            session=session,
            log_type_name="task_fail",
            description=f"Отсутствует метод для выполнения задачи {task.name}",
        )
        logger.warning(
            f"Нет метода для задачи ({task.name}), "
            "записан лог ошибки, обновлена запись в истории задач со статусом 'in_progress' на 'error'\n"
        )
        return

    try:
        await task_func(session=session)
    except Exception as exc:
        await update_task_history_status(
            session=session,
            task_history_id=task.task_history_id,
            new_status='error',
        )
        traceback_info = ''.join(traceback.format_tb(exc.__traceback__))
        await create_log(
            session=session,
            log_type_name="task_fail",
            description=f"Ошибка выполнения задачи {task.name}."
                        f"Текст ошибки:\n'{exc}'\nТрейсбек ошибки:\n{traceback_info}",
        )
        logger.exception(
            f"Ошибка запуска метода задачи {task.name}, записан лог ошибки, "
            f"обновлена запись в истории задач со статусом 'in_progress' на 'error'\n"
        )
        return

    await update_task_history_status(
        session=session,
        task_history_id=task.task_history_id,
        new_status='done',
    )
    logger.info(f"Обновлена запись в истории задач со статусом 'in_progress' на 'done' для задачи ({task.name})")


async def reschedule_task(session: AsyncSession, task: Row, task_queue: TaskQueue | None = None) -> None:
    """
    Планирование повторного выполнения задачи scheduler (основной сервис scheduler).
    """

    task_launch = task.start_date + datetime.timedelta(seconds=task.period)

    await add_task_to_queue(task=task, task_queue=task_queue, task_launch=task_launch)
    await add_task_to_history(session=session, task=task, task_launch=task_launch)

    logger.info(f"Общая задача ({task.name}) повторно запланирована\n")
