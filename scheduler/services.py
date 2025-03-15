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
    Creating a log (main scheduler service).
    """

    log_type = await LogType.get_unique(session=session, data={'name': log_type_name})

    new_log = Log(log_type_id=log_type.id, description=description)
    session.add(new_log)

    await session.commit()


async def add_task_to_queue(task: Row, task_queue: TaskQueue, task_launch: datetime.datetime) -> None:
    """
    Adding a shared task to the redis queue (auxiliary scheduler service).
    """

    formated_task_launch = task_launch.replace(tzinfo=datetime.timezone.utc).timestamp()

    await task_queue.add_task_to_queue(task_name=task.name, task_launch=formated_task_launch)
    logger.info(f"Added task to redis queue {task.name, task_launch.isoformat()}")


async def add_task_to_history(session: AsyncSession, task: Row, task_launch: datetime.datetime) -> None:
    """
    Adding a shared or personal task to the task history (auxiliary scheduler service).
    """

    planned_task_history_status = await TaskHistoryStatus.get_unique(session=session, data=dict(name="planned"))
    new_task_history = TaskHistory(
        start_date=task_launch,
        task_id=task.id,
        status_id=planned_task_history_status.id,
    )
    session.add(new_task_history)

    await session.commit()
    logger.info(f"Added a new entry to the task history with the status 'planned' for the task ({task.name})")


async def init_common_tasks(session: AsyncSession, task_queue: TaskQueue) -> None:
    """
    Initializing shared tasks in the redis queue and in the DB (main scheduler service).
    """

    tasks_in_db = await Task.get_all(session=session)
    logger.info(f"Received tasks from DB - {len(tasks_in_db)}")

    tasks_in_redis = await task_queue.get_all_tasks_from_queue()
    logger.info(f"Received tasks from redis - {len(tasks_in_redis)}\n")

    initiated_tasks = [task for task in tasks_in_db if task.name not in tasks_in_redis]
    for task in initiated_tasks:
        task_launch = datetime.datetime.utcnow() + datetime.timedelta(seconds=task.period)
        await add_task_to_queue(task=task, task_queue=task_queue, task_launch=task_launch)
        await add_task_to_history(session=session, task=task, task_launch=task_launch)
        logger.info(f"Shared task ({task.name}) initialized\n\n")


async def get_planned_common_task_by_task_name(session: AsyncSession, task_name: str) -> Row | None:
    """
    Getting a shared task with a history entry in the "planned" status (auxiliary scheduler service).
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
    Getting a shared task with a history entry in the "planned" status or a scheduler cycle delay
    (main scheduler service).
    """

    base_delay = server_config["scheduler_base_delay"]

    task_name, task_time_launch = await task_queue.pop_next_task_from_queue()
    if not task_name or not task_time_launch:
        logger.info("No shared tasks in redis queue\n")
        return None, base_delay

    logger.info(
        f"Next shared task in redis queue - "
        f"{task_name, datetime.datetime.utcfromtimestamp(task_time_launch).isoformat()}"
    )
    current_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).timestamp()
    if task_time_launch > current_timestamp:
        delay = min(task_time_launch - current_timestamp, base_delay)
        await task_queue.add_task_to_queue(task_name=task_name, task_launch=task_time_launch)
        logger.info(
            f"The shared task ({task_name}) is too early to execute, the task has been returned back to redis\n"
        )
        return None, delay

    planned_common_task = await get_planned_common_task_by_task_name(session=session, task_name=task_name)
    if not planned_common_task:
        logger.warning(
            f"No entry in task history with status 'planned' "
            f"for shared task ({task_name}), error log written\n"
        )
        return None, None

    return planned_common_task, None


async def update_task_history_status(session: AsyncSession, task_history_id: UUID, new_status: str) -> None:
    """
    Changing the status in the task execution history (auxiliary scheduler service).
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
    Executing a scheduler task (main scheduler service).
    """

    await update_task_history_status(
        session=session,
        task_history_id=task.task_history_id,
        new_status='in_progress',
    )
    logger.info(f"Updated task history entry from 'planned' status to 'in_progress' for task ({task.name})")

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
            description=f"There is no method to perform the task {task.name}",
        )
        logger.warning(
            f"There is no method for the task ({task.name}), "
            "error log written, task history entry updated from 'in_progress' to 'error'\n"
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
            description=f"Error executing task {task.name}."
                        f"Error text:\n'{exc}'\nError traceback:\n{traceback_info}",
        )
        logger.exception(
            f"Error starting method of task {task.name}, error log written, "
            f" updated task history entry with status 'in_progress' to 'error'\n"
        )
        return

    await update_task_history_status(
        session=session,
        task_history_id=task.task_history_id,
        new_status='done',
    )
    logger.info(f"Updated task history entry with status 'in_progress' to 'done' for task ({task.name})")


async def reschedule_task(session: AsyncSession, task: Row, task_queue: TaskQueue | None = None) -> None:
    """
    Scheduling a repeat execution of a scheduler task (main scheduler service).
    """

    task_launch = task.start_date + datetime.timedelta(seconds=task.period)

    await add_task_to_queue(task=task, task_queue=task_queue, task_launch=task_launch)
    await add_task_to_history(session=session, task=task, task_launch=task_launch)

    logger.info(f"Shared task ({task.name}) rescheduled\n")
