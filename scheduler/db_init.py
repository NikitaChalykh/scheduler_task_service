import logging
import asyncio

from sqlalchemy.ext.asyncio import AsyncSession

from common.db_sessions import async_session
from common.models.models import LogType, Task, TaskHistoryStatus

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def db_init(session: AsyncSession):
    """
    Initialization of the database with initial values.
    """

    logger.info("Create dictionary: incident log types")
    await LogType.obj_set(session=session, data={"name": "task_queue_fail"})
    await LogType.obj_set(session=session, data={"name": "task_fail"})


    logger.info("Create dictionary: task history statuses")
    await TaskHistoryStatus.obj_set(session=session, data={"name": "planned"})
    await TaskHistoryStatus.obj_set(session=session, data={"name": "in_progress"})
    await TaskHistoryStatus.obj_set(session=session, data={"name": "done"})
    await TaskHistoryStatus.obj_set(session=session, data={"name": "error"})


    logger.info("Create tasks")
    tasks = [
        {
            'name': 'example_task_1',
            'description': 'Пример задачи 1',
            'period': 60,
        },
        {
            'name': 'example_task_2',
            'description': 'Пример задачи 2',
            'period': 60 * 5,
        },
        {
            'name': 'example_task_3',
            'description': 'Пример задачи 3',
            'period': 60 * 10,
        },
    ]
    for item in tasks:
        await Task.obj_set(session=session, data=item)


async def async_main() -> None:
    """
    Method of initialization of the database with data with logging.
    """

    async with async_session() as session:
        try:
            await db_init(session)
        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    asyncio.run(async_main())
