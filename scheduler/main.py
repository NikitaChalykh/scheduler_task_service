import logging
import asyncio
import subprocess

from common import server_config
from common.redis.task_queue import TaskQueue
from common.db_sessions import async_session
from services import (
    create_log,
    init_common_tasks,
    get_actual_planned_common_task_or_delay,
    run_task,
    reschedule_task,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Initializing and starting the scheduler queue.
    """

    try:
        task_queue: TaskQueue = TaskQueue(queue_name=server_config["scheduler_queue_name"])
        async with async_session() as session:
            logger.info("Start initialization of shared tasks\n")
            await init_common_tasks(session=session, task_queue=task_queue)

        logger.info("The main scheduler loop has startedr\n")
        while True:
            async with async_session() as session:
                logger.info("Checking for shared tasks in redis queue\n")
                actual_planned_common_task, delay = await get_actual_planned_common_task_or_delay(
                    session=session,
                    task_queue=task_queue,
                )
                if actual_planned_common_task:
                    logger.info(
                        f"The next general task to be completed is - "
                        f"{actual_planned_common_task.name, actual_planned_common_task.start_date.isoformat()}"
                    )
                    await run_task(session=session, task=actual_planned_common_task)
                    await reschedule_task(session=session, task=actual_planned_common_task, task_queue=task_queue)
                if delay:
                    logger.info(f"The scheduler loop sleeps for {delay} seconds and starts again\n")
                    await asyncio.sleep(delay)

                logger.info("Restart the scheduler cycle\n")

    except Exception:
        async with async_session() as session:
            await create_log(
                session=session,
                log_type_name="task_queue_fail",
                description="The scheduler task queue is unavailable",
            )
        logger.exception(
            f"The scheduler task queue is unavailable, "
            f"the scheduler loop sleeps for {server_config['scheduler_base_delay']} seconds and restarts\n"
        )
        await asyncio.sleep(server_config["scheduler_base_delay"])
        await main()


if __name__ == "__main__":
    subprocess.Popen(["alembic", "upgrade", "head"])
    asyncio.run(main())
