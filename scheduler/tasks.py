import asyncio
import logging

logger = logging.getLogger(__name__)


async def example_task_1() -> None:
    """
    Example task 1 (scheduler task service).
    """

    logger.info("Starting task 'example_task'")

    await asyncio.sleep(1)

    logger.info("Task 'example_task' completed successfully")


async def example_task_2() -> None:
    """
    Example task 2 (scheduler task service).
    """

    logger.info("Starting task 'example_task_2'")

    await asyncio.sleep(2)

    logger.info("Task 'example_task_2' completed successfully")


async def example_task_3() -> None:
    """
    Example task 3 (task scheduler service).
    """

    logger.info("Starting task 'example_task_3'")

    await asyncio.sleep(3)

    logger.info("Task 'example_task_3' completed successfully")