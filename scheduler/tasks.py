import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def example_task_1() -> None:
    """
    Пример задачи 1 (сервис задачи scheduler).
    """

    logger.info("Запускается задача 'example_task'")

    await asyncio.sleep(1)

    logger.info("Задача 'example_task' успешно завершена")


async def example_task_2() -> None:
    """
    Пример задачи 2 (сервис задачи scheduler).
    """

    logger.info("Запускается задача 'example_task_2'")

    await asyncio.sleep(2)

    logger.info("Задача 'example_task_2' успешно завершена")


async def example_task_3() -> None:
    """
    Пример задачи 3 (сервис задачи scheduler).
    """

    logger.info("Запускается задача 'example_task_3'")

    await asyncio.sleep(3)

    logger.info("Задача 'example_task_3' успешно завершена")