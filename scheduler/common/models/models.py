from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, Column, ForeignKey, String, Text, DateTime, Integer, text
from sqlalchemy.dialects.postgresql import UUID

from common.models.base_models import BaseModel


class LogType(BaseModel):
    """
    Table of log types for the incident and accident log.
    """

    __tablename__ = "log_types"

    name = Column(String(255), nullable=False)

    @classmethod
    async def get_unique(cls, session: AsyncSession, data: dict) -> BaseModel | None:
        """
        Retrieving an object by unique parameters.
        """

        stm = select(cls).where(cls.name == data["name"])
        result = await session.execute(stm)

        return result.scalars().first()


class Log(BaseModel):
    """
    Table of logs for the incident and accident log.
    """

    __tablename__ = "logs"

    create_date = Column(DateTime(), nullable=False, server_default=text("timezone('UTC', now())"))
    log_type_id = Column(UUID, ForeignKey("log_types.id"), nullable=False)
    description = Column(Text(), nullable=False)


class Task(BaseModel):
    """
    Table with tasks for the scheduler.
    """

    __tablename__ = "tasks"

    name = Column(String(50), nullable=False, unique=True)
    description = Column(String(500), nullable=False, server_default="")
    period = Column(Integer, nullable=False)

    @classmethod
    async def get_unique(cls, session: AsyncSession, data: dict) -> BaseModel | None:
        """
        Retrieving an object by unique parameters.
        """

        stm = select(cls).where(cls.name == data["name"])
        result = await session.execute(stm)

        return result.scalars().first()


class TaskHistoryStatus(BaseModel):
    """
    Table of statuses for task execution history.
    """

    __tablename__ = "task_history_statuses"

    name = Column(String(50), nullable=False, unique=True)

    @classmethod
    async def get_unique(cls, session: AsyncSession, data: dict) -> BaseModel | None:
        """
        Retrieving an object by unique parameters.
        """

        stm = select(cls).where(cls.name == data["name"])
        result = await session.execute(stm)

        return result.scalars().first()


class TaskHistory(BaseModel):
    """
    Table with task execution history.
    """

    __tablename__ = "task_history"

    create_date = Column(DateTime(), nullable=False, server_default=text("timezone('UTC', now())"))
    start_date = Column(DateTime(), nullable=False, server_default=text("timezone('UTC', now())"))
    task_id = Column(UUID, ForeignKey("tasks.id"), nullable=False)
    status_id = Column(UUID, ForeignKey("task_history_statuses.id"), nullable=False)
