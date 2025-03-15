import uuid
from typing import Sequence

from sqlalchemy import Column, select, update, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BaseModel(Base):
    """
    Base class of ORM models.
    """

    __abstract__ = True

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid())

    @classmethod
    async def get_all(cls, session: AsyncSession) -> Sequence[Base]:
        """
        Returns all records.
        """

        stm = await session.execute(
            select(cls)
        )
        return stm.scalars().all()

    @classmethod
    async def get_unique(cls, session: AsyncSession, data: dict) -> Base | None:
        """
        Getting an object by unique parameters. Each model has its own parameters.
        """
        pass

    @classmethod
    async def obj_set(cls, session: AsyncSession, data: dict) -> uuid.UUID | Column:
        """
        The method creates records if there are none.
        """

        obj = await cls.get_unique(session=session, data=data)
        if obj:
            return obj.id
        else:
            new_obj = cls(**data)
            session.add(new_obj)
            await session.commit()
            return new_obj.id
