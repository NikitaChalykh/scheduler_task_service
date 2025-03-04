from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from common import db_config

engine = create_async_engine(db_config["database_url"])
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator:
    """
    Инициализация асинхронной сессии к БД.
    """

    async with async_session() as session:
        yield session
