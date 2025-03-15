from pydantic import Field, ConfigDict, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DBSettings(BaseSettings):
    """
    DB settings.
    """

    host: str = Field("localhost", alias="POSTGRES_HOST")
    port: str = Field("5432", alias="POSTGRES_PORT")
    user: str = Field("postgres", alias="POSTGRES_USER")
    password: str = Field("postgres", alias="POSTGRES_PASSWORD")
    db_name: str = Field("postgres", alias="POSTGRES_DB")

    default_database_url: str = Field("postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}")
    default_alembic_database_url: str = Field("postgresql://{user}:{password}@{host}:{port}/{db_name}")

    @model_validator(mode="after")
    def set_database_url(cls, values):
        values.database_url = values.default_database_url.format(
            user=values.user,
            password=values.password,
            host=values.host,
            port=values.port,
            db_name=values.db_name,
        )
        values.alembic_database_url = values.default_alembic_database_url.format(
            user=values.user,
            password=values.password,
            host=values.host,
            port=values.port,
            db_name=values.db_name,
        )
        return values

    model_config = ConfigDict(extra='allow')


class RedisSettings(BaseSettings):
    """
    Redis settings.
    """

    host: str = Field("localhost", alias="REDIS_HOST")
    port: str = Field("6379", alias="REDIS_PORT")
    username: str = Field("", alias="REDIS_USER")
    password: str = Field("", alias="REDIS_PASSWORD")


class ServerSettings(BaseSettings):
    """
    Scheduler settings.
    """

    scheduler_base_delay: int = Field(300, env="SCHEDULER_BASE_DELAY")
    scheduler_queue_name: str = Field("scheduler_tasks", alias="SCHEDULER_QUEUE_NAME")
