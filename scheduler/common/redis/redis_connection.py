import redis.asyncio as asyncio_redis

from common import redis_config


class RedisConnection:
    """
    Base class for asynchronous interaction with redis.
    """

    def __init__(self) -> None:
        """
        Initialization of the base class for connecting to redis.
        """

        redis_connect = asyncio_redis.client.Redis(
            host=redis_config["host"],
            port=redis_config["port"],
            username=redis_config["username"],
            password=redis_config["password"],
            encoding="utf-8",
            decode_responses=True,
        )
        self.redis_connect = redis_connect
