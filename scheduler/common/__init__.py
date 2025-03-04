from common.settings import DBSettings, RedisSettings, ServerSettings

db_config: dict = DBSettings().dict()
redis_config: dict = RedisSettings().dict()
server_config: dict = ServerSettings().dict()
