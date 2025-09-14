import redis
from loguru import logger

class RedisConfig:
    def __init__(self, **context):
        self.__host = context['REDIS_HOST']
        self.__port = context['REDIS_PORT']
        self.__db = context['REDIS_DB']

    def redis_conn(self):
        try:
            r = redis.Redis(
                host=self.__host,
                port=self.__port,
                db=self.__db,
                password=None,
                decode_responses=True
            )
            if r.ping():
                return r

        except redis.ConnectionError as e:
            logger.error(f"Error Redis Connection : {e}")
            return None