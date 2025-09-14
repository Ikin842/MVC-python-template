from config.base import settings
from config.s3_config import S3Config
from config.elastic_config import ElasticConfig
from config.kafka_config import KafkaConfig
from config.rabbitmq_config import RabbitConfig
from config.redis_config import RedisConfig
from config.beanstalk_config import BeanstalkConfig

rmq_config = RabbitConfig(**settings.dict())
kafka_config = KafkaConfig(**settings.dict())
s3_connection = S3Config(**settings.dict()).connection_s3()
es_connection = ElasticConfig(**settings.dict())
redis_connection = RedisConfig(**settings.dict()).redis_conn()
beanstalk_connection = BeanstalkConfig(**settings.dict()).beanstalk_conn()
