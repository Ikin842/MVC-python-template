from pydantic_settings import BaseSettings

class Config(BaseSettings):

    ELASTICSEARCH_HOST: str
    ELASTICSEARCH_PORT:str
    ELASTICSEARCH_USERNAME: str
    ELASTICSEARCH_PASSWORD: str

    KAFKA_BOOTSTRAP_SERVER: str
    KAFKA_CONSUMER_TOPIC: str
    KAFKA_GROUP_ID: str
    KAFKA_AUTO_OFFSET_RESET: str
    KAFKA_MAX_POLL_RECORDS: int
    KAFKA_TARGET_TOPIC: str

    POSTGRE_NAME: str
    POSTGRE_HOST: str
    POSTGRE_PORT: int
    POSTGRE_USERNAME: str
    POSTGRE_PASSWORD: str
    POSTGRE_DATABASE: str

    S3_ENDPOINT: str
    ACCESS_KEY: str
    SECRET_KEY: str
    BUCKET_NAME: str
    S3_FOLDER_PATH: str

    RABBITMQ_HOST: str
    RABBITMQ_PORT: str
    RABBITMQ_USERNAME: str
    RABBITMQ_PASSWORD: str
    RABBITMQ_VHOST: str
    RABBITMQ_QUEUE: str
    RABBITMQ_ROUTING_KEY: str
    RABBITMQ_EXCHANGE: str

    MONGODB_CLIENT:str
    MONGODB_DATABASE:str
    MONGODB_COLLECTION: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

settings = Config()