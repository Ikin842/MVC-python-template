import json
import pika
from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType

class RabbitConfig:
    def __init__(self, **context):
        self.__host = context['RABBITMQ_HOST']
        self.__port = context['RABBITMQ_PORT']
        self.__user = context['RABBITMQ_USERNAME']
        self.__pass = context['RABBITMQ_PASSWORD']
        self.__vhost = context['RABBITMQ_VHOST']
        self.__exchange = context['RABBITMQ_EXCHANGE']
        self.__queue = context['RABBITMQ_QUEUE']
        self.__routing_key = context['RABBITMQ_ROUTING_KEY']
        self.connect: BlockingConnection = None
        self.channel: BlockingChannel = None

    def credentials(self):
        return pika.PlainCredentials(
            username=self.__user,
            password=self.__pass
        )

    def parameters(self):
        return pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            virtual_host=self.__vhost,
            credentials=self.credentials(),
            heartbeat=2400,
            blocked_connection_timeout=2400
        )

    def open_connection(self):
        self.connect = pika.BlockingConnection(parameters=self.parameters())
        self.channel = self.connect.channel()
        return self.channel

    def generate_queue(self):
        channel = self.open_connection()
        channel.exchange_declare(
            exchange=self.__exchange,
            exchange_type=ExchangeType.direct,
            durable=True
        )
        args = {
            'x-dead-letter-exchange': self.__exchange,
            'x-dead-letter-routing-key': self.__routing_key
        }

        channel.queue_declare(queue=self.__queue, durable=True, arguments=args)
        channel.queue_bind(

            queue=self.__queue,
            exchange=self.__exchange,
            routing_key=self.__routing_key
        )

    def produce_message(self, message: dict):
        self.channel.basic_publish(
            exchange=self.__exchange,
            routing_key=self.__routing_key,
            body=json.dumps(message).encode('utf-8')
        )

    def close_connection(self):
        self.channel.close()
        self.connect.close()

    def get_queue(self):
        return self.__queue

