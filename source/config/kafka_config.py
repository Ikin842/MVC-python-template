import json
from kafka import KafkaConsumer, KafkaProducer

class KafkaConfig:
    def __init__(self, **context):
        self.__kafka_consumer_topic = context['KAFKA_CONSUMER_TOPIC']
        self.__kafka_boostrap_server = context['KAFKA_BOOTSTRAP_SERVER']
        self.__kafka_group_id = context['KAFKA_GROUP_ID']
        self.__kafka_auto_offset_reset = context['KAFKA_AUTO_OFFSET_RESET']
        self.__kafka_max_poll_records = context['KAFKA_MAX_POLL_RECORDS']
        self.__kafka_target_topic = context['KAFKA_TARGET_TOPIC']
        self.__producer = self.produce_config()

    def consumer_config(self) -> KafkaConsumer:
        consumer = KafkaConsumer(
            bootstrap_servers=self.__kafka_boostrap_server.split(','),
            group_id=self.__kafka_group_id,
            max_poll_records=self.__kafka_max_poll_records,
            auto_offset_reset=self.__kafka_auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        consumer.subscribe(topics=self.__kafka_consumer_topic.split(','))
        return consumer

    def produce_config(self) -> KafkaProducer:
        producer = KafkaProducer(
            bootstrap_servers=self.__kafka_boostrap_server.split(',')
        )
        return producer

    def send_msg(self, message):
        self.__producer.send(self.__kafka_target_topic, value=json.dumps(message).encode('utf-8'))
        self.__producer.flush()
