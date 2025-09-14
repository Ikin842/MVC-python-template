import json
import time
import typer
from loguru import logger
from config.base import settings
from config import kafka_config
from models.base_mapper import BaseMapper
from service import elastic_service, postgres_service

app = typer.Typer()

@app.command()
def run():
    Consumer().main()

class Consumer:
    def __init__(self) -> None:
        self.__es_service = elastic_service
        self.__pg_service = postgres_service
        self.__results: dict = {}

    def main(self):
        consumer = kafka_config.consumer_config()
        logger.info(f"Start Consuming...[{settings.KAFKA_CONSUMER_TOPIC}]")

        try:
            while True:
                start_time = time.time()
                messages = consumer.poll(5.0)

                for _, message_list in messages.items():
                    for message in message_list:
                        raw = message.value

                        results = BaseMapper(raw).get_results()
                        self.__results.update({
                            table_name: self.__results.get(table_name, []) + result
                            for table_name, result in results.items()}
                        )

                    self.__pg_service.connect()
                    for table_name, result in self.__results.items():
                        row_count = self.__pg_service.ingest(result, table_name)
                        logger.info("Ingested {}: {}", table_name, row_count)

                    logger.info("=======================================")
                    logger.info(f"consumer {settings.KAFKA_CONSUMER_TOPIC} {len(message_list)}")
                    logger.info("Time: {}", time.time() - start_time)
                    logger.info("=======================================")
                    self.__pg_service.close()

        except KeyboardInterrupt:
            print("Interrupt Keyboard")
        except Exception as err:
            raise err

if __name__ == "__main__":
    app()