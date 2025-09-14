import json
import typer
from loguru import logger
from config import rmq_config

app = typer.Typer()

@app.command()
def run():
    Consumer().consume_message()

class Consumer:
    def __init__(self):
        self.rabit_config = rmq_config
        self.chanel = self.rabit_config.open_connection()

    def consume_message(self) -> None:
        self.rabit_config.generate_queue()
        self.chanel.basic_qos(prefetch_count=1)

        try:
            logger.info(f"Start Consuming Queue : {self.rabit_config.get_queue()}")
            for method_frame, properties, body in self.chanel.consume(
                    self.rabit_config.get_queue(), inactivity_timeout=2, auto_ack=False
            ):
                if body is None:
                    continue
                try:
                    message = json.loads(body.decode('utf-8'))
                    logger.info(message)

                except json.JSONDecodeError:
                    logger.error("Failed to decode message body.")
                except Exception as e:
                    logger.error(f"Unexpected error during processing: {e}")
                finally:
                    self.chanel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except KeyboardInterrupt:
            logger.info("🛑 Stop Consuming ... ")
            self.chanel.stop_consuming()
            logger.info("Consumer Stopped.")

        except Exception as error:
            logger.error(f"❌ Unexpected error: {str(error)}")
            raise

        finally:
            self.rabit_config.close_connection()

if __name__ == '__main__':
    app()
