import json
import time
import traceback
import typer
from loguru import logger
from config import beanstalk_connection

app = typer.Typer()

@app.command()
def run() -> None:
    BeanstalkConsumer().main()

class BeanstalkConsumer:
    def __init__(self) -> None:
        self.tube: str = "tube name"
        self.beanstalk_conn = beanstalk_connection

    def main(self) -> None:
        self.beanstalk_conn.watch(tube=self.tube)
        logger.info(f"Started consuming from tube: {self.tube}")

        while True:
            job = self.beanstalk_conn.reserve(timeout=900)
            if not job:
                continue

            try:
                message = json.loads(job.body)
                if not message:
                    logger.warning("Received empty message. Deleting job.")
                    self.beanstalk_conn.delete(job)

            except Exception as e:
                self._handle_failed_job(job, e)

    def _handle_failed_job(self, job, error: Exception) -> None:
        logger.error(f"Failed to process job: {error}")
        logger.debug(traceback.format_exc())
        self.beanstalk_conn.bury(job)
        time.sleep(10)



if __name__ == "__main__":
    app()
