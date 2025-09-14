import json
import typer
import time
from loguru import logger
from config import settings
from config import s3_connection
from models.base_mapper import BaseMapper
from service import postgres_service

app = typer.Typer()

@app.command()
def run():
    AutoIngest().main()

class AutoIngest:
    def __init__(self):
        self.conn_s3 = s3_connection
        self.ingest = postgres_service

    def main(self):
        bucket_name = settings.BUCKET_NAME
        folder_path = settings.S3_FOLDER_PATH

        files = self.conn_s3.glob(f's3://{bucket_name}/{folder_path}**')
        list_path = [file for file in files if file.endswith('.json')]

        for item in list_path:
            with self.conn_s3.open(item, 'r') as f:
                start_time = time.time()
                data = json.load(f)
                result = BaseMapper(data).get_results()

                for table_name, result in result.items():
                    count = self.ingest.insert_data(result, table_name)
                    logger.info("Ingested {}: {}", table_name, count)

                logger.info("===============================")
                logger.info("end time : {}", time.time() - start_time)
                logger.info("===============================")

if __name__ == "__main__":
    app()
