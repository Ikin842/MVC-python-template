import pandas as pd
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import create_engine, URL, text
from loguru import logger

class MySQLService:
    def __init__(self, **context):
        self.__dbase = context['MYSQL_DATABASE']
        self.__host = context['MYSQL_HOST']
        self.__port = context['MYSQL_PORT']
        self.__user = context['MYSQL_USERNAME']
        self.__pass = context['MYSQL_PASSWORD']
        self.__conn = None

    def connect(self):
        url_object = URL.create(
            "mysql+pymysql",
            username=self.__user,
            password=self.__pass,
            host=self.__host,
            database=self.__dbase,
            port=self.__port
        )
        db = create_engine(url_object)
        self.__conn = db.connect()

    @staticmethod
    def _insert_on_duplicate_upsert(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_stmt = insert(table.table).values(data)
        update_dict = {
            col.name: insert_stmt.inserted[col.name]
            for col in table.table.columns
            if not col.primary_key
        }

        upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)
        result = conn.execute(upsert_stmt)
        return result.rowcount

    def ingest(self, df, table_name: str):
        try:
            if df.empty:
                print("DataFrame is empty after filtering. Nothing to ingest.")
                return 0

            df = df.drop_duplicates(subset=['id'], keep='last')
            row_count = df.to_sql(
                table_name,
                self.__conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method=self._insert_on_duplicate_upsert
            )
            self.__conn.commit()
            return row_count

        except Exception as e:
            logger.error(f"Error ingesting data: {e}")
            return 0

    def close(self):
        self.__conn.close()

    def read_query(self, query):
        df = pd.read_sql_query(text(query), self.__conn)
        return df
