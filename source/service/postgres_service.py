import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, URL, text
from loguru import logger

class PostgresService:
    def __init__(self, **context):
        self.__dbase = context['POSTGRE_DATABASE']
        self.__host = context['POSTGRE_HOST']
        self.__port = context['POSTGRE_PORT']
        self.__user = context['POSTGRE_USERNAME']
        self.__pass = context['POSTGRE_PASSWORD']
        self.__conn = None

    def connect(self):
        url_object = URL.create(
            "postgresql",
            username=self.__user,
            password=self.__pass,
            host=self.__host,
            database=self.__dbase,
            port=self.__port
        )
        db = create_engine(url_object)
        self.__conn = db.connect()

    @staticmethod
    def _insert_on_conflict_upsert(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_statement = insert(table.table).values(data)
        conflict_update = insert_statement.on_conflict_do_update(
            constraint=f"{table.table.name}_pkey",
            set_={column.key: column for column in insert_statement.excluded},
        )
        result = conn.execute(conflict_update)
        return result.rowcount

    def ingest(self, datas: list, table_name: str):
        try:
            df = pd.DataFrame(datas)
            if df.empty:
                print("DataFrame is empty after filtering. Nothing to ingest.")
                return 0

            row_count = df.to_sql(
                table_name,
                self.__conn,
                if_exists="append",
                index=False,
                schema="datamart",
                method=self._insert_on_conflict_upsert
            )
            self.__conn.commit()
            return row_count

        except Exception as e:
            logger.error(f"Error ingesting data: {e}")
            return 0

    def close(self):
        self.__conn.connection.close()

    def read_query(self, query):
        df = pd.read_sql_query(text(query), self.__conn)
        return df