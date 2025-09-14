from loguru import logger
from config import es_connection
from elasticsearch import helpers
from elasticsearch.helpers import streaming_bulk

class ElasticService:
    def __init__(self):
        self.__index = "sample_index"
        self.__es_conn = es_connection
        self.__results = []

    def read_query(self, query, index):
        scan_results = helpers.scan(
            self.__es_conn,
            query=query,
            index=index,
            size=1000,
            scroll="60m",
            request_timeout=3600
        )

        for hit in scan_results:
            source = hit['_source']
            self.__results.append(source)

        return self.__results

    @staticmethod
    def _insert_actions(id_data_pairs, index_name):
        for doc_id, data in id_data_pairs:
            yield {
                "_index": index_name,
                "_id": doc_id,
                "_source": data
            }

    @staticmethod
    def _update_actions(id_data_pairs, index_name):
        for doc_id, data in id_data_pairs:
            yield {
                "_op_type": "update",
                "_index": index_name,
                "_id": doc_id,
                "doc": data,
            }

    def _action_generator(self, id_data_pairs, operation_type):
        if operation_type == 'insert':
            actions_generator = self._insert_actions(id_data_pairs, self.__index)
            return actions_generator
        elif operation_type == 'update':
            actions_generator = self._update_actions(id_data_pairs, self.__index)
            return actions_generator
        else:
            logger.error("Invalid operation type! Choose either 'insert' or 'update'.")
            return None

    def ingest_data(self, id_data_pairs: list, operation_type: str = 'insert'):
        try:
            total = 0
            for success, info in streaming_bulk(
                    self.__es_conn,self._action_generator(id_data_pairs, operation_type), chunk_size=100
            ):
                if success:
                    total += 1
                else:
                    logger.error(f'Failed to update document: {info}')
            return total

        except Exception as e:
            logger.error(f"Error updating Elasticsearch: {e}")
            return 0