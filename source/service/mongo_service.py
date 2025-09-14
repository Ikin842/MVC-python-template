import pymongo
from loguru import logger
from config.base import settings

class MongoService:

    def __init__(self):
        self.__client = pymongo.MongoClient(settings.MONGODB_CLIENT)
        self.__database = self.__client[settings.MONGODB_DATABASE]

    def ingest_data(self, data_raw: dict, match_field: str = "_id"):
        try:
            collection = self.__database[settings.MONGODB_COLLECTION]
            filter_query = {match_field: data_raw.get(match_field)}
            update = {"$set": data_raw}
            result = collection.update_one(filter_query, update, upsert=True)
            return result.acknowledged
        except Exception as e:
            logger.error(f"Error updating MongoDB: {e}")
            return False