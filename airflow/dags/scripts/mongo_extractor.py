from datetime import datetime
from airflow.providers.mongo.hooks.mongo import MongoHook

class MongoExtractor:
    def __init__(self, mongo_conn_id: str = "mongo_default"):
        self.mongo_conn_id = mongo_conn_id

    @staticmethod
    def _build_query(field: str, watermark: int) -> dict:
        # Если водянного знака нет, то получаем все записи, мы еще ничего не обрабатывали.
        if watermark <= 0:
            return {}

        # Если водянной знак есть, то запрашиваем только новые записи, нам нужно обрабатывать только их.
        return {
            field: {
                "$gt": datetime.fromtimestamp(watermark),
            }
        }

    def _get_mongo_collection(self, collection_name: str):
        mongo_hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        client = mongo_hook.get_conn()
        database = client.get_default_database()
        return client, database[collection_name]

    def read_collection_chunk(self, collection_name: str, watermark_field: str, watermark: int, chunk_idx: int = 0,
                              chunk_size: int = 10) -> list[dict]:
        query = self._build_query(watermark_field, watermark)
        client, collection = self._get_mongo_collection(collection_name)

        try:
            cursor = (collection.find(query)
                      .sort(watermark_field, 1)
                      .skip(chunk_idx * chunk_size)
                      .limit(chunk_size))

            # Выдадим сразу все записи в списке, так как их все равно нужно мапить.
            # Проще загрузить все сразу, а не реализовывать логику работы с курсором с накладными расходами.
            return list(cursor)
        finally:
            client.close()

    def fetch_max_value(self, collection_name: str, watermark_field: str, watermark: int):
        query = self._build_query(watermark_field, watermark)
        client, collection = self._get_mongo_collection(collection_name)

        try:
            # Найдем документ с максимальным значением водянного знака.
            documents = list(collection.find(query, {watermark_field: 1, "_id": 0})
                             .sort(watermark_field, -1)
                             .limit(1))

            # Если не удалось получить документы, то вернем None.
            if documents is not None and len(documents) > 0:
                return documents[0].get(watermark_field)
            else:
                return None
        finally:
            client.close()

    def count_new_entries(self, collection_name: str, watermark_field: str, watermark: int):
        query = self._build_query(watermark_field, watermark)
        client, collection = self._get_mongo_collection(collection_name)

        try:
            return collection.count_documents(query)
        finally:
            client.close()
