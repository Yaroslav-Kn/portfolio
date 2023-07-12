from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class ReaderMongo:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_orders(self, load_threshold: datetime, collection_name: str, limit: int) -> List[Dict]:
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {'update_ts': {'$gt': load_threshold}}

        # Формируем сортировку по update_ts. 
        sort = [('update_ts', 1)]

        # Вычитываем документы из MongoDB
        docs = list(self.dbs.get_collection(collection_name).find(filter=filter, sort=sort, limit=limit))
        return docs
