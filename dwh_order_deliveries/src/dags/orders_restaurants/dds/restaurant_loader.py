from logging import Logger
from typing import List

from orders_restaurants.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class MongoObj(BaseModel):
    id: int
    object_id: str 
    object_value: str
    update_ts: datetime


class RestaurantObj:
    def __init__(self, restaurant_id: str, restaurant_name: str, active_from: datetime, active_to: datetime) -> None:
        self.restaurant_id = restaurant_id
        self.restaurant_name = restaurant_name
        self.active_from = active_from
        self.active_to = active_to


class StgOrdersystemRestaurantsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, last_loaded_record_id: int) -> List[MongoObj]:
        with self._db.client().cursor(row_factory=class_row(MongoObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s                   
                    ORDER BY id ASC
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs


class DdsRestaurantDestRepository:

    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj) -> None: 
        with conn.cursor() as cur:
            cur.execute(                
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to,
                },
            )


class DdsRestaurantLoader:
    WF_KEY = "restaurant_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = StgOrdersystemRestaurantsRepository(pg_dest)
        self.dds = DdsRestaurantDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_restaurants(last_loaded)
            self.log.info(f"Found {len(load_queue)} restaurrants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу.
            for json_restaurant in load_queue:
                json_restaurant = str2json(json_restaurant.object_value)
                new_rest = RestaurantObj(json_restaurant['_id'], 
                                         json_restaurant['name'], 
                                         json_restaurant['update_ts'],
                                         datetime(2099, 12, 31))  
                self.dds.insert_restaurant(conn, new_rest)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
