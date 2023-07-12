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


class OrderObj:
    def __init__(self, user_id: int, restaurant_id: int, timestamp_id: int, order_key: str, order_status: str) -> None:
        self.user_id = user_id
        self.restaurant_id = restaurant_id
        self.timestamp_id = timestamp_id
        self.order_key = order_key
        self.order_status = order_status

class RestaurantObj:
    def __init__(self, id: int, restaurant_id: str) -> None:
        self.id = id
        self.restaurant_id = restaurant_id

class UserObj:
    def __init__(self, id: int, user_id: str) -> None:
        self.id = id
        self.user_id = user_id

class TimestampObj:
    def __init__(self, id: int, ts: datetime) -> None:
        self.id = id
        self.ts = ts

class StgOrdersystemOrdersRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, last_loaded_record_id: int) -> List[MongoObj]:
        with self._db.client().cursor(row_factory=class_row(MongoObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(last_loaded_record_id)s                   
                    ORDER BY id ASC
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs

class DdsRestauntsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id
                    FROM dds.dm_restaurants
                """
            )
            objs = cur.fetchall()
        return objs
    
class DdsUsersRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, user_id
                    FROM dds.dm_users
                """
            )
            objs = cur.fetchall()
        return objs
    
class DdsTimestampsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_tss(self) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts
                    FROM dds.dm_timestamps
                """
            )
            objs = cur.fetchall()
        return objs

class DdsOrdersDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None: 
        with conn.cursor() as cur:
            cur.execute(                
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                """,
                {
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,                    
                    "order_key": order.order_key,
                    "order_status": order.order_status,                   
                },
            )

class DdsOrderLoader:
    WF_KEY = "order_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = StgOrdersystemOrdersRepository(pg_dest)
        self.dds_rest = DdsRestauntsRepository(pg_dest)
        self.dds_user = DdsUsersRepository(pg_dest)
        self.dds_ts = DdsTimestampsRepository(pg_dest)
        self.dds = DdsOrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_orders(last_loaded)
            self.log.info(f"Found {len(load_queue)} prodaucts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            # Получаем список всех ресторанов и их id в базе и преобразуем в словарь.
            load_queue_rest = self.dds_rest.list_restaurants()
            self.log.info(f"Found {len(load_queue_rest)} restaurrants to load.")
            if not load_queue_rest:
                self.log.info("Quitting.")
                return
            dict_rest = {}
            for rest in load_queue_rest:
                dict_rest[rest.restaurant_id] = rest.id

            # Получаем список всех пользователей и их id в базе и преобразуем в словарь.
            load_queue_user = self.dds_user.list_users()
            self.log.info(f"Found {len(load_queue_user)} users to load.")
            if not load_queue_user:
                self.log.info("Quitting.")
                return
            dict_user = {}
            for user in load_queue_user:
                dict_user[user.user_id] = user.id

            # Получаем список всех показктелей времени и их id в базе и преобразуем в словарь.
            load_queue_ts = self.dds_ts.list_tss()
            self.log.info(f"Found {len(load_queue_ts)} timestamps to load.")
            if not load_queue_ts:
                self.log.info("Quitting.")
                return
            dict_ts = {}
            for ts in load_queue_ts:
                dict_ts[ts.ts] = ts.id

            # Сохраняем объекты в базу.
            for json_order in load_queue:
                json_order = str2json(json_order.object_value)
                new_json_order = OrderObj(dict_user[json_order['user']['id']], 
                                        dict_rest[json_order['restaurant']['id']], 
                                        dict_ts[datetime.strptime(json_order['update_ts'], '%Y-%m-%d %H:%M:%S')], 
                                        json_order['_id'],
                                        json_order['final_status'])
                self.dds.insert_order(conn, new_json_order)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
