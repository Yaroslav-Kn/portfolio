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


class ProductObj:
    def __init__(self,  id: int, product_id: str, restaurant_id: int, product_name: str, product_price: float, active_from: datetime, active_to: datetime) -> None:
        self.id = id
        self.product_id = product_id
        self.restaurant_id = restaurant_id
        self.product_name = product_name
        self.product_price = product_price
        self.active_from = active_from
        self.active_to = active_to

class RestaurantObj:
    def __init__(self, id: int, restaurant_id: str) -> None:
        self.id = id
        self.restaurant_id = restaurant_id

class StgOrdersystemProductsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, last_loaded_record_id: int) -> List[MongoObj]:
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

class DdsProductsDestRepository:

    def insert_product(self, conn: Connection, product: ProductObj) -> None: 
        #Получаем список данных товаров в данном ресторане
        with conn.cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    SELECT * 
                    FROM dds.dm_products 
                    WHERE product_id=%(product_id)s AND restaurant_id=%(restaurant_id)s
                    ORDER BY active_from DESC
                """,
                {
                    "product_id": product.product_id,
                    "restaurant_id": product.restaurant_id                 
                },
            )
            lis_product = cur.fetchall()

            #если список пуст, то проудкт новый и мы его добавляем 
            if len(lis_product)==0:
                with conn.cursor() as cur:
                    cur.execute(                
                        """
                            INSERT INTO dds.dm_products(product_id, restaurant_id, product_name, product_price, active_from, active_to)
                            VALUES (%(product_id)s, %(restaurant_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                        """,
                        {
                            "product_id": product.product_id,
                            "restaurant_id": product.restaurant_id,
                            "product_name": product.product_name,                    
                            "product_price": product.product_price,
                            "active_from": product.active_from,
                            "active_to": product.active_to,
                            
                        },
                    )

            # Если список не пуст и цена товара отличается, то перебираем и записываем с новой ценой
            for i, product_in_base in enumerate(lis_product):
                if product.product_price != product_in_base.product_price:
                    # Перебираем до того момента, пока дата новой цены продукта не окажется больше даты из базы
                    # При выгрузке списка продуктов из базы они сортируются по дате в порядке убывания
                    if product.active_from > product_in_base.active_from:
                        # Вносим изменения в базу
                        # Меняем дату до которой активна текущая цена
                        with conn.cursor() as cur:
                            cur.execute(                
                                """
                                    UPDATE dds.dm_products 
                                    SET active_to = %(active_to)s
                                    WHERE id = %(product_id)s
                                """,
                                {
                                    "product_id": product_in_base.product_id,
                                    "active_to": product.active_from,                                
                                },
                            )
                        # Проверяем не первый ли это элемент в списке. Если нет, то ставим значение activ_to
                        # из предыдущего элемента
                        if i != 0:
                            product.active_to = lis_product[i-1].active_from
                        # Добавляем в базу запись про продукт с новой ценой
                        with conn.cursor() as cur:
                            cur.execute(                
                                """
                                    INSERT INTO dds.dm_products(product_id, restaurant_id, product_name, product_price, active_from, active_to)
                                    VALUES (%(product_id)s, %(restaurant_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                                """,
                                {
                                    "product_id": product.product_id,
                                    "restaurant_id": product.restaurant_id,
                                    "product_name": product.product_name,                    
                                    "product_price": product.product_price,
                                    "active_from": product.active_from,
                                    "active_to": product.active_to,
                                    
                                },
                            )           


class DdsProductLoader:
    WF_KEY = "product_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = StgOrdersystemProductsRepository(pg_dest)
        self.dds_rest = DdsRestauntsRepository(pg_dest)
        self.dds = DdsProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_products(last_loaded)
            self.log.info(f"Found {len(load_queue)} prodaucts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            # Получаем список всех ресторанов и их id в базе.
            load_queue_rest = self.dds_rest.list_restaurants()
            self.log.info(f"Found {len(load_queue)} restaurrants to load.")
            if not load_queue_rest:
                self.log.info("Quitting.")
                return
            dict_rest = {}
            for rest in load_queue_rest:
                dict_rest[rest.restaurant_id] = rest.id

            # Сохраняем объекты в базу.
            for json_product in load_queue:
                json_product = str2json(json_product.object_value)
                for product in json_product['order_items']:
                    new_product = ProductObj(-1,
                                            product['id'], 
                                            dict_rest[json_product['restaurant']['id']], 
                                            product['name'],
                                            product['price'],
                                            json_product['update_ts'],
                                            datetime(2099, 12, 31))  
                    self.dds.insert_product(conn, new_product)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
