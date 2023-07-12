from logging import Logger
from typing import List

from orders_restaurants.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class BonusSystemEvents(BaseModel):
    id: int
    event_ts: datetime 
    event_type: str
    event_value: str


class FctProductSalesObj:
    def __init__(self,  product_id: int, order_id: int, count: int, price: float, total_sum: float, bonus_payment: float, bonus_grant: float) -> None:
        self.product_id = product_id
        self.order_id = order_id
        self.count = count
        self.price = price
        self.total_sum = total_sum
        self.bonus_payment = bonus_payment
        self.bonus_grant = bonus_grant

class ProductObj:
    def __init__(self, id: int, product_id: str) -> None:
        self.id = id
        self.product_id = product_id

class OrderObj:
    def __init__(self, id: int, order_key: str) -> None:
        self.id = id
        self.order_key = order_key

class StgBonusEventsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_bonus_events(self, last_loaded_record_id: int) -> List[BonusSystemEvents]:
        with self._db.client().cursor(row_factory=class_row(BonusSystemEvents)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = 'bonus_transaction' AND id > %(last_loaded_record_id)s                   
                    ORDER BY id ASC
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs

class DdsProductsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id
                    FROM dds.dm_products
                """
            )
            objs = cur.fetchall()
        return objs
    
class DdsOrdersRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_key
                    FROM dds.dm_orders
                """
            )
            objs = cur.fetchall()
        return objs

class DdsFctProductSalesDestRepository:

    def insert_product(self, conn: Connection, product: ProductObj) -> None: 
        #Получаем список данных товаров в данном ресторане
        with conn.cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(                
                """
                    INSERT INTO dds.fct_product_sales(product_id, 
                                                    order_id, 
                                                    count, 
                                                    price, 
                                                    total_sum, 
                                                    bonus_payment,
                                                    bonus_grant)
                    VALUES (%(product_id)s, 
                            %(order_id)s,
                              %(count)s, 
                              %(price)s, 
                              %(total_sum)s, 
                              %(bonus_payment)s, 
                              %(bonus_grant)s)
                """,
                {
                    "product_id": product.product_id,
                    "order_id": product.order_id,
                    "count": product.count,                    
                    "price": product.price,
                    "total_sum": product.total_sum,
                    "bonus_payment": product.bonus_payment,
                    "bonus_grant": product.bonus_grant,
                    
                },                 

            )        

class DdsFctProduct_Sales_Loader:
    WF_KEY = "fct_product_sales_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = StgBonusEventsRepository(pg_dest)
        self.dds_prod = DdsProductsRepository(pg_dest)
        self.dds_ord = DdsOrdersRepository(pg_dest)
        self.dds = DdsFctProductSalesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_insert_fct_product_sales(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_bonus_events(last_loaded)
            self.log.info(f"Found {len(load_queue)} bonus events to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            # Получаем список всех продуктов и их id в базе.
            load_queue_prod = self.dds_prod.list_products()
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue_prod:
                self.log.info("Quitting.")
                return
            dict_prod = {}
            for prod in load_queue_prod:
                dict_prod[prod.product_id] = prod.id
                            
            # Получаем список всех заказов и их id в базе.
            load_queue_order = self.dds_ord.list_orders()
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue_order:
                self.log.info("Quitting.")
                return
            dict_order = {}
            for order in load_queue_order:
                dict_order[order.order_key] = order.id

            # Сохраняем объекты в базу.
            for json_fact in load_queue:
                json_fact = str2json(json_fact.event_value)
                for fact in json_fact['product_payments']:
                    new_fact = FctProductSalesObj(dict_prod[fact['product_id']],
                                                dict_order[json_fact['order_id']],
                                                fact['quantity'],
                                                fact['price'],  
                                                fact['product_cost'], 
                                                fact['bonus_payment'],
                                                fact['bonus_grant']  
                                                )  
                    self.dds.insert_product(conn, new_fact)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
