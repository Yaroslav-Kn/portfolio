from logging import Logger
from typing import List

from deliveries.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class CourierObj(BaseModel):
    id: int
    courier_id: str 


class RateObj(BaseModel):
    id: int
    rate: int 


class TimestampObj(BaseModel):
    id: int
    ts: datetime 


class ApiObj(BaseModel):
    id: int
    object_value: str 


class FctOrderDeliveriesObj:

    def __init__(self, id_couriers: int, id_timestamps_order_deliv: int, id_rate_deliveries: int, sum: float, tip_sum:float):
        self.id_couriers = id_couriers
        self.id_timestamps_order_deliv = id_timestamps_order_deliv
        self.id_rate_deliveries = id_rate_deliveries
        self.sum = sum
        self.tip_sum = tip_sum


class DdsApiRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_api_deliveries(self, last_loaded_record_id: int) -> List[ApiObj]:
        with self._db.client().cursor(row_factory=class_row(ApiObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.api_deliveries
                    WHERE id > %(last_loaded_record_id)s                   
                    ORDER BY id ASC 
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs
    

class DdsCouriersRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self) -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT id, courier_id
                    FROM dds.dm_couriers
                """
            )
            objs = cur.fetchall()
        return objs
    

class DdsRateRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_rates(self) -> List[RateObj]:
        with self._db.client().cursor(row_factory=class_row(RateObj)) as cur:
            cur.execute(
                """
                    SELECT id, rate
                    FROM dds.dm_rates_deliveries
                """
            )
            objs = cur.fetchall()
        return objs


class DdsTimestampDelivRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ts(self) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts
                    FROM dds.dm_timestamps_order_deliv
                """
            )
            objs = cur.fetchall()
        return objs
    

class DdsFctOrderDeliveriesRepository:

    def insert_fct_ord_deliv(self, conn: Connection, fct_ord_deliv: FctOrderDeliveriesObj) -> None: 
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_order_deliveries(id_couriers, id_timestamps_order_deliv, id_rate_deliveries, sum, tip_sum)
                    VALUES (%(id_couriers)s, %(id_timestamps_order_deliv)s, %(id_rate_deliveries)s, %(sum)s, %(tip_sum)s)
                """,
                {
                    "id_couriers": fct_ord_deliv.id_couriers,
                    "id_timestamps_order_deliv": fct_ord_deliv.id_timestamps_order_deliv,
                    "id_rate_deliveries": fct_ord_deliv.id_rate_deliveries,                    
                    "sum": fct_ord_deliv.sum,                
                    "tip_sum": fct_ord_deliv.tip_sum,
                },
            )


class DdsFctOrderDeliveriesLoader:
    WF_KEY = "fct_ord_deliv_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DdsApiRepository(pg_dest)
        self.cour = DdsCouriersRepository(pg_dest)
        self.rate = DdsRateRepository(pg_dest)
        self.ts = DdsTimestampDelivRepository(pg_dest)
        self.fct = DdsFctOrderDeliveriesRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fct_ord_deliv(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_api_deliveries(last_loaded)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            # вычитываем из базы сведения о курьерах
            list_couriers = self.cour.list_couriers()
            dict_couriers = {courier.courier_id: courier.id for courier in list_couriers}

            # вычитываем из базы сведения о датах
            list_rate = self.ts.list_ts()
            dict_ts= {ts.ts: ts.id for ts in list_rate}

            # вычитываем из базы сведения о рэйтингах
            list_rate = self.rate.list_rates()
            dict_rates = {rate.rate: rate.id for rate in list_rate}

            # Сохраняем объекты в базу.
            for json_user in load_queue:
                json_user = str2json(json_user.object_value)

                try:
                    key_ts = datetime.strptime(json_user['order_ts'], '%Y-%m-%d %H:%M:%S.%f')
                except:
                    key_ts = datetime.strptime(json_user['order_ts'], '%Y-%m-%d %H:%M:%S')

                new_obj = FctOrderDeliveriesObj(
                                                 dict_couriers[json_user['courier_id']],
                                                 dict_ts[key_ts],                                                 
                                                 dict_rates[json_user['rate']],
                                                 json_user['sum'],
                                                 json_user['tip_sum']
                                                )
                self.fct.insert_fct_ord_deliv(conn, new_obj)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
