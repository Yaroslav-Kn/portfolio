from logging import Logger
from typing import List

from deliveries.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import datetime


class ApiObj(BaseModel):
    id: int
    object_value: str 


class CourierObj:

    def __init__(self, courier_id: str, courier_name: str) -> None:
        self.courier_id = courier_id
        self.courier_name = courier_name


class DdsApiCouriersRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, last_loaded_record_id: int) -> List[ApiObj]:
        with self._db.client().cursor(row_factory=class_row(ApiObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.api_couriers
                    WHERE id > %(last_loaded_record_id)s                   
                    ORDER BY id ASC
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs


class DdsCourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None: 
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                },
            )


class DdsCourierLoader:
    WF_KEY = "courier_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DdsApiCouriersRepository(pg_dest)
        self.dds = DdsCourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_users(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_couriers(last_loaded)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу.
            for json_user in load_queue:
                json_user = str2json(json_user.object_value)
                new_user = CourierObj(json_user['_id'], json_user['name'])  
                self.dds.insert_courier(conn, new_user)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
