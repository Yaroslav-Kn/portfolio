from logging import Logger
from typing import List

from orders_restaurants.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class MongoObj(BaseModel):
    id: int
    object_id: str 
    object_value: str
    update_ts: datetime


class TimestampObj:
    def __init__(self, ts: datetime, year: int , month: int, day: int, time: time, date: date) -> None:
        self.ts = ts
        self.year = year
        self.month = month
        self.day = day
        self.time = time
        self.date = date


class StgOrdersystemTimestampsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, last_loaded_record_id: int) -> List[MongoObj]:
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


class DdsTimestampDestRepository:

    def insert_timestamp(self, conn: Connection, timestamp: TimestampObj) -> None: 
        with conn.cursor() as cur:
            cur.execute(                
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date,
                },
            )


class DdsTimestampLoader:
    WF_KEY = "timestamp_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = StgOrdersystemTimestampsRepository(pg_dest)
        self.dds = DdsTimestampDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_timestamps(last_loaded)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу.
            for json_timestamp in load_queue:
                json_timestamp = str2json(json_timestamp.object_value)
                ts = datetime.strptime(json_timestamp['update_ts'], '%Y-%m-%d %H:%M:%S')
                new_rest = TimestampObj(ts, 
                                         ts.year, 
                                         ts.month,
                                         ts.day,
                                         ts.time(),
                                         ts.date(),
                                         )  
                self.dds.insert_timestamp(conn, new_rest)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
