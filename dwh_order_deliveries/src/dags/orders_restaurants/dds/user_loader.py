from logging import Logger
from typing import List

from orders_restaurants.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import datetime


class MongoObj(BaseModel):
    id: int
    object_id: str 
    object_value: str
    update_ts: datetime.datetime


class UserObj:

    def __init__(self, _id: str, login: str, name: str) -> None:
        self._id = _id
        self.login = login
        self.name = name


class StgOrdersystemUsersRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, last_loaded_record_id: int) -> List[MongoObj]:
        with self._db.client().cursor(row_factory=class_row(MongoObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_users
                    WHERE id > %(last_loaded_record_id)s                   
                    ORDER BY id ASC 
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs


class DdsUserDestRepository:

    def insert_user(self, conn: Connection, user: UserObj) -> None: 
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                """,
                {
                    "user_id": user._id,
                    "user_name": user.name,
                    "user_login": user.login,
                },
            )


class DdsUserLoader:
    WF_KEY = "user_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = StgOrdersystemUsersRepository(pg_dest)
        self.dds = DdsUserDestRepository()
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
            load_queue = self.stg.list_users(last_loaded)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу.
            for json_user in load_queue:
                json_user = str2json(json_user.object_value)
                new_user = UserObj(json_user['_id'], json_user['login'], json_user['name'])  
                self.dds.insert_user(conn, new_user)

            # Сохраняем прогресс.
            # Пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
