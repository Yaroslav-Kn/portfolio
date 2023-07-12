from logging import Logger
from typing import List
from typing import Any
import requests
import json

from deliveries.stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from pydantic import BaseModel


class ObjOriginRepository:
    #получаем список на запрос по Api
    def list_obj(self, url: str, method_url: str, headers: dict, dict_filter: dict) -> List[str]:
        # преобразуем строку метода с учётом фильтров
        method_url = f'{method_url}?sort_field={dict_filter["sort_field"]}&sort_direction={dict_filter["sort_direction"]}&limit={dict_filter["limit"]}&offset={dict_filter["offset"]}'
        r = requests.get(url + method_url, headers=headers)
        objs = json.loads(r.content)
        return objs


class ApiObjDestRepository:
    #зпаисываем в базу
    def insert_obj(self, conn: Connection, table_name: str, val: Any) -> None:   
        str_val = json2str(val)   
        print(table_name)  
        with conn.cursor() as cur:
            first_part = f'INSERT INTO stg.{table_name}' # создаём первую часть строки для вставки информации в базу 
            cur.execute(
                first_part + """ (object_value) 
                    VALUES (%(str_val)s);                  
                """,
                {
                    'str_val': str_val               
                }
            )


class ApiObjLoader:
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_dest: PgConnect, table_name: str, url: str, method_url: str, headers: dict, dict_filter: dict, log: Logger) -> None:
        self.WF_KEY = f"{table_name}_origin_to_stg_workflow"
        self.pg_dest = pg_dest
        self.origin = ObjOriginRepository()
        self.stg = ApiObjDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.table_name = table_name
        self.log = log
        self.url = url
        self.method_url = method_url
        self.headers = headers
        self.dict_filter = dict_filter

    def load_objs(self):
        # Запускаем цикл, чтобы вычитать все значения
        flag_queue = 1
        while flag_queue > 0:
            with self.pg_dest.connection() as conn:

                # Прочитываем состояние загрузки
                # Если настройки еще нет, заводим ее.
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
                if not wf_setting:
                    wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

                # Вычитываем очередную пачку объектов.
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
                self.dict_filter['offset'] = last_loaded  # задаём значение с которого заюирать информацию (поскольку у нас инкрементная загрузка)
                load_queue = self.origin.list_obj(self.url, self.method_url, self.headers, self.dict_filter)
                self.log.info(f"Found {len(load_queue)} objects to load.")
                if not load_queue:
                    self.log.info("Quitting.")
                    return
                flag_queue = len(load_queue)

                # Сохраняем объекты в базу dwh.
                for obj in load_queue:
                    self.stg.insert_obj(conn, self.table_name, obj)

                # Сохраняем прогресс.
                # Либо откатываем все изменения целиком, в случае ошибки.
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]  + len(load_queue)
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
