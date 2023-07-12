from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, id: str, update_ts: datetime, val: Any, table_name: str):
        str_val = json2str(val)
        with conn.cursor() as cur:
            first_part = f'INSERT INTO stg.{table_name}' # создаём первую часть строки для вставки информации в базу 
            cur.execute(
                    first_part +  """(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )
