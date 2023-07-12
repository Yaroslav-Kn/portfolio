from logging import Logger
from typing import List

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class CourierLedgerDestRepository:

    def insert_courier_ledger(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger (courier_id,
                                                        courier_name,
                                                        settlement_year,
                                                        settlement_month,
                                                        orders_count,
                                                        orders_total_sum,
                                                        rate_avg,
                                                        order_processing_fee,
                                                        courier_order_sum,
                                                        courier_tips_sum,
                                                        courier_reward_sum)	
                    SELECT dc.courier_id,
                            dc.courier_name,
                            dtod."year" AS settlement_year,
                            dtod."month" AS settlement_month,
                            COUNT(*) AS orders_count,
                            SUM(fod.sum) AS orders_total_sum,
                            rate_avg,
                            SUM(fod.sum) * 0.25 AS order_processing_fee,
                            SUM(CASE 
                                    WHEN rate_avg < 4 THEN GREATEST(0.05 * fod.sum, 100)
                                    WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(0.07 * fod."sum", 150)
                                    WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(0.08 * fod."sum", 175)
                                    ELSE GREATEST(0.07 * fod."sum", 200)
                                END) AS courier_sum,
                            SUM(fod.tip_sum) AS courier_tips_sum, 
                            (SUM(CASE 
                                    WHEN rate_avg < 4 THEN GREATEST(0.05 * fod.sum, 100)
                                    WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(0.07 * fod."sum", 150)
                                    WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(0.08 * fod."sum", 175)
                                    ELSE GREATEST(0.07 * fod."sum", 200)
                                END) + SUM(fod.tip_sum)) * 0.95 AS courier_reward_sum
                    --- Вложенным запросым посчитаем средний рейтинг, чтобы выше могли посчитать правильную плату за доставку,
                    --- т.к. параметры минимальной платы необходимо учитывать исходя из одного заказа
                    FROM (SELECT fod.id_couriers,			
                                AVG(CAST(drd.rate AS FLOAT)) AS rate_avg	
                        FROM dds.fct_order_deliveries AS fod
                        JOIN dds.dm_couriers AS dc ON dc.id = fod.id_couriers 
                        JOIN dds.dm_timestamps_order_deliv AS dtod ON dtod.id = fod.id_timestamps_order_deliv 
                        JOIN dds.dm_rates_deliveries AS drd ON drd.id = fod.id_rate_deliveries 
                        GROUP BY fod.id_couriers, dtod."year", dtod."month") AS avg_r
                    JOIN dds.fct_order_deliveries AS fod ON avg_r.id_couriers = fod.id_couriers 
                    JOIN dds.dm_couriers AS dc ON dc.id = fod.id_couriers 
                    JOIN dds.dm_timestamps_order_deliv AS dtod ON dtod.id = fod.id_timestamps_order_deliv 
                    JOIN dds.dm_rates_deliveries AS drd ON drd.id = fod.id_rate_deliveries 
                    WHERE dtod."month" = EXTRACT('month' FROM NOW()) - 1 --- Выбираем значения за прошлый месяц
                    GROUP BY dc.courier_id, dc.courier_name, settlement_year, settlement_month, rate_avg
                    ORDER BY dc.courier_id
                    ON CONFLICT (courier_id, courier_name, settlement_year, settlement_month) DO UPDATE 
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,                
            )


class CourierLedgerReportLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.cdm = CourierLedgerDestRepository()
        self.log = log        

    def load_courier_ledger(self):
        # открываем транзакцию.
        with self.pg_dest.connection() as conn:          
            self.cdm.insert_courier_ledger(conn)
            self.log.info(f"Load finished on")
