from logging import Logger
from typing import List

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class SettlementReportDestRepository:

    def insert_settlement_report(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    WITH order_sums AS (
                        SELECT
                            r.id                    AS restaurant_id,
                            r.restaurant_name       AS restaurant_name,
                            tss.date                AS settlement_date,
                            SUM(fct.total_sum)      AS orders_total_sum,
                            SUM(fct.bonus_payment)  AS orders_bonus_payment_sum,
                            SUM(fct.bonus_grant)    AS orders_bonus_granted_sum
                        FROM dds.fct_product_sales as fct
                            INNER JOIN dds.dm_orders AS orders
                                ON fct.order_id = orders.id
                            INNER JOIN dds.dm_timestamps as tss
                                ON tss.id = orders.timestamp_id
                            INNER JOIN dds.dm_restaurants AS r
                                on r.id = orders.restaurant_id
                        WHERE orders.order_status = 'CLOSED'
                        GROUP BY
                            r.id,
                            r.restaurant_name,
                            tss.date
                    ),
                    order_count AS (
                    SELECT COUNT(orders.id),
                            orders.restaurant_id,
                            dt."date" 
                        FROM dds.dm_orders AS orders
                        INNER JOIN dds.dm_timestamps dt ON dt.id = orders.timestamp_id 
                        WHERE order_status = 'CLOSED'
                        GROUP BY orders.restaurant_id, dt."date" )
                    INSERT INTO cdm.dm_settlement_report(
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum
                    )
                    SELECT
                        s.restaurant_id,
                        s.restaurant_name,
                        s.settlement_date,
                        os.count AS orders_count,
                        s.orders_total_sum,
                        s.orders_bonus_payment_sum,
                        s.orders_bonus_granted_sum,
                        s.orders_total_sum * 0.25 AS order_processing_fee,
                        s.orders_total_sum - s.orders_total_sum * 0.25 - s.orders_bonus_payment_sum AS restaurant_reward_sum
                    FROM order_sums AS s
                    INNER JOIN order_count os ON os.restaurant_id = s.restaurant_id AND os."date" = s.settlement_date
                    WHERE s.settlement_date BETWEEN (NOW() - INTERVAL '1 month') AND NOW()
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,                
            )


class SettlementReportLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.cdm = SettlementReportDestRepository()
        self.log = log        

    def load_settlementreport(self):
        with self.pg_dest.connection() as conn:          
            self.cdm.insert_settlement_report(conn)
            self.log.info(f"Load finished on")
