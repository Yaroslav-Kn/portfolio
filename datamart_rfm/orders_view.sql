CREATE OR REPLACE VIEW analysis.orders AS 
SELECT 
    DISTINCT o.order_id,
    o.order_ts,
    o.user_id,
    o.bonus_payment,
    o.payment,
    o."cost",
    o.bonus_grant,
    LAST_VALUE(p.status_id) OVER (PARTITION BY p.order_id ORDER BY dttm ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS status
FROM 
    production.orders AS o
JOIN
    production.orderstatuslog AS p
        USING(order_id) 