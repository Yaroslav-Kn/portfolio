INSERT INTO analysis.tmp_rfm_monetary_value (user_id, monetary_value)
SELECT
	u.id,
	NTILE(5) OVER(ORDER BY lo.sum_payment NULLS FIRST) AS monetary_value
FROM
	analysis.users u
LEFT OUTER JOIN (
	SELECT
		o.user_id,
		SUM(o.payment) AS sum_payment
	FROM
		analysis.orders o
	LEFT OUTER JOIN analysis.orderstatuses os ON
		o.status = os.id
	WHERE
		os."key" = 'Closed'
		AND EXTRACT(YEAR FROM o.order_ts) >= 2022
	GROUP BY
		o.user_id
) AS lo ON
	u.id = lo.user_id
ORDER BY u.id
	

