INSERT INTO analysis.tmp_rfm_recency (user_id, recency)
SELECT
	u.id,
	NTILE (5) OVER (ORDER BY lo.last_order NULLS FIRST) recency
FROM
	analysis.users u
LEFT OUTER JOIN (
	SELECT
		o.user_id,
		MAX(o.order_ts) AS last_order
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
