INSERT INTO analysis.tmp_rfm_frequency (user_id, frequency)
SELECT
	u.id,
	NTILE (5) OVER(ORDER BY lo.count_order NULLS FIRST) AS frequency
FROM
	analysis.users u
LEFT OUTER JOIN (
	SELECT
		o.user_id,
		COUNT(*) AS count_order
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
ORDER BY
	u.id
