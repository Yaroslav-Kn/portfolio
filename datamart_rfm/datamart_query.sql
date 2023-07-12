INSERT INTO
	analysis.dm_rfm_segments (user_id,
	recency,
	frequency,
	monetary_value)
SELECT
	r.user_id,
	r.recency,
	f.frequency,
	mv.monetary_value
FROM
	analysis.tmp_rfm_recency r
INNER JOIN analysis.tmp_rfm_frequency f ON
	f.user_id = r.user_id
INNER JOIN analysis.tmp_rfm_monetary_value mv ON
	mv.user_id = r.user_id
ORDER BY
	r.user_id
