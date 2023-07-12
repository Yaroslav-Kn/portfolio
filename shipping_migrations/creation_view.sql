CREATE OR REPLACE VIEW shipping_datamart AS
SELECT 
	ss.shipping_id,
	si.vendor_id,
	st.transfer_type,
	date_part('day', (ss.shipping_end_fact_datetime - ss.shiping_start_fact_datetime)) AS full_day_at_shipping,
	CASE
		WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1
		ELSE 0
	END AS is_delay,
	CASE
		WHEN ss.status = 'finished' THEN 1
		ELSE 0
	END AS is_shipping_finish,
	CASE
		WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
			THEN date_part('day', ss.shipping_end_fact_datetime - si.shipping_plan_datetime)
		ELSE 0
	END AS delay_day_at_shipping,
	si.payment_amount,
	si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_id + sa.agreement_commission) AS vat,
	si.payment_amount * sa.agreement_commission AS profit
FROM
	shipping_status ss
JOIN shipping_info si
		USING(shipping_id)
JOIN shipping_transfer st ON
	st.id = si.shipping_transfer_id
JOIN shipping_country_rates scr ON
	scr.id = si.shipping_country_rates_id 
JOIN shipping_agreement sa ON
	sa.agreement_id = si.shipping_agreement_id 

