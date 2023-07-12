-- Заполняем справочник стоимости доставки в страны
INSERT
	INTO
	shipping_country_rates (shipping_country,
	shipping_country_base_rate)
(
	SELECT
		DISTINCT 
		shipping_country,
		shipping_country_base_rate
	FROM
		shipping);
	
-- Заполняем справочник тарифов доставки вендора по договору
INSERT
	INTO
	shipping_agreement(agreement_id,
	agreement_number,
	agreement_rate,
	agreement_commission)
(
	SELECT
		DISTINCT
		vendor_array[1]::int AS agreement_id,
		vendor_array[2] AS agreement_number,
		vendor_array[3]::NUMERIC(14,
		2) AS agreement_rate,
		vendor_array[4]::NUMERIC(14,
		2) AS agreement_commission
	FROM
		(
		SELECT
			regexp_split_to_array(vendor_agreement_description, ':+') AS vendor_array
		FROM
			shipping) AS vendor_table
	ORDER BY
		agreement_id);
	
-- Заполняем справочник о типах доставки
INSERT
	INTO
	shipping_transfer(transfer_type,
	transfer_model,
	shipping_transfer_rate)
	(
	SELECT
		DISTINCT
	    tranfser_array[1] AS transfer_type,
		tranfser_array[2] AS transfer_model,
		shipping_transfer_rate
	FROM
		(
		SELECT
			regexp_split_to_array(shipping_transfer_description, ':+') AS tranfser_array,
			shipping_transfer_rate
		FROM
			shipping) AS transfer_table
	ORDER BY
		transfer_type,
		transfer_model);
	
-- Заполняем справочник комиссий по странам, с уникальными доставками
INSERT
	INTO
	shipping_info(shipping_id,
	vendor_id,
	payment_amount,
	shipping_plan_datetime,
	shipping_transfer_id,
	shipping_agreement_id,
	shipping_country_rates_id)
	(
	SELECT
		DISTINCT
		s.shippingid,
		s.vendorid,
		s.payment_amount,
		s.shipping_plan_datetime,
		st.id,
		sa.agreement_id,
		scr.id
	FROM
		shipping s
	JOIN shipping_transfer st ON
		s.shipping_transfer_description = st.transfer_type || ':' || st.transfer_model
	JOIN shipping_agreement sa ON
		CAST(split_part(s.vendor_agreement_description, ':', 1) AS INT) = sa.agreement_id
	JOIN shipping_country_rates scr ON
		s.shipping_country = scr.shipping_country
	ORDER BY
		shippingid);

-- Заполнение таблицы статусов о доставке
INSERT
	INTO
	shipping_status(
    shipping_id,
	status,
	state,	
	shiping_start_fact_datetime,
	shipping_end_fact_datetime)
(WITH se AS
(
	-- таблица для определения фактического времени доставки
	SELECT
		shippingid,
		MIN(
	CASE
		WHEN state = 'booked' THEN state_datetime ELSE NULL
	END
		) AS shipping_start_fact_datetime,
		MAX(
	CASE
		WHEN state = 'recieved' THEN state_datetime ELSE NULL
	END
		) AS shipping_end_fact_datetime
	FROM
		shipping
	GROUP BY
		shippingid)
	--объединение данных
	SELECT DISTINCT
		FIRST_VALUE(shippingid) OVER(PARTITION BY shippingid ORDER BY state_datetime DESC),
		FIRST_VALUE(status) OVER(PARTITION BY shippingid ORDER BY state_datetime DESC),
		FIRST_VALUE(state) OVER(PARTITION BY shippingid ORDER BY state_datetime DESC),
		se.shipping_start_fact_datetime,
		se.shipping_end_fact_datetime
	FROM
		shipping s
	JOIN se
			USING(shippingid)
	);
