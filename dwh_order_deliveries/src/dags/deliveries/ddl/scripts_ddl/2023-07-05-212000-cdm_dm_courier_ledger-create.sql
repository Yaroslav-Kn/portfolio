CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
	id SERIAL NOT NULL PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	rate_avg numeric(14,2) NOT NULL,
	order_processing_fee numeric(14,2) NOT NULL,
	courier_order_sum numeric(14,2) NOT NULL,
	courier_tips_sum numeric(14,2) NOT NULL,
	courier_reward_sum numeric(14,2) NOT NULL,
	CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((settlement_year >= 2022) AND (settlement_year < 2500))),
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_settlement_orders_count_check CHECK (orders_total_sum >= 0),
	CONSTRAINT dm_courier_ledger_settlement_orders_total_sum_check CHECK (orders_total_sum > 0),
	CONSTRAINT dm_courier_ledger_settlement_rate_avg_check CHECK (((rate_avg >= 1) AND (rate_avg <= 5))),
	CONSTRAINT dm_courier_ledger_settlement_order_processing_fee_check CHECK (order_processing_fee > 0),
	CONSTRAINT dm_courier_ledger_settlement_courier_order_sum_check CHECK (courier_order_sum > 0),
	CONSTRAINT dm_courier_ledger_settlement_courier_tips_sum_check CHECK (courier_tips_sum >= 0),
	CONSTRAINT dm_courier_ledger_settlement_courier_reward_sum_check CHECK (courier_reward_sum > 0),
	CONSTRAINT dm_settlement_report_courier_date_unique_check UNIQUE (courier_id, courier_name, settlement_year, settlement_month)
);