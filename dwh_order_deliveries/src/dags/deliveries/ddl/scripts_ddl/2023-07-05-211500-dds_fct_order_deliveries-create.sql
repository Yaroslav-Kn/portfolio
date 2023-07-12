CREATE TABLE IF NOT EXISTS dds.fct_order_deliveries (
	id serial4 NOT NULL PRIMARY KEY,
	id_couriers int4 NOT NULL,
	id_timestamps_order_deliv int4 NOT NULL,
	id_rate_deliveries int4 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_sum_check CHECK ((sum > (0)::numeric)),
	CONSTRAINT dm_tip_sum_check CHECK ((tip_sum >= (0)::numeric)),
	CONSTRAINT fct_order_deliveries_couriers_id_ref FOREIGN KEY (id_couriers) REFERENCES dds.dm_couriers(id),
	CONSTRAINT fct_order_deliveries_timestamps_id_ref FOREIGN KEY (id_timestamps_order_deliv) REFERENCES dds.dm_timestamps_order_deliv(id),
	CONSTRAINT fct_order_deliveries_rates_id_ref FOREIGN KEY (id_rate_deliveries) REFERENCES dds.dm_rates_deliveries(id)
);