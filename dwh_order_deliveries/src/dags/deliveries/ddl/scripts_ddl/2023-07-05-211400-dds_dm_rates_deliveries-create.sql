CREATE TABLE IF NOT EXISTS dds.dm_rates_deliveries (
	id serial4 NOT NULL PRIMARY KEY,
	rate int NOT NULL UNIQUE,
	CONSTRAINT dm_rate_check CHECK ((rate >= 1) AND (rate <= 5))
);