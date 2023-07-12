CREATE TABLE IF NOT EXISTS dds.dm_timestamps_order_deliv (
	id serial4 NOT NULL PRIMARY KEY,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);