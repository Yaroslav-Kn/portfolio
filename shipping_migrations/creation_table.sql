-- Удаляем представление, если создано
DROP VIEW IF EXISTS shipping_datamart;

-- Удаляем таблицы, если уже созданы
DROP TABLE IF EXISTS shipping_info;
DROP TABLE IF EXISTS shipping_country_rates;
DROP TABLE IF EXISTS shipping_agreement;
DROP TABLE IF EXISTS shipping_transfer;
DROP TABLE IF EXISTS shipping_status;

-- Создаём справочник стоимости доставки в страны
CREATE TABLE shipping_country_rates(
id SERIAL,
shipping_country VARCHAR(65),
shipping_country_base_rate NUMERIC(14,3),
PRIMARY KEY (id)
);

-- Создаём справочник тарифов доставки вендора по договору
CREATE TABLE shipping_agreement(
agreement_id INT,
agreement_number VARCHAR(15),
agreement_rate NUMERIC(14,2),
agreement_commission NUMERIC(14,2),
PRIMARY KEY (agreement_id)
);

-- Создаём справочник о типах доставки
CREATE TABLE shipping_transfer(
id SERIAL,
transfer_type VARCHAR(5),
transfer_model VARCHAR(15),
shipping_transfer_rate NUMERIC(14,3),
PRIMARY KEY (id)
);

-- Создаём справочник комиссий по странам
CREATE TABLE shipping_info(
shipping_id INT,
vendor_id INT8,
payment_amount NUMERIC(14,2),
shipping_plan_datetime TIMESTAMP,
shipping_transfer_id INT,
shipping_agreement_id INT,
shipping_country_rates_id INT,
PRIMARY KEY (shipping_id),
FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer(id),
FOREIGN KEY (shipping_agreement_id) REFERENCES shipping_agreement(agreement_id),
FOREIGN KEY (shipping_country_rates_id) REFERENCES shipping_country_rates(id)
);

--Создаём таблицу статусов о доставке
CREATE TABLE shipping_status(
shipping_id INT8,
status TEXT, 
state TEXT,
shiping_start_fact_datetime TIMESTAMP,
shipping_end_fact_datetime TIMESTAMP,
PRIMARY KEY (shipping_id)
);