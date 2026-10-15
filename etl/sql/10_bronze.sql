/* ============================================================================
   BRONZE: сырые таблицы TPC-H без изменений + метки загрузки.
   Источник — коннектор tpch, масштаб {{scale}}. Формат PARQUET, кодек ZSTD
   (задан в каталоге lake: iceberg.compression-codec=ZSTD).
   _batch_id / _ingested_at — технические колонки: какой загрузкой и когда
   пришла строка (аудит, идемпотентная перезагрузка, дедупликация в Silver).
   ========================================================================== */

CREATE SCHEMA IF NOT EXISTS lake.bronze;

DROP TABLE IF EXISTS lake.bronze.region;
CREATE TABLE lake.bronze.region WITH (format = 'PARQUET') AS
SELECT t.*, '{{batch_id}}' AS _batch_id, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at
FROM tpch.{{scale}}.region t;

DROP TABLE IF EXISTS lake.bronze.nation;
CREATE TABLE lake.bronze.nation WITH (format = 'PARQUET') AS
SELECT t.*, '{{batch_id}}' AS _batch_id, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at
FROM tpch.{{scale}}.nation t;

DROP TABLE IF EXISTS lake.bronze.customer;
CREATE TABLE lake.bronze.customer WITH (format = 'PARQUET') AS
SELECT t.*, '{{batch_id}}' AS _batch_id, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at
FROM tpch.{{scale}}.customer t;

DROP TABLE IF EXISTS lake.bronze.orders;
CREATE TABLE lake.bronze.orders WITH (format = 'PARQUET') AS
SELECT t.*, '{{batch_id}}' AS _batch_id, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at
FROM tpch.{{scale}}.orders t;

DROP TABLE IF EXISTS lake.bronze.lineitem;
CREATE TABLE lake.bronze.lineitem WITH (format = 'PARQUET') AS
SELECT t.*, '{{batch_id}}' AS _batch_id, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at
FROM tpch.{{scale}}.lineitem t;
