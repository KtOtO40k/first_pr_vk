# Iceberg: Schema Evolution и Time Travel — протокол

## 1. Schema Evolution

**Рабочая копия крупных заказов**

```sql
CREATE TABLE lake.silver.large_orders_evo WITH (format = 'PARQUET') AS SELECT * FROM lake.silver.large_orders
```

**Схема до изменений**

```sql
DESCRIBE lake.silver.large_orders_evo
```

| Column | Type | Extra | Comment |
|---|---|---|---|
| orderkey | bigint |  |  |
| custkey | bigint |  |  |
| customer_name | varchar |  |  |
| mktsegment | varchar |  |  |
| nation_name | varchar |  |  |
| region_name | varchar |  |  |
| orderdate | date |  |  |
| order_year | bigint |  |  |
| totalprice | decimal(12,2) |  |  |
| total_quantity | decimal(15,2) |  |  |

_… всего строк: 11_

**Новая колонка**

```sql
ALTER TABLE lake.silver.large_orders_evo ADD COLUMN size_class VARCHAR
```

**Переименование колонки**

```sql
ALTER TABLE lake.silver.large_orders_evo RENAME COLUMN total_quantity TO total_qty
```

**Расширение типа (суммы растут — DECIMAL(12,2) → DECIMAL(18,2))**

```sql
ALTER TABLE lake.silver.large_orders_evo ALTER COLUMN totalprice SET DATA TYPE DECIMAL(18, 2)
```

**Схема после изменений**

```sql
DESCRIBE lake.silver.large_orders_evo
```

| Column | Type | Extra | Comment |
|---|---|---|---|
| orderkey | bigint |  |  |
| custkey | bigint |  |  |
| customer_name | varchar |  |  |
| mktsegment | varchar |  |  |
| nation_name | varchar |  |  |
| region_name | varchar |  |  |
| orderdate | date |  |  |
| order_year | bigint |  |  |
| totalprice | decimal(18,2) |  |  |
| total_qty | decimal(15,2) |  |  |

_… всего строк: 12_

**Старые строки читаются по новой схеме**

```sql
SELECT orderkey, customer_name, total_qty, totalprice, size_class FROM lake.silver.large_orders_evo ORDER BY orderkey LIMIT 5
```

| orderkey | customer_name | total_qty | totalprice | size_class |
|---|---|---|---|---|
| 6882 | Customer#000070978 | 303.00 | 522208.58 | NULL |
| 29158 | Customer#000266129 | 305.00 | 414117.20 | NULL |
| 502886 | Customer#000065534 | 312.00 | 436520.22 | NULL |
| 551136 | Customer#000458344 | 308.00 | 456462.18 | NULL |
| 565574 | Customer#000564391 | 301.00 | 483333.08 | NULL |

Файлов данных до ALTER: **1**, после: **1**. Iceberg записал только новую версию схемы (metadata.json); колонки сопоставляются по id поля, поэтому переименование и расширение типа не требуют переписывать файлы. Расширение типа: выполнено.

**Заполняем новую колонку**

```sql
UPDATE lake.silver.large_orders_evo SET size_class = CASE WHEN total_qty >= 320 THEN 'XL' ELSE 'L' END
```

**Результат**

```sql
SELECT size_class, count(*) AS orders FROM lake.silver.large_orders_evo GROUP BY size_class ORDER BY 1
```

| size_class | orders |
|---|---|
| L | 233 |
| XL | 16 |

## 2. Сбой, Time Travel, восстановление

«Хороший» снапшот: `2291032443032949736` (2026-10-12 19:05:13.969 UTC): {'orders': 249, 'amount': Decimal('113403560.04'), 'zero_prices': 0}

**Сбой ETL: обнулены суммы заказов 1995+ года**

```sql
UPDATE lake.silver.large_orders_evo SET totalprice = 0 WHERE order_year >= 1995
```

После сбоя: {'orders': 249, 'amount': Decimal('51052351.01'), 'zero_prices': 138}

**После сбоя приходит легитимный новый заказ**

```sql
INSERT INTO lake.silver.large_orders_evo (orderkey, custkey, customer_name, mktsegment, nation_name, region_name, orderdate, order_year, totalprice, total_qty, line_count, size_class) SELECT 999999999, custkey, customer_name, mktsegment, nation_name, region_name, DATE '1998-08-01', 1998, 123456.78, total_qty, line_count, 'L' FROM lake.silver.large_orders_evo ORDER BY orderkey LIMIT 1
```

**История снапшотов**

```sql
SELECT snapshot_id, committed_at, operation, element_at(summary, 'added-data-files') AS added_files, element_at(summary, 'added-delete-files') AS added_delete_files, element_at(summary, 'total-records') AS total_records FROM lake.silver."large_orders_evo$snapshots" ORDER BY committed_at
```

| snapshot_id | committed_at | operation | added_files | added_delete_files | total_records |
|---|---|---|---|---|---|
| 4737157008766802078 | 2026-10-12 19:05:12.842000+00:00 | append | 1 | NULL | 249 |
| 2291032443032949736 | 2026-10-12 19:05:13.969000+00:00 | overwrite | 1 | 1 | 498 |
| 683382510239451544 | 2026-10-12 19:05:14.558000+00:00 | overwrite | 1 | 1 | 636 |
| 5391568584304992579 | 2026-10-12 19:05:14.967000+00:00 | append | 1 | NULL | 637 |

**Time Travel по id снапшота**

```sql
SELECT count(*) AS orders, sum(totalprice) AS amount, count_if(totalprice = 0) AS zero_prices FROM lake.silver.large_orders_evo FOR VERSION AS OF 2291032443032949736
```

| orders | amount | zero_prices |
|---|---|---|
| 249 | 113403560.04 | 0 |

**Time Travel по времени**

```sql
SELECT count(*) AS orders, sum(totalprice) AS amount FROM lake.silver.large_orders_evo FOR TIMESTAMP AS OF TIMESTAMP '2026-10-12 19:05:13.969 UTC'
```

| orders | amount |
|---|---|
| 249 | 113403560.04 |

### Восстановление A — точечно, MERGE из старого снапшота

**Возвращаем только испорченную колонку**

```sql
MERGE INTO lake.silver.large_orders_evo t USING (SELECT orderkey, totalprice FROM lake.silver.large_orders_evo FOR VERSION AS OF 2291032443032949736) s ON t.orderkey = s.orderkey WHEN MATCHED AND t.totalprice <> s.totalprice THEN UPDATE SET totalprice = s.totalprice
```

После MERGE: {'orders': 250, 'amount': Decimal('113527016.82'), 'zero_prices': 0}; новый заказ на месте: True

### Восстановление B — полный откат rollback_to_snapshot

**Копия «как было» в отдельную таблицу (CTAS из снапшота)**

```sql
CREATE TABLE lake.silver.large_orders_restored AS SELECT * FROM lake.silver.large_orders_evo FOR VERSION AS OF 2291032443032949736
```

**Откат таблицы**

```sql
ALTER TABLE lake.silver.large_orders_evo EXECUTE rollback_to_snapshot(2291032443032949736)
```

**История снапшотов**

```sql
SELECT snapshot_id, committed_at, operation, element_at(summary, 'added-data-files') AS added_files, element_at(summary, 'added-delete-files') AS added_delete_files, element_at(summary, 'total-records') AS total_records FROM lake.silver."large_orders_evo$snapshots" ORDER BY committed_at
```

| snapshot_id | committed_at | operation | added_files | added_delete_files | total_records |
|---|---|---|---|---|---|
| 4737157008766802078 | 2026-10-12 19:05:12.842000+00:00 | append | 1 | NULL | 249 |
| 2291032443032949736 | 2026-10-12 19:05:13.969000+00:00 | overwrite | 1 | 1 | 498 |
| 683382510239451544 | 2026-10-12 19:05:14.558000+00:00 | overwrite | 1 | 1 | 636 |
| 5391568584304992579 | 2026-10-12 19:05:14.967000+00:00 | append | 1 | NULL | 637 |
| 5308893126731736580 | 2026-10-12 19:05:15.734000+00:00 | overwrite | 1 | 1 | 775 |

## Итог

| Состояние | Заказов | Сумма totalprice | Обнулённых сумм | Новый заказ |
|---|---:|---:|---:|---|
| «Хороший» снапшот | 249 | 113403560.04 | 0 | — |
| После сбоя ETL | 249 | 51052351.01 | 138 | — |
| + легитимная запись | 250 | 51175807.79 | 138 | есть |
| A: MERGE из снапшота | 250 | 113527016.82 | 0 | сохранён |
| B: rollback_to_snapshot | 249 | 113403560.04 | 0 | потерян |
