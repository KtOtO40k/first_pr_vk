# Сводка результатов (сгенерировано tools/report.py)

## Окружение

| Параметр | Значение |
|---|---|
| Trino | 476 (узлов: 1) |
| Масштаб | sf4 |
| CPU в Docker VM | 12 |
| RAM в Docker VM | 7.5 ГБ |
| Ядро Docker VM | 6.18.40.1-microsoft-standard-WSL2 |

## Слои

### bronze

| Таблица | Строк | Колонок | Файлов | МБ | Формат | Кодек в файле | Row group (макс. файл) | Партиции | Сортировка |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `lake.bronze.region` | 5 | 5 | 1 | 0,001 | PARQUET | ZSTD | 1 | нет | нет |
| `lake.bronze.nation` | 25 | 6 | 1 | 0,002 | PARQUET | ZSTD | 1 | нет | нет |
| `lake.bronze.customer` | 600 000 | 10 | 2 | 32,647 | PARQUET | ZSTD | 1 | нет | нет |
| `lake.bronze.orders` | 6 000 000 | 11 | 9 | 152,531 | PARQUET | ZSTD | 1 | нет | нет |
| `lake.bronze.lineitem` | 23 996 604 | 18 | 12 | 623,400 | PARQUET | ZSTD | 1 | нет | нет |

### silver

| Таблица | Строк | Колонок | Файлов | МБ | Формат | Кодек в файле | Row group (макс. файл) | Партиции | Сортировка |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `lake.silver.customer_dim` | 600 000 | 8 | 1 | 22,772 | PARQUET | ZSTD | 4 | нет | нет |
| `lake.silver.order_facts` | 6 000 000 | 14 | 6 | 137,099 | PARQUET | ZSTD | 14 | нет | total_quantity ASC NULLS FIRST |
| `lake.silver.large_orders` | 249 | 11 | 1 | 0,007 | PARQUET | ZSTD | 1 | нет | нет |

### gold

| Таблица | Строк | Колонок | Файлов | МБ | Формат | Кодек в файле | Row group (макс. файл) | Партиции | Сортировка |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `lake.gold.q18_top100` | 100 | 9 | 1 | 0,004 | PARQUET | ZSTD | 1 | нет | нет |
| `lake.gold.large_orders_by_year_segment` | 175 | 8 | 1 | 0,003 | PARQUET | ZSTD | 1 | нет | нет |
| `lake.gold.order_size_distribution` | 14 | 6 | 1 | 0,001 | PARQUET | ZSTD | 1 | нет | нет |

### Построение слоёв

| Слой | Таблица | Строк | Время, с | CPU, с | Пик памяти, МБ |
|---|---:|---:|---:|---:|---:|
| bronze | `lake.bronze.region` | 5 | 0,24 | 0,1 | 0 |
| bronze | `lake.bronze.nation` | 25 | 0,23 | 0,1 | 0 |
| bronze | `lake.bronze.customer` | 600 000 | 13,28 | 17,9 | 52 |
| bronze | `lake.bronze.orders` | 6 000 000 | 9,99 | 51,7 | 215 |
| bronze | `lake.bronze.lineitem` | 23 996 604 | 36,10 | 230,2 | 819 |
| silver | `lake.silver.customer_dim` | 600 000 | 7,64 | 9,5 | 147 |
| silver | `lake.silver.order_facts` | 6 000 000 | 36,74 | 161,4 | 1 366 |
| silver | `lake.silver.large_orders` | 249 | 1,20 | 1,1 | 71 |
| gold | `lake.gold.q18_top100` | 100 | 0,57 | 0,0 | 0 |
| gold | `lake.gold.large_orders_by_year_segment` | 175 | 2,61 | 16,3 | 178 |
| gold | `lake.gold.order_size_distribution` | 14 | 0,38 | 1,5 | 0 |

### Качество данных

| Проверка | Значение |
|---|---:|
| bronze.lineitem: строк | 23996604 |
| lineitem: NULL orderkey | 0 |
| lineitem: quantity <= 0 | 0 |
| lineitem: extendedprice <= 0 | 0 |
| lineitem: discount или tax вне [0;1] | 0 |
| lineitem: позиции без заказа | 0 |
| bronze.orders: строк | 6000000 |
| orders: заказы без позиций | 0 |
| silver.order_facts: строк | 6000000 |
| order_facts: totalprice расходится с суммой позиций | 0 |
| order_facts: макс. |totalprice − charged_amount| | 0.1148 |
| order_facts: макс. |totalprice − charged_amount| на одну позицию | 0.0205 |
| orders: totalprice ≠ сумме позиций по арифметике генератора (усечение до копеек) | 0 |
| bronze.customer: строк | 600000 |
| customer: дубли custkey | 0 |
| silver.customer_dim: строк | 600000 |
| customer_dim: без страны | 0 |
| order_facts: заказов с total_quantity > 300 | 249 |
| silver.large_orders: строк | 249 |
| order_facts: максимум total_quantity | 328.00 |

## Бенчмарк Bronze (customer + orders + lineitem)

Записей на вариант: 3, замеров чтения: 5 после прогрева. Медиана, в скобках min–max. Жирным — лучшая медиана.

| Вариант | Кодек в файле | Bronze, МБ | lineitem, МБ | Сжатие vs NONE | Запись, с | Запись − генерация, с | Q18, с | Q18 прочитано, МБ | TEXT_SCAN, с | TEXT_SCAN прочитано, МБ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PARQUET + NONE | UNCOMPRESSED | 2 095,1 | 1 515,4 | 1,00× | 46,5 (45,7–46,6) | 18,6 | 2,98 (2,73–4,20) | 533,8 | 1,51 (1,36–1,91) | 759,8 |
| PARQUET + SNAPPY | SNAPPY | 1 115,2 | 837,1 | 1,88× | 45,0 (44,8–50,1) | 17,0 | 2,67 (2,55–4,34) | 230,0 | 1,36 (1,21–2,15) | 333,1 |
| PARQUET + GZIP | GZIP | **788,0** | 609,0 | 2,66× | 63,0 (62,1–75,9) | 35,0 | 2,88 (2,53–4,47) | 139,8 | 1,31 (1,26–1,74) | 219,6 |
| PARQUET + ZSTD | ZSTD | 808,4 | 623,3 | 2,59× | 44,3 (41,1–45,8) | 16,4 | **2,60 (2,41–3,18)** | 129,0 | 1,17 (1,07–1,45) | 233,3 |
| ORC + NONE | UNCOMPRESSED | 2 330,7 | 1 838,3 | 1,00× | 46,6 (45,7–57,0) | 18,7 | 3,31 (2,86–3,70) | 554,7 | 2,11 (1,51–3,83) | 899,5 |
| ORC + SNAPPY | SNAPPY | 1 148,0 | 911,2 | 2,03× | 42,0 (40,1–44,6) | 14,0 | 2,96 (2,76–4,32) | 249,3 | 1,12 (0,99–1,89) | 342,0 |
| ORC + GZIP | ZLIB | 808,8 | 641,4 | 2,88× | 52,9 (51,7–54,2) | 24,9 | 2,95 (2,70–3,17) | 169,6 | 1,23 (0,92–1,70) | 223,5 |
| ORC + ZSTD | ZSTD | 830,6 | 660,8 | 2,81× | **41,7 (40,0–43,2)** | 13,7 | 3,32 (2,95–3,61) | 175,9 | **1,02 (0,88–1,38)** | 231,5 |

Генерация тех же таблиц коннектором tpch без записи: **27,9 с** (медиана).

### Холодный (первый) запуск и CPU

| Вариант | Q18 холодный, с | Q18 CPU, с | TEXT_SCAN холодный, с | TEXT_SCAN CPU, с |
|---|---:|---:|---:|---:|
| PARQUET + NONE | 5,74 | 15,3 | 1,86 | 5,7 |
| PARQUET + SNAPPY | 3,04 | 15,5 | 1,26 | 6,6 |
| PARQUET + GZIP | 2,62 | 16,1 | 1,32 | 7,5 |
| PARQUET + ZSTD | 2,60 | 15,3 | 1,06 | 6,2 |
| ORC + NONE | 3,73 | 17,9 | 1,74 | 5,3 |
| ORC + SNAPPY | 3,50 | 18,0 | 1,04 | 5,9 |
| ORC + GZIP | 2,96 | 18,1 | 1,23 | 6,7 |
| ORC + ZSTD | 3,07 | 18,5 | 1,13 | 5,4 |

### Таблица для шаблона (Parquet)

| Сжатие на Bronze | Время запроса | Размер Bronze, МБ | Вывод |
|---|---:|---:|---:|
| GZIP | 2,88 с | 788,0 |  |
| ZSTD | 2,60 с | 808,4 |  |

## Сортировка Silver и слои медальона (Q18)

| Источник | Таблица | Файлов | МБ | Время, с | Прочитано, МБ | Строк обработано |
|---|---:|---:|---:|---:|---:|---:|
| Bronze: исходный Q18 (3 таблицы + подзапрос) | `bronze.lineitem` | 12 | 623,40 | 2,51 (2,25–3,99) | 133,00 | 55 193 457 |
| Silver order_facts без сортировки | `bm_layout.order_facts_unsorted` | 6 | 133,63 | 0,37 (0,33–1,31) | 78,62 | 7 200 000 |
| Silver order_facts, sorted_by total_quantity | `silver.order_facts` | 6 | 137,10 | 0,23 (0,18–0,53) | 13,31 | 1 938 263 |
| Gold: готовая витрина q18_top100 | `gold.q18_top100` | 1 | 0,00 | 0,04 (0,03–0,08) | 0,00 | 100 |

- silver.order_facts (sorted_by): в 6 файлах таблицы 33 row group (6 000 000 строк), из них max(total_quantity) > 300 у **6** (738 263 строк) — только их Trino должен читать.
- order_facts без сортировки: в 6 файлах таблицы 35 row group (6 000 000 строк), из них max(total_quantity) > 300 у **35** (6 000 000 строк) — только их Trino должен читать.

### Очистка хранилища между записями

| Момент | Бакет до, МБ | Бакет после, МБ | Ожидание, с |
|---|---:|---:|---:|
| PARQUET+NONE после записи #1 | 3065.8 | 3065.8 | 20 |
| PARQUET+NONE после записи #2 | 3065.8 | 3065.8 | 20 |
| после варианта PARQUET+NONE | 3065.5 | 3065.5 | 20 |
| PARQUET+SNAPPY после записи #1 | 4180.7 | 4180.7 | 20 |
| PARQUET+SNAPPY после записи #2 | 4180.7 | 4180.7 | 20 |
| после варианта PARQUET+SNAPPY | 4181.3 | 4181.3 | 20 |
| PARQUET+GZIP после записи #1 | 4970.0 | 4970.0 | 20 |
| PARQUET+GZIP после записи #2 | 4970.0 | 4970.0 | 20 |
| после варианта PARQUET+GZIP | 4970.0 | 4970.0 | 20 |
| PARQUET+ZSTD после записи #1 | 5779.1 | 5779.1 | 20 |
| PARQUET+ZSTD после записи #2 | 5779.0 | 5779.0 | 20 |
| после варианта PARQUET+ZSTD | 5779.0 | 5779.0 | 20 |
| ORC+NONE после записи #1 | 8110.3 | 8110.3 | 20 |
| ORC+NONE после записи #2 | 8110.3 | 8110.3 | 20 |
| после варианта ORC+NONE | 8110.3 | 8110.3 | 20 |
| ORC+SNAPPY после записи #1 | 9259.0 | 9259.0 | 20 |
| ORC+SNAPPY после записи #2 | 9258.9 | 9258.9 | 20 |
| после варианта ORC+SNAPPY | 9259.0 | 9259.0 | 20 |
| ORC+GZIP после записи #1 | 10068.5 | 10068.5 | 20 |
| ORC+GZIP после записи #2 | 10068.5 | 10068.5 | 20 |
| после варианта ORC+GZIP | 10068.5 | 10068.5 | 20 |
| ORC+ZSTD после записи #1 | 10899.7 | 10899.7 | 20 |
| ORC+ZSTD после записи #2 | 10899.6 | 10899.6 | 20 |
| после варианта ORC+ZSTD | 10899.7 | 10899.7 | 20 |

![Размер и запись](charts/bench_storage.png)

![Чтение](charts/bench_reads.png)

# Проверка корректности Gold (Q18)

Заказов с суммарным количеством > 300 в tpch.sf4: **249** (в топ-100 попадают 100).

| # | orderkey raw | orderkey gold | custkey raw / gold | totalprice raw | totalprice gold | qty raw | qty gold |
|---:|---:|---:|---|---:|---:|---:|---:|
| 1 | 22897223 | 22897223 | 328669 / 328669 | 564,350.60 | 564,350.60 | 309 | 309 |
| 2 | 17366275 | 17366275 | 66835 / 66835 | 539,704.09 | 539,704.09 | 316 | 316 |
| 3 | 18449123 | 18449123 | 538331 / 538331 | 537,736.85 | 537,736.85 | 313 | 313 |
| 4 | 22220514 | 22220514 | 44407 / 44407 | 535,926.78 | 535,926.78 | 306 | 306 |
| 5 | 21135524 | 21135524 | 155789 / 155789 | 532,920.05 | 532,920.05 | 320 | 320 |
| 6 | 16139072 | 16139072 | 526468 / 526468 | 526,055.27 | 526,055.27 | 304 | 304 |
| 7 | 11231077 | 11231077 | 205100 / 205100 | 522,991.24 | 522,991.24 | 304 | 304 |
| 8 | 6882 | 6882 | 70978 / 70978 | 522,208.58 | 522,208.58 | 303 | 303 |
| 9 | 4722021 | 4722021 | 512479 / 512479 | 518,421.75 | 518,421.75 | 323 | 323 |
| 10 | 4267751 | 4267751 | 430360 / 430360 | 515,053.98 | 515,053.98 | 301 | 301 |

Строк: raw 100, gold 100. **Итог: СОВПАДАЕТ** (сравнены все строки; показаны первые 10).

## Эталон TPC-H, SF1

| | строк | custkey | orderkey | orderdate | totalprice | quantity |
|---|---:|---:|---:|---|---:|---:|
| Спецификация | 57 | 128120 | 4722021 | 1994-04-07 | 544,089.09 | 323 |
| Trino, tpch.sf1 | 57 | 128120 | 4722021 | 1994-04-07 | 544,089.09 | 323 |

**Итог: СОВПАДАЕТ**. На SF1 условию удовлетворяют только 57 заказов — LIMIT 100 там ничего не отсекает.

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
