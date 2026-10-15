/* ============================================================================
   SILVER: очищенные, нормализованные, обогащённые сущности.

   customer_dim  — клиент + страна + регион; дедупликация по custkey.
   order_facts   — одна строка на заказ: атрибуты заказа + агрегаты по его
                   позициям (кол-во позиций, суммарное количество товара,
                   суммы). Отсортирован по total_quantity (sorted_by): Q18
                   фильтрует total_quantity > 300, и min/max-статистика
                   row group позволяет Trino пропустить почти весь файл.
   large_orders  — «крупные» заказы (total_quantity > 300) с данными клиента:
                   тот самый подзапрос Q18, вынесенный в слой.

   Очистка: NULL-ключи, количество/цена <= 0, скидка/налог вне [0;1],
            позиции без заказа (INNER JOIN), дубли клиентов.
   Нормализация: DOUBLE -> DECIMAL для денег и количества; trim/upper кодов.
   Обогащение: страна/регион клиента; line_count, charged_amount, доля
            возвратов; флаг price_consistent — сверка totalprice заказа с суммой
            его позиций (правило TPC-H: sum(extendedprice*(1-discount)*(1+tax)),
            с допуском на усечение до копеек в генераторе).
   ========================================================================== */

CREATE SCHEMA IF NOT EXISTS lake.silver;

DROP TABLE IF EXISTS lake.silver.customer_dim;
CREATE TABLE lake.silver.customer_dim WITH (format = 'PARQUET') AS
WITH ranked AS (
    SELECT c.*,
           row_number() OVER (PARTITION BY c.custkey ORDER BY c._ingested_at DESC) AS dup_rank
    FROM lake.bronze.customer c
    WHERE c.custkey IS NOT NULL
)
SELECT
    r.custkey,
    trim(r.name)                       AS customer_name,
    trim(r.address)                    AS address,
    trim(r.phone)                      AS phone,
    CAST(r.acctbal AS DECIMAL(12, 2))  AS acctbal,
    upper(trim(r.mktsegment))          AS mktsegment,
    upper(trim(n.name))                AS nation_name,
    upper(trim(g.name))                AS region_name
FROM ranked r
LEFT JOIN lake.bronze.nation n ON r.nationkey = n.nationkey
LEFT JOIN lake.bronze.region g ON n.regionkey = g.regionkey
WHERE r.dup_rank = 1;

DROP TABLE IF EXISTS lake.silver.order_facts;
CREATE TABLE lake.silver.order_facts
WITH (format = 'PARQUET', sorted_by = ARRAY['total_quantity']) AS
WITH lines AS (
    SELECT
        l.orderkey,
        count(*)                                                        AS line_count,
        CAST(sum(CAST(l.quantity AS DECIMAL(12, 2))) AS DECIMAL(15, 2)) AS total_quantity,
        CAST(sum(CAST(l.extendedprice AS DECIMAL(12, 2))) AS DECIMAL(18, 2)) AS gross_amount,
        CAST(sum(CAST(l.extendedprice AS DECIMAL(12, 2))
                 * (1 - CAST(l.discount AS DECIMAL(4, 2)))
                 * (1 + CAST(l.tax AS DECIMAL(4, 2)))) AS DECIMAL(18, 4)) AS charged_amount,
        count_if(l.returnflag = 'R')                                    AS returned_lines,
        max(l.receiptdate)                                              AS last_receiptdate
    FROM lake.bronze.lineitem l
    WHERE l.orderkey IS NOT NULL
      AND l.quantity > 0
      AND l.extendedprice > 0
      AND l.discount BETWEEN 0 AND 1
      AND l.tax BETWEEN 0 AND 1
    GROUP BY l.orderkey
)
SELECT
    o.orderkey,
    o.custkey,
    o.orderdate,
    year(o.orderdate)                          AS order_year,
    upper(trim(o.orderstatus))                 AS orderstatus,
    upper(trim(o.orderpriority))               AS orderpriority,
    CAST(o.totalprice AS DECIMAL(12, 2))       AS totalprice,
    li.line_count,
    li.total_quantity,
    li.gross_amount,
    li.charged_amount,
    -- допуск 0,021 на позицию: генератор TPC-H считает totalprice в целых копейках и на каждой позиции
    -- дважды усекает результат (после скидки и после налога), теряя до 2,06 коп. на позицию
    abs(CAST(o.totalprice AS DECIMAL(12, 2)) - li.charged_amount)
        <= 0.021 * li.line_count               AS price_consistent,
    li.returned_lines,
    li.last_receiptdate
FROM lake.bronze.orders o
JOIN lines li ON li.orderkey = o.orderkey
WHERE o.custkey IS NOT NULL
  AND o.orderdate IS NOT NULL;

DROP TABLE IF EXISTS lake.silver.large_orders;
CREATE TABLE lake.silver.large_orders WITH (format = 'PARQUET') AS
SELECT
    f.orderkey,
    f.custkey,
    c.customer_name,
    c.mktsegment,
    c.nation_name,
    c.region_name,
    f.orderdate,
    f.order_year,
    f.totalprice,
    f.total_quantity,
    f.line_count
FROM lake.silver.order_facts f
JOIN lake.silver.customer_dim c ON c.custkey = f.custkey
WHERE f.total_quantity > 300;
