/* ============================================================================
   GOLD: витрины для бизнес-аналитики (Superset).

   q18_top100                   — ответ на Q18: топ-100 крупных заказов по сумме.
   large_orders_by_year_segment — сколько крупных заказов по годам, сегментам
                                  и регионам и какая это доля всех заказов.
   order_size_distribution      — распределение заказов по суммарному количеству
                                  товара (шаг 25): видно, насколько редки > 300.
   ========================================================================== */

CREATE SCHEMA IF NOT EXISTS lake.gold;

DROP TABLE IF EXISTS lake.gold.q18_top100;
CREATE TABLE lake.gold.q18_top100 WITH (format = 'PARQUET') AS
SELECT
    CAST(row_number() OVER (ORDER BY totalprice DESC, orderdate, orderkey) AS INTEGER) AS top_rank,
    customer_name,
    custkey,
    orderkey,
    orderdate,
    totalprice,
    total_quantity,
    mktsegment,
    nation_name
FROM lake.silver.large_orders
ORDER BY totalprice DESC, orderdate, orderkey
LIMIT 100;

DROP TABLE IF EXISTS lake.gold.large_orders_by_year_segment;
CREATE TABLE lake.gold.large_orders_by_year_segment WITH (format = 'PARQUET') AS
SELECT
    f.order_year,
    c.mktsegment,
    c.region_name,
    count(*)                                              AS all_orders,
    count_if(f.total_quantity > 300)                      AS large_orders,
    count(DISTINCT CASE WHEN f.total_quantity > 300 THEN f.custkey END) AS large_customers,
    -- 100e0 (DOUBLE), а не 100.0: с DECIMAL-литералом деление идёт с одним знаком после запятой,
    -- и доля в сотые процента округляется до 0,0
    CAST(100e0 * count_if(f.total_quantity > 300) / count(*) AS DECIMAL(9, 4)) AS large_share_pct,
    sum(CASE WHEN f.total_quantity > 300 THEN f.totalprice END) AS large_orders_amount
FROM lake.silver.order_facts f
JOIN lake.silver.customer_dim c ON c.custkey = f.custkey
GROUP BY f.order_year, c.mktsegment, c.region_name;

DROP TABLE IF EXISTS lake.gold.order_size_distribution;
CREATE TABLE lake.gold.order_size_distribution WITH (format = 'PARQUET') AS
WITH b AS (
    SELECT CAST(floor((total_quantity - 1) / 25) AS INTEGER) * 25 + 1 AS qty_from
    FROM lake.silver.order_facts
)
SELECT
    qty_from,
    qty_from + 24                                              AS qty_to,
    format('%03d–%03d', qty_from, qty_from + 24)               AS size_bucket,
    qty_from > 300                                             AS is_large,
    count(*)                                                   AS orders,
    CAST(100e0 * count(*) / sum(count(*)) OVER () AS DECIMAL(9, 4)) AS share_pct
FROM b
GROUP BY qty_from;
