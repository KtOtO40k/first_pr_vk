"""ELT: tpch -> Bronze -> Silver -> Gold для Q18, затем инвентаризация слоёв,
проверки качества данных и проверка корректности витрины.

    docker compose run --rm -T tools python tools/elt.py

Пишет в results/: environment.md, elt_steps.csv, layers.csv, dq_checks.csv,
order_facts_rowgroups.csv, gold_q18_top100.csv, gold_*.csv, validation.md
"""
from __future__ import annotations

import os
import platform
import time
from decimal import Decimal

from lh import (OUT, Q18_THRESHOLD, REPO, SCALE, Lake, data_files, file_codec, new_batch_id,
                parquet_row_groups, q18, save_csv, say, statements)

LAYERS = {
    "bronze": ["region", "nation", "customer", "orders", "lineitem"],
    "silver": ["customer_dim", "order_facts", "large_orders"],
    "gold": ["q18_top100", "large_orders_by_year_segment", "order_size_distribution"],
}
# Эталон TPC-H Q18 для SF1 (quantity > 300): 57 строк, первая строка ответа
SF1_EXPECTED_ROWS = 57
SF1_FIRST = {"custkey": 128120, "orderkey": 4722021, "orderdate": "1994-04-07",
             "totalprice": 544089.09, "quantity": 323}


def wait_trino(lake_factory, limit_s=300):
    t0 = time.time()
    while True:
        try:
            lake = lake_factory()
            lake.one("SELECT count(*) FROM tpch.tiny.region")
            say("Trino отвечает")
            return lake
        except Exception as e:  # noqa: BLE001
            if time.time() - t0 > limit_s:
                raise RuntimeError(f"Trino недоступен: {e}") from e
            time.sleep(3)


def environment(lake: Lake) -> None:
    nodes = lake.q("SELECT node_id, node_version, coordinator FROM system.runtime.nodes").rows
    mem = 0
    try:
        mem = int(next(l for l in open("/proc/meminfo") if l.startswith("MemTotal")).split()[1])
    except (OSError, StopIteration):
        pass
    md = ["| Параметр | Значение |", "|---|---|",
          f"| Trino | {nodes[0][1]} (узлов: {len(nodes)}) |",
          f"| Масштаб | {SCALE} |",
          f"| CPU в Docker VM | {os.cpu_count()} |",
          f"| RAM в Docker VM | {mem / 1024 / 1024:.1f} ГБ |",
          f"| Ядро Docker VM | {platform.release()} |"]
    (OUT / "environment.md").write_text("\n".join(md) + "\n", encoding="utf-8")


def build_layers() -> None:
    batch = new_batch_id()
    say(f"batch_id загрузки: {batch}")
    plan = [
        ("bronze", "10_bronze.sql", {"compression_codec": "ZSTD"}),
        # маленькие row group (8 МБ) — чтобы сортировка по total_quantity давала
        # узкие min/max внутри файла и Trino мог пропускать row group по фильтру Q18
        ("silver", "20_silver.sql", {"compression_codec": "ZSTD", "parquet_writer_block_size": "8MB"}),
        ("gold", "30_gold.sql", {"compression_codec": "ZSTD"}),
    ]
    steps = []
    for layer, fname, session in plan:
        lake = Lake(**session)
        say(f"===== {layer.upper()} ({fname})")
        for stmt in statements(REPO / "etl" / "sql" / fname, scale=SCALE, batch_id=batch):
            head = " ".join(stmt.split())[:100]
            r = lake.q(stmt)
            say(f"{r.server_s:8.2f} s | {head}")
            if head.upper().startswith("CREATE TABLE"):
                steps.append({"layer": layer, "table": head.split()[2], "rows_written": r.rows[0][0],
                              **r.metrics()})
    save_csv("elt_steps.csv", steps)


def inventory(lake: Lake) -> None:
    rows = []
    for layer, tables in LAYERS.items():
        for t in tables:
            fq = f"{layer}.{t}"
            fp = lake.footprint(fq)
            lay = lake.layout(fq)
            cols = [r[0] for r in lake.q(f"SHOW COLUMNS FROM lake.{fq}").rows]
            sample = lake.biggest_file(fq)
            codec = file_codec(sample) if sample else {}
            rows.append({"layer": layer, "table": f"lake.{fq}",
                         "rows": lake.one(f"SELECT count(*) FROM lake.{fq}"),
                         "columns": len(cols), "column_list": ", ".join(cols),
                         "files": fp["files"], "size_bytes": fp["bytes"], "size_mb": fp["mb"],
                         "format": lay["format"], "codec_in_file": codec.get("codec", ""),
                         "row_groups_biggest_file": codec.get("blocks", ""),
                         "partitioning": lay["partitioning"], "sorted_by": lay["sorted_by"]})
            say(f"{fq:42s} {rows[-1]['rows']:>12,} строк {fp['mb']:>10.2f} МБ "
                f"{rows[-1]['codec_in_file']:>8s} sort={lay['sorted_by']}")
    save_csv("layers.csv", rows)


def dq(lake: Lake) -> None:
    checks = [
        ("bronze.lineitem: строк", "SELECT count(*) FROM lake.bronze.lineitem"),
        ("lineitem: NULL orderkey", "SELECT count_if(orderkey IS NULL) FROM lake.bronze.lineitem"),
        ("lineitem: quantity <= 0", "SELECT count_if(quantity <= 0) FROM lake.bronze.lineitem"),
        ("lineitem: extendedprice <= 0", "SELECT count_if(extendedprice <= 0) FROM lake.bronze.lineitem"),
        ("lineitem: discount или tax вне [0;1]",
         "SELECT count_if(discount NOT BETWEEN 0 AND 1 OR tax NOT BETWEEN 0 AND 1) FROM lake.bronze.lineitem"),
        ("lineitem: позиции без заказа",
         "SELECT count(*) FROM lake.bronze.lineitem l LEFT JOIN lake.bronze.orders o "
         "ON o.orderkey = l.orderkey WHERE o.orderkey IS NULL"),
        ("bronze.orders: строк", "SELECT count(*) FROM lake.bronze.orders"),
        ("orders: заказы без позиций",
         "SELECT count(*) FROM lake.bronze.orders o LEFT JOIN "
         "(SELECT DISTINCT orderkey FROM lake.bronze.lineitem) l ON l.orderkey = o.orderkey "
         "WHERE l.orderkey IS NULL"),
        ("silver.order_facts: строк", "SELECT count(*) FROM lake.silver.order_facts"),
        ("order_facts: totalprice расходится с суммой позиций",
         "SELECT count_if(NOT price_consistent) FROM lake.silver.order_facts"),
        ("order_facts: макс. |totalprice − charged_amount|",
         "SELECT max(abs(totalprice - charged_amount)) FROM lake.silver.order_facts"),
        ("order_facts: макс. |totalprice − charged_amount| на одну позицию",
         "SELECT max(abs(totalprice - charged_amount) / line_count) FROM lake.silver.order_facts"),
        # точная проверка: генератор TPC-H считает в копейках и усекает дважды на каждой позиции
        ("orders: totalprice ≠ сумме позиций по арифметике генератора (усечение до копеек)",
         "SELECT count_if(CAST(o.totalprice AS DECIMAL(12, 2)) <> g.gen_total) FROM lake.bronze.orders o JOIN "
         "(SELECT orderkey, sum(truncate(truncate(CAST(extendedprice AS DECIMAL(12, 2)) "
         "* (1 - CAST(discount AS DECIMAL(4, 2))), 2) * (1 + CAST(tax AS DECIMAL(4, 2))), 2)) AS gen_total "
         "FROM lake.bronze.lineitem GROUP BY orderkey) g ON g.orderkey = o.orderkey"),
        ("bronze.customer: строк", "SELECT count(*) FROM lake.bronze.customer"),
        ("customer: дубли custkey", "SELECT count(*) - count(DISTINCT custkey) FROM lake.bronze.customer"),
        ("silver.customer_dim: строк", "SELECT count(*) FROM lake.silver.customer_dim"),
        ("customer_dim: без страны", "SELECT count_if(nation_name IS NULL) FROM lake.silver.customer_dim"),
        (f"order_facts: заказов с total_quantity > {Q18_THRESHOLD}",
         f"SELECT count_if(total_quantity > {Q18_THRESHOLD}) FROM lake.silver.order_facts"),
        ("silver.large_orders: строк", "SELECT count(*) FROM lake.silver.large_orders"),
        ("order_facts: максимум total_quantity", "SELECT max(total_quantity) FROM lake.silver.order_facts"),
    ]
    out = []
    for name, sql in checks:
        v = lake.one(sql)
        out.append({"check": name, "value": v})
        say(f"DQ  {name}: {v}")
    save_csv("dq_checks.csv", out)


def sorted_evidence(lake: Lake) -> None:
    """min/max total_quantity по row group всех файлов order_facts (каждый файл отсортирован отдельно)."""
    try:
        rows = []
        for path in data_files(lake, "silver.order_facts"):
            rows += [{"file": path.rsplit("/", 1)[-1], "row_group": i, "rows": n, "min_total_quantity": lo,
                      "max_total_quantity": hi} for i, n, lo, hi in parquet_row_groups(path, "total_quantity")]
        save_csv("order_facts_rowgroups.csv", rows)
    except Exception as e:  # noqa: BLE001
        say(f"не удалось прочитать row group: {e}")


def validate(lake: Lake) -> None:
    say(f"Проверка: Q18 напрямую по tpch.{SCALE}")
    raw = lake.q(q18(f"tpch.{SCALE}"))
    gold = lake.q("SELECT customer_name, custkey, orderkey, orderdate, totalprice, total_quantity "
                  "FROM lake.gold.q18_top100 ORDER BY top_rank")
    save_csv("gold_q18_top100.csv", [dict(zip(gold.columns, r)) for r in gold.rows])
    for t in ["large_orders_by_year_segment", "order_size_distribution"]:
        r = lake.q(f"SELECT * FROM lake.gold.{t} ORDER BY 1, 2")
        save_csv(f"gold_{t}.csv", [dict(zip(r.columns, x)) for x in r.rows])

    qualifying = lake.one(f"SELECT count(*) FROM (SELECT orderkey FROM tpch.{SCALE}.lineitem "
                          f"GROUP BY orderkey HAVING sum(quantity) > {Q18_THRESHOLD})")
    md = ["# Проверка корректности Gold (Q18)\n",
          f"Заказов с суммарным количеством > {Q18_THRESHOLD} в tpch.{SCALE}: **{qualifying}** "
          f"(в топ-100 попадают {min(qualifying, 100)}).\n",
          "| # | orderkey raw | orderkey gold | custkey raw / gold | totalprice raw | totalprice gold | qty raw | qty gold |",
          "|---:|---:|---:|---|---:|---:|---:|---:|"]
    ok = len(raw.rows) == len(gold.rows)
    for i, (r, g) in enumerate(zip(raw.rows, gold.rows), 1):
        same = (r[2] == g[2] and r[1] == g[1] and abs(float(r[4]) - float(Decimal(str(g[4])))) < 0.005
                and abs(float(r[5]) - float(Decimal(str(g[5])))) < 0.005)
        ok &= same
        if i <= 10 or not same:
            md.append(f"| {i} | {r[2]} | {g[2]} | {r[1]} / {g[1]} | {float(r[4]):,.2f} | {float(g[4]):,.2f} | "
                      f"{float(r[5]):.0f} | {float(g[5]):.0f} |")
    md.append(f"\nСтрок: raw {len(raw.rows)}, gold {len(gold.rows)}. "
              f"**Итог: {'СОВПАДАЕТ' if ok else 'РАСХОЖДЕНИЕ'}** (сравнены все строки; показаны первые 10).\n")

    say("Проверка: эталон TPC-H Q18 для SF1")
    sf1 = lake.q(q18("tpch.sf1"))
    f = sf1.rows[0] if sf1.rows else [None] * 6
    sf1_ok = (len(sf1.rows) == SF1_EXPECTED_ROWS and f[1] == SF1_FIRST["custkey"] and f[2] == SF1_FIRST["orderkey"]
              and str(f[3]) == SF1_FIRST["orderdate"] and abs(float(f[4]) - SF1_FIRST["totalprice"]) < 0.005
              and abs(float(f[5]) - SF1_FIRST["quantity"]) < 0.005)
    md += ["## Эталон TPC-H, SF1\n", "| | строк | custkey | orderkey | orderdate | totalprice | quantity |",
           "|---|---:|---:|---:|---|---:|---:|",
           f"| Спецификация | {SF1_EXPECTED_ROWS} | {SF1_FIRST['custkey']} | {SF1_FIRST['orderkey']} | "
           f"{SF1_FIRST['orderdate']} | {SF1_FIRST['totalprice']:,.2f} | {SF1_FIRST['quantity']} |",
           f"| Trino, tpch.sf1 | {len(sf1.rows)} | {f[1]} | {f[2]} | {f[3]} | {float(f[4]):,.2f} | {float(f[5]):.0f} |",
           f"\n**Итог: {'СОВПАДАЕТ' if sf1_ok else 'РАСХОЖДЕНИЕ'}**. На SF1 условию удовлетворяют только "
           f"{len(sf1.rows)} заказов — LIMIT 100 там ничего не отсекает.\n"]
    (OUT / "validation.md").write_text("\n".join(md), encoding="utf-8")
    say(f"Gold vs raw: {'OK' if ok else 'FAIL'}; эталон SF1: {'OK' if sf1_ok else 'FAIL'}")


def main() -> None:
    lake = wait_trino(Lake)
    environment(lake)
    build_layers()
    lake = Lake()
    inventory(lake)
    dq(lake)
    sorted_evidence(lake)
    validate(lake)
    say("ELT готов. Дальше: tools/iceberg_demo.py, затем ./run_bench.sh")


if __name__ == "__main__":
    main()
