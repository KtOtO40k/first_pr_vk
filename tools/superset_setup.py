"""Часть 2, п. 6.2: Superset как BI-инструмент поверх Lakehouse.

Через REST API Superset (подключение «Lakehouse (Trino)» создаёт сам контейнер):
  1. SQL Lab: запрос к Bronze (lake.bronze.lineitem/orders) — результат сохраняется
     в results/superset_bronze_query.json как доказательство чтения Bronze из BI;
  2. сохранённый запрос SQL Lab по Bronze;
  3. датасеты: bronze.orders, gold.q18_top100, gold.large_orders_by_year_segment,
     gold.order_size_distribution;
  4. графики и дашборд «Q18 — крупные заказы».

    docker compose --profile bi up -d superset
    docker compose run --rm -T tools python tools/superset_setup.py
"""
from __future__ import annotations

import json
import os
import time

import requests

from lh import Q18_THRESHOLD, save_json, say

URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
DB_NAME = "Lakehouse (Trino)"
DASH_TITLE = "Q18 — крупные заказы (Lakehouse)"
s = requests.Session()


def api(method: str, path: str, **kw):
    r = s.request(method, f"{URL}/api/v1/{path}", timeout=600, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path}: HTTP {r.status_code} {r.text[:400]}")
    return r.json() if r.content else {}


def login() -> None:
    for i in range(60):
        try:
            if requests.get(f"{URL}/health", timeout=5).ok:
                break
        except requests.RequestException:
            pass
        say(f"ждём Superset ({i + 1})")
        time.sleep(5)
    tok = api("POST", "security/login", json={"username": "admin", "password": "admin",
                                               "provider": "db", "refresh": True})["access_token"]
    s.headers["Authorization"] = f"Bearer {tok}"
    try:
        csrf = api("GET", "security/csrf_token/")["result"]
        s.headers["X-CSRFToken"] = csrf
        s.headers["Referer"] = URL
    except RuntimeError:
        pass  # CSRF выключен в superset_config.py


def find(resource: str, field: str, value: str):
    q = f"(filters:!((col:{field},opr:eq,value:'{value}')),page_size:100)"
    res = api("GET", f"{resource}/", params={"q": q})["result"]
    return res[0]["id"] if res else None


def database_id() -> int:
    db = find("database", "database_name", DB_NAME)
    if not db:
        raise RuntimeError(f"нет подключения «{DB_NAME}» — проверь логи контейнера lh-superset")
    return db


def dataset(db: int, schema: str, table: str) -> int:
    q = f"(filters:!((col:table_name,opr:eq,value:'{table}')),page_size:100)"
    for ds in api("GET", "dataset/", params={"q": q})["result"]:
        if ds.get("schema") == schema:
            return ds["id"]
    return api("POST", "dataset/", json={"database": db, "schema": schema, "table_name": table})["id"]


def metric(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def chart(name: str, viz: str, ds: int, params: dict, dash: int) -> int:
    old = find("chart", "slice_name", name)
    params = {"viz_type": viz, "datasource": f"{ds}__table", **params}
    body = {"slice_name": name, "viz_type": viz, "datasource_id": ds, "datasource_type": "table",
            "params": json.dumps(params, ensure_ascii=False), "dashboards": [dash]}
    if old:
        api("PUT", f"chart/{old}", json=body)
        return old
    return api("POST", "chart/", json=body)["id"]


def main() -> None:
    login()
    db = database_id()
    say(f"подключение к Trino: database id {db}")

    bronze_sql = (f"SELECT size_class, count(*) AS orders FROM (\n"
                  f"  SELECT orderkey, CASE WHEN sum(quantity) > {Q18_THRESHOLD} THEN 'крупный (> {Q18_THRESHOLD})'\n"
                  f"                        WHEN sum(quantity) > 200 THEN 'большой (201–{Q18_THRESHOLD})'\n"
                  f"                        ELSE 'обычный (≤ 200)' END AS size_class\n"
                  f"  FROM lake.bronze.lineitem GROUP BY orderkey) t\n"
                  f"GROUP BY size_class ORDER BY orders DESC")
    say("SQL Lab: запрос к Bronze")
    t0 = time.time()
    res = api("POST", "sqllab/execute/", json={"database_id": db, "sql": bronze_sql, "schema": "bronze",
                                                "runAsync": False, "json": True})
    save_json("superset_bronze_query.json", {"sql": bronze_sql, "seconds": round(time.time() - t0, 2),
                                              "columns": [c.get("column_name", c.get("name")) for c in res.get("columns", [])],
                                              "data": res.get("data")})
    if not find("saved_query", "label", "Bronze: распределение заказов по размеру"):
        api("POST", "saved_query/", json={"db_id": db, "schema": "bronze", "sql": bronze_sql,
                                          "label": "Bronze: распределение заказов по размеру"})

    dash = find("dashboard", "dashboard_title", DASH_TITLE) or \
        api("POST", "dashboard/", json={"dashboard_title": DASH_TITLE, "published": True})["id"]

    ds_orders = dataset(db, "bronze", "orders")
    ds_top = dataset(db, "gold", "q18_top100")
    ds_year = dataset(db, "gold", "large_orders_by_year_segment")
    ds_dist = dataset(db, "gold", "order_size_distribution")

    charts = {
        "Bronze: заказы по приоритету и статусу": chart(
            "Bronze: заказы по приоритету и статусу", "echarts_timeseries_bar", ds_orders,
            {"x_axis": "orderpriority", "metrics": [metric("COUNT(*)", "заказов")], "groupby": ["orderstatus"],
             "row_limit": 1000, "stack": "Stack", "show_legend": True}, dash),
        "Крупных заказов всего": chart(
            "Крупных заказов всего", "big_number_total", ds_year,
            {"metric": metric("SUM(large_orders)", "крупных заказов"), "subheader": f"total_quantity > {Q18_THRESHOLD}"},
            dash),
        "Крупные заказы по годам и сегментам": chart(
            "Крупные заказы по годам и сегментам", "echarts_timeseries_bar", ds_year,
            {"x_axis": "order_year", "metrics": [metric("SUM(large_orders)", "крупных заказов")],
             "groupby": ["mktsegment"], "row_limit": 1000, "stack": "Stack", "show_legend": True}, dash),
        "Распределение заказов по количеству товара": chart(
            "Распределение заказов по количеству товара", "echarts_timeseries_bar", ds_dist,
            {"x_axis": "size_bucket", "metrics": [metric("SUM(orders)", "заказов")], "row_limit": 100,
             "logAxis": True, "show_legend": False}, dash),
        "Q18: топ-100 крупных заказов": chart(
            "Q18: топ-100 крупных заказов", "table", ds_top,
            {"query_mode": "raw", "all_columns": ["top_rank", "customer_name", "custkey", "orderkey", "orderdate",
                                                   "totalprice", "total_quantity", "mktsegment", "nation_name"],
             "order_by_cols": [json.dumps(["top_rank", True])], "row_limit": 100}, dash),
    }
    summary = {"superset": URL, "database_id": db, "dashboard_id": dash,
               "dashboard_url": f"{URL}/superset/dashboard/{dash}/",
               "datasets": {"bronze.orders": ds_orders, "gold.q18_top100": ds_top,
                            "gold.large_orders_by_year_segment": ds_year, "gold.order_size_distribution": ds_dist},
               "charts": charts}
    save_json("superset_setup.json", summary)
    say(f"дашборд: {summary['dashboard_url']} (admin / admin)")


if __name__ == "__main__":
    main()
