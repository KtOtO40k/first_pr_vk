"""Часть 2: Schema Evolution и Time Travel на Silver (копия silver.large_orders).

Сценарий:
  1. Эволюция схемы без перезаписи данных:
       ADD COLUMN size_class, RENAME COLUMN total_quantity -> total_qty,
       расширение типа totalprice DECIMAL(12,2) -> DECIMAL(18,2).
  2. UPDATE заполняет size_class.                         -> «хороший» снапшот
  3. Сбой ETL: UPDATE обнулил totalprice у заказов 1995+ года.
  4. После сбоя приходит легитимная запись (INSERT нового заказа).
  5. Time Travel: читаем «хороший» снапшот (FOR VERSION / FOR TIMESTAMP AS OF).
  6. Восстановление A — точечное: MERGE возвращает только испорченную колонку
     из старого снапшота, легитимная новая запись сохраняется.
  7. Восстановление B — полный откат rollback_to_snapshot: возвращает всё,
     но теряет и легитимную запись. Сравниваем оба способа.

    docker compose run --rm -T tools python tools/iceberg_demo.py
Протокол: results/iceberg_demo.md
"""
from __future__ import annotations

from lh import OUT, Lake, say

T = "lake.silver.large_orders_evo"
SNAP = 'lake.silver."large_orders_evo$snapshots"'
FILES = 'lake.silver."large_orders_evo$files"'
log: list[str] = ["# Iceberg: Schema Evolution и Time Travel — протокол\n"]


def run(lake: Lake, title: str, sql: str, show: bool = True, limit: int = 10):
    say(title)
    r = lake.q(sql)
    log.append(f"**{title}**\n\n```sql\n{sql.strip()}\n```\n")
    if show and r.columns:
        log.append("| " + " | ".join(r.columns) + " |")
        log.append("|" + "---|" * len(r.columns))
        for row in r.rows[:limit]:
            log.append("| " + " | ".join("NULL" if v is None else str(v) for v in row) + " |")
        if len(r.rows) > limit:
            log.append(f"\n_… всего строк: {len(r.rows)}_")
        log.append("")
    return r


def state(lake: Lake) -> dict:
    orders, amount, zeros = lake.q(f"SELECT count(*) AS orders, sum(totalprice) AS amount, "
                                   f"count_if(totalprice = 0) AS zero_prices FROM {T}").rows[0]
    return {"orders": orders, "amount": amount, "zero_prices": zeros}


def snapshots(lake: Lake):
    return run(lake, "История снапшотов",
               f"SELECT snapshot_id, committed_at, operation, "
               f"element_at(summary, 'added-data-files') AS added_files, "
               f"element_at(summary, 'added-delete-files') AS added_delete_files, "
               f"element_at(summary, 'total-records') AS total_records "
               f"FROM {SNAP} ORDER BY committed_at")


def main() -> None:
    lake = Lake(compression_codec="ZSTD")
    lake.q(f"DROP TABLE IF EXISTS {T}")
    lake.q("DROP TABLE IF EXISTS lake.silver.large_orders_restored")

    log.append("## 1. Schema Evolution\n")
    run(lake, "Рабочая копия крупных заказов",
        f"CREATE TABLE {T} WITH (format = 'PARQUET') AS SELECT * FROM lake.silver.large_orders", show=False)
    run(lake, "Схема до изменений", f"DESCRIBE {T}")
    files_before = lake.one(f"SELECT count(*) FROM {FILES}")
    run(lake, "Новая колонка", f"ALTER TABLE {T} ADD COLUMN size_class VARCHAR", show=False)
    run(lake, "Переименование колонки", f"ALTER TABLE {T} RENAME COLUMN total_quantity TO total_qty", show=False)
    widened = True
    try:
        run(lake, "Расширение типа (суммы растут — DECIMAL(12,2) → DECIMAL(18,2))",
            f"ALTER TABLE {T} ALTER COLUMN totalprice SET DATA TYPE DECIMAL(18, 2)", show=False)
    except Exception as e:  # noqa: BLE001
        widened = False
        log.append(f"_Расширение типа не поддержано этой версией: `{str(e)[:160]}`_\n")
    run(lake, "Схема после изменений", f"DESCRIBE {T}")
    files_after = lake.one(f"SELECT count(*) FROM {FILES}")
    run(lake, "Старые строки читаются по новой схеме",
        f"SELECT orderkey, customer_name, total_qty, totalprice, size_class FROM {T} ORDER BY orderkey LIMIT 5")
    log.append(f"Файлов данных до ALTER: **{files_before}**, после: **{files_after}**. "
               f"Iceberg записал только новую версию схемы (metadata.json); колонки сопоставляются "
               f"по id поля, поэтому переименование и расширение типа не требуют переписывать файлы. "
               f"Расширение типа: {'выполнено' if widened else 'не поддержано'}.\n")

    run(lake, "Заполняем новую колонку",
        f"UPDATE {T} SET size_class = CASE WHEN total_qty >= 320 THEN 'XL' ELSE 'L' END", show=False)
    run(lake, "Результат", f"SELECT size_class, count(*) AS orders FROM {T} GROUP BY size_class ORDER BY 1")

    log.append("## 2. Сбой, Time Travel, восстановление\n")
    good_id = lake.one(f"SELECT snapshot_id FROM {SNAP} ORDER BY committed_at DESC LIMIT 1")
    good_ts = lake.one(f"SELECT CAST(committed_at AS varchar) FROM {SNAP} WHERE snapshot_id = {good_id}")
    good = state(lake)
    log.append(f"«Хороший» снапшот: `{good_id}` ({good_ts}): {good}\n")

    run(lake, "Сбой ETL: обнулены суммы заказов 1995+ года",
        f"UPDATE {T} SET totalprice = 0 WHERE order_year >= 1995", show=False)
    broken = state(lake)
    log.append(f"После сбоя: {broken}\n")
    run(lake, "После сбоя приходит легитимный новый заказ",
        f"INSERT INTO {T} (orderkey, custkey, customer_name, mktsegment, nation_name, region_name, orderdate, "
        f"order_year, totalprice, total_qty, line_count, size_class) "
        f"SELECT 999999999, custkey, customer_name, mktsegment, nation_name, region_name, DATE '1998-08-01', "
        f"1998, 123456.78, total_qty, line_count, 'L' FROM {T} ORDER BY orderkey LIMIT 1", show=False)
    with_new = state(lake)
    snapshots(lake)

    run(lake, "Time Travel по id снапшота", f"SELECT count(*) AS orders, sum(totalprice) AS amount, "
        f"count_if(totalprice = 0) AS zero_prices FROM {T} FOR VERSION AS OF {good_id}")
    run(lake, "Time Travel по времени", f"SELECT count(*) AS orders, sum(totalprice) AS amount "
        f"FROM {T} FOR TIMESTAMP AS OF TIMESTAMP '{good_ts}'")

    log.append("### Восстановление A — точечно, MERGE из старого снапшота\n")
    run(lake, "Возвращаем только испорченную колонку",
        f"MERGE INTO {T} t USING (SELECT orderkey, totalprice FROM {T} FOR VERSION AS OF {good_id}) s "
        f"ON t.orderkey = s.orderkey "
        f"WHEN MATCHED AND t.totalprice <> s.totalprice THEN UPDATE SET totalprice = s.totalprice", show=False)
    merged = state(lake)
    new_kept = lake.one(f"SELECT count(*) FROM {T} WHERE orderkey = 999999999")
    log.append(f"После MERGE: {merged}; новый заказ на месте: {bool(new_kept)}\n")

    log.append("### Восстановление B — полный откат rollback_to_snapshot\n")
    run(lake, "Копия «как было» в отдельную таблицу (CTAS из снапшота)",
        f"CREATE TABLE lake.silver.large_orders_restored AS SELECT * FROM {T} FOR VERSION AS OF {good_id}",
        show=False)
    try:
        run(lake, "Откат таблицы", f"ALTER TABLE {T} EXECUTE rollback_to_snapshot({good_id})", show=False)
    except Exception as e:  # noqa: BLE001
        say(f"ALTER ... EXECUTE недоступен ({str(e)[:80]}), использую процедуру")
        run(lake, "Откат таблицы (процедура)",
            f"CALL lake.system.rollback_to_snapshot('silver', 'large_orders_evo', {good_id})", show=False)
    rolled = state(lake)
    new_after_rb = lake.one(f"SELECT count(*) FROM {T} WHERE orderkey = 999999999")
    snapshots(lake)

    log.extend(["## Итог\n", "| Состояние | Заказов | Сумма totalprice | Обнулённых сумм | Новый заказ |",
            "|---|---:|---:|---:|---|",
            f"| «Хороший» снапшот | {good['orders']} | {good['amount']} | {good['zero_prices']} | — |",
            f"| После сбоя ETL | {broken['orders']} | {broken['amount']} | {broken['zero_prices']} | — |",
            f"| + легитимная запись | {with_new['orders']} | {with_new['amount']} | {with_new['zero_prices']} | есть |",
            f"| A: MERGE из снапшота | {merged['orders']} | {merged['amount']} | {merged['zero_prices']} | "
            f"{'сохранён' if new_kept else 'потерян'} |",
            f"| B: rollback_to_snapshot | {rolled['orders']} | {rolled['amount']} | {rolled['zero_prices']} | "
            f"{'сохранён' if new_after_rb else 'потерян'} |", ""])
    (OUT / "iceberg_demo.md").write_text("\n".join(log), encoding="utf-8")
    say(f"готово: good={good} broken={broken} merged={merged} rollback={rolled}")


if __name__ == "__main__":
    main()
