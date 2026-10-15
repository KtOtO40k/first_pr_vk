"""Бенчмарк хранения Bronze для Q18: {PARQUET, ORC} x {NONE, SNAPPY, GZIP, ZSTD}
+ эксперимент «сортировка Silver и слои медальона».

Этапы (можно запускать по отдельности — так делает run_bench.sh):
  --writes-only  записать Bronze-варианты (WRITE_REPEATS раз каждый, медиана);
  --reads-only   базовая линия генератора, чтение всех вариантов, эксперимент с сортировкой;
  --layout-only  только эксперимент с сортировкой.
  --variants PARQUET:ZSTD,ORC:GZIP   ограничить набор вариантов.

Почему этапы разделены: каждая перезапись удаляет старые файлы, а MinIO/Lakekeeper
освобождают место асинхронно. Между вариантами скрипт ждёт, пока занятый объём
бакета перестанет уменьшаться (wait_for_purge) — иначе диск Docker раздувается,
а замеры записи искажаются (см. results/bench/storage_log.csv).

Результаты: results/bench/*.csv
"""
from __future__ import annotations

import argparse
import random
import re
import time

from lh import (READ_REPEATS, REPO, SCALE, WRITE_REPEATS, Lake, bucket_usage_mb, data_files, file_codec,
                load_csv, parquet_row_groups, q18, q18_from_silver, save_csv, say, statements,
                text_scan)

FORMATS = ["PARQUET", "ORC"]
CODECS = ["NONE", "SNAPPY", "GZIP", "ZSTD"]
TABLES = ["customer", "orders", "lineitem"]   # всё, что читает Q18
B = "bench/"


def schema_of(fmt: str, codec: str) -> str:
    return f"bm_{fmt}_{codec}".lower()


def wait_for_purge(label: str, max_wait_s: int = 300) -> None:
    """Ждём, пока объём бакета стабилизируется (удалённые файлы физически стёрты)."""
    log = load_csv(B + "storage_log.csv")
    try:
        prev = bucket_usage_mb()
    except Exception as e:  # noqa: BLE001
        say(f"не удалось измерить бакет ({e}); пауза 30 с")
        time.sleep(30)
        return
    start, t0, stable = prev, time.time(), 0
    while time.time() - t0 < max_wait_s and stable < 2:
        time.sleep(10)
        cur = bucket_usage_mb()
        stable = stable + 1 if abs(cur - prev) < 1 else 0
        prev = cur
    log.append({"moment": label, "bucket_mb_before": round(start, 1), "bucket_mb_after": round(prev, 1),
                "waited_s": round(time.time() - t0)})
    save_csv(B + "storage_log.csv", log)
    say(f"бакет: {start:,.0f} -> {prev:,.0f} МБ за {time.time() - t0:.0f} с")


# ----------------------------------------------------------------------------- запись
def write_variant(fmt: str, codec: str) -> None:
    sch = schema_of(fmt, codec)
    lake = Lake(compression_codec=codec)
    lake.q(f"CREATE SCHEMA IF NOT EXISTS lake.{sch}")
    rows = []
    for rep in range(1, WRITE_REPEATS + 1):
        for t in TABLES:
            lake.q(f"DROP TABLE IF EXISTS lake.{sch}.{t}")
            r = lake.q(f"CREATE TABLE lake.{sch}.{t} WITH (format = '{fmt}') AS SELECT * FROM tpch.{SCALE}.{t}")
            rows.append({"variant": f"{fmt}+{codec}", "format": fmt, "codec": codec, "repeat": rep,
                         "table": t, "rows": r.rows[0][0], **r.metrics()})
            say(f"[{fmt}+{codec}] запись {t:8s} #{rep}: {r.server_s:6.1f} s, CPU {r.cpu_s:6.1f} s")
        if rep < WRITE_REPEATS:
            wait_for_purge(f"{fmt}+{codec} после записи #{rep}")
    save_csv(B + f"{sch}_writes.csv", rows)

    sizes = []
    for t in TABLES:
        fp = lake.footprint(f"{sch}.{t}")
        path = lake.biggest_file(f"{sch}.{t}")
        fc = file_codec(path) if path else {}
        sizes.append({"variant": f"{fmt}+{codec}", "format": fmt, "codec": codec, "table": t,
                      "rows": fp["records"], "files": fp["files"], "size_bytes": fp["bytes"],
                      "size_mb": fp["mb"], "codec_in_file": fc.get("codec", ""),
                      "container_in_file": fc.get("container", ""), "blocks_in_file": fc.get("blocks", ""),
                      "checked_file": path})
    save_csv(B + f"{sch}_sizes.csv", sizes)
    total = sum(s["size_mb"] for s in sizes)
    say(f"[{fmt}+{codec}] Bronze {total:,.1f} МБ; кодек в файле lineitem: "
        f"{next(s['codec_in_file'] for s in sizes if s['table'] == 'lineitem')}")
    (REPO / "results" / B / f"{sch}.done").write_text(time.strftime("%F %T"))


# ----------------------------------------------------------------------------- генератор
def generator_baseline() -> None:
    lake, rows = Lake(), []
    for t in TABLES:
        cols = [r[0] for r in lake.q(f"SHOW COLUMNS FROM tpch.{SCALE}.{t}").rows]
        for rep in range(1, WRITE_REPEATS + 1):
            r = lake.q(f"SELECT count(*), checksum(ROW({', '.join(cols)})) FROM tpch.{SCALE}.{t}")
            rows.append({"table": t, "repeat": rep, **r.metrics()})
            say(f"генерация tpch.{SCALE}.{t:8s} #{rep}: {r.server_s:6.1f} s")
    save_csv(B + "generator.csv", rows)


# ----------------------------------------------------------------------------- чтение
def read_variants(variants) -> None:
    queries = {"Q18": q18, "TEXT_SCAN": text_scan}
    lake, out = Lake(), []

    def measure(fmt, codec, qname, rnd):
        r = lake.q(queries[qname](f"lake.{schema_of(fmt, codec)}"))
        out.append({"variant": f"{fmt}+{codec}", "format": fmt, "codec": codec, "query": qname,
                    "round": rnd, "warmup": rnd == 0, "result_rows": len(r.rows), **r.metrics()})
        say(f"[{fmt}+{codec}] {qname:9s} {'прогрев' if rnd == 0 else f'раунд {rnd}'}: "
            f"{r.server_s:6.2f} s, прочитано {r.read_mb:8.1f} МБ")

    for fmt, codec in variants:
        for qn in queries:
            measure(fmt, codec, qn, 0)
    rnd_gen = random.Random(2026)
    for rnd in range(1, READ_REPEATS + 1):
        order = variants[:]
        rnd_gen.shuffle(order)
        for fmt, codec in order:
            for qn in queries:
                measure(fmt, codec, qn, rnd)
    for fmt, codec in variants:
        save_csv(B + f"{schema_of(fmt, codec)}_reads.csv",
                 [x for x in out if x["variant"] == f"{fmt}+{codec}"])


# ----------------------------------------------------------------------------- сортировка и слои
def layout_experiment() -> None:
    lake = Lake(compression_codec="ZSTD", parquet_writer_block_size="8MB")
    lake.q("CREATE SCHEMA IF NOT EXISTS lake.bm_layout")
    lake.q("DROP TABLE IF EXISTS lake.bm_layout.order_facts_unsorted")
    # та же SQL-логика, что и для silver.order_facts, но без sorted_by:
    # порядок строк после GROUP BY — случайный относительно total_quantity
    stmt = next(s for s in statements(REPO / "etl" / "sql" / "20_silver.sql")
                if s.startswith("CREATE TABLE lake.silver.order_facts"))
    stmt = stmt.replace("lake.silver.order_facts", "lake.bm_layout.order_facts_unsorted", 1)
    stmt = re.sub(r",\s*sorted_by\s*=\s*ARRAY\['total_quantity'\]", "", stmt)
    say("строю order_facts без сортировки")
    lake.q(stmt)
    try:
        rg_rows = []
        for path in data_files(lake, "bm_layout.order_facts_unsorted"):
            rg_rows += [{"file": path.rsplit("/", 1)[-1], "row_group": i, "rows": n, "min_total_quantity": lo,
                         "max_total_quantity": hi} for i, n, lo, hi in parquet_row_groups(path, "total_quantity")]
        save_csv(B + "unsorted_rowgroups.csv", rg_rows)
    except Exception as e:  # noqa: BLE001
        say(f"row group не прочитаны: {e}")

    cases = [
        ("Bronze: исходный Q18 (3 таблицы + подзапрос)", q18("lake.bronze"), "bronze.lineitem"),
        ("Silver order_facts без сортировки", q18_from_silver("lake.bm_layout.order_facts_unsorted"),
         "bm_layout.order_facts_unsorted"),
        ("Silver order_facts, sorted_by total_quantity", q18_from_silver("lake.silver.order_facts"),
         "silver.order_facts"),
        ("Gold: готовая витрина q18_top100", "SELECT * FROM lake.gold.q18_top100 ORDER BY top_rank",
         "gold.q18_top100"),
    ]
    reader, out = Lake(), []
    meta = {name: reader.footprint(tbl) for name, _, tbl in cases}
    order = list(range(len(cases)))
    rng = random.Random(7)
    for rnd in range(0, READ_REPEATS + 1):
        if rnd:
            rng.shuffle(order)
        for i in order:
            name, sql, tbl = cases[i]
            r = reader.q(sql)
            out.append({"case": name, "table": tbl, "table_files": meta[name]["files"],
                        "table_mb": meta[name]["mb"], "round": rnd, "warmup": rnd == 0,
                        "result_rows": len(r.rows), **r.metrics()})
            say(f"{name:46s} {'прогрев' if rnd == 0 else f'раунд {rnd}'}: {r.server_s:6.2f} s, "
                f"прочитано {r.read_mb:7.1f} МБ, строк {r.rows_processed:,}")
    save_csv(B + "layout.csv", out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variants", default="")
    ap.add_argument("--writes-only", action="store_true")
    ap.add_argument("--reads-only", action="store_true")
    ap.add_argument("--layout-only", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    a = ap.parse_args()

    variants = [(f, c) for f in FORMATS for c in CODECS]
    if a.variants:
        variants = [tuple(v.strip().upper().split(":")) for v in a.variants.split(",")]
    say(f"масштаб {SCALE}; вариантов {len(variants)}; записей {WRITE_REPEATS}; чтений {READ_REPEATS}")
    t0 = time.time()

    if a.layout_only:
        layout_experiment()
        return
    if not a.reads_only:
        for i, (fmt, codec) in enumerate(variants, 1):
            if a.skip_existing and (REPO / "results" / B / f"{schema_of(fmt, codec)}.done").exists():
                say(f"[{i}/{len(variants)}] {fmt}+{codec} уже записан")
                continue
            say(f"[{i}/{len(variants)}] запись {fmt}+{codec}")
            write_variant(fmt, codec)
            wait_for_purge(f"после варианта {fmt}+{codec}")
            say(f"прошло {(time.time() - t0) / 60:.1f} мин")
    if a.writes_only:
        return
    generator_baseline()
    read_variants(variants)
    layout_experiment()
    say(f"бенчмарк завершён за {(time.time() - t0) / 60:.1f} мин")


if __name__ == "__main__":
    main()
