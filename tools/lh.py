"""lh — небольшая библиотека для работы со стендом: Trino-сессии с замером,
метаданные Iceberg-таблиц, чтение футеров файлов в MinIO, запись результатов."""
from __future__ import annotations

import csv
import json
import os
import re
import statistics
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import trino

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "results"
OUT.mkdir(exist_ok=True)

SCALE = os.getenv("SCALE", "sf4")
WRITE_REPEATS = int(os.getenv("WRITE_REPEATS", "3"))
READ_REPEATS = int(os.getenv("READ_REPEATS", "5"))
CATALOG = "lake"
Q18_THRESHOLD = 300


def say(text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} | {text}", flush=True)


# ----------------------------------------------------------------------------- Trino
@dataclass
class Run:
    """Результат одного запроса: строки + клиентское время + серверная статистика."""
    rows: list
    columns: list[str]
    client_s: float
    stats: dict = field(default_factory=dict)
    query_id: str | None = None

    def _ms(self, key: str) -> float:
        return float(self.stats.get(key) or 0) / 1000

    @property
    def server_s(self) -> float:
        v = self.stats.get("elapsedTimeMillis")
        return float(v) / 1000 if v is not None else self.client_s

    @property
    def cpu_s(self) -> float:
        return self._ms("cpuTimeMillis")

    @property
    def read_mb(self) -> float:
        return float(self.stats.get("physicalInputBytes") or 0) / 1e6

    @property
    def rows_processed(self) -> int:
        return int(self.stats.get("processedRows") or 0)

    @property
    def peak_mem_mb(self) -> float:
        return float(self.stats.get("peakMemoryBytes") or 0) / 1e6

    def metrics(self) -> dict:
        return {"server_s": round(self.server_s, 3), "client_s": round(self.client_s, 3),
                "cpu_s": round(self.cpu_s, 3), "read_mb": round(self.read_mb, 3),
                "rows_processed": self.rows_processed, "peak_mem_mb": round(self.peak_mem_mb, 1)}


class Lake:
    """Сессия Trino в каталоге lake. session — сессионные свойства (имя без префикса каталога)."""

    _supported: set[str] | None = None

    def __init__(self, **session):
        url = urlparse(os.getenv("TRINO_URL", "http://localhost:8080"))
        self.session = session
        props = {f"{CATALOG}.{k}": str(v) for k, v in session.items() if self._is_supported(k)}
        self.conn = trino.dbapi.connect(host=url.hostname, port=url.port or 8080, http_scheme=url.scheme,
                                        user="lab", catalog=CATALOG, schema="bronze",
                                        session_properties=props, request_timeout=3600)

    @classmethod
    def _is_supported(cls, name: str) -> bool:
        if cls._supported is None:
            url = urlparse(os.getenv("TRINO_URL", "http://localhost:8080"))
            c = trino.dbapi.connect(host=url.hostname, port=url.port or 8080, http_scheme=url.scheme,
                                    user="lab", catalog=CATALOG)
            cur = c.cursor()
            cur.execute(f"SHOW SESSION LIKE '{CATALOG}.%'")
            cls._supported = {r[0].split(".", 1)[1] for r in cur.fetchall() if "." in str(r[0])}
            say(f"сессионных свойств каталога {CATALOG}: {len(cls._supported)}")
        ok = name in cls._supported
        if not ok:
            say(f"ВНИМАНИЕ: сессионного свойства {CATALOG}.{name} нет в этой версии Trino — пропускаю")
        return ok

    def q(self, sql: str) -> Run:
        cur = self.conn.cursor()
        t = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        dt = time.perf_counter() - t
        cols = [d[0] for d in (cur.description or [])]
        return Run(rows, cols, dt, dict(cur.stats or {}), cur.query_id)

    def one(self, sql: str):
        return self.q(sql).rows[0][0]

    def dicts(self, sql: str) -> list[dict]:
        r = self.q(sql)
        return [dict(zip(r.columns, row)) for row in r.rows]

    # --------------------------------------------------- метаданные Iceberg
    def footprint(self, table: str) -> dict:
        """Размер таблицы по метаданным Iceberg ($files): только файлы данных текущего снапшота."""
        schema, name = table.split(".")
        n, size, recs = self.q(
            f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0), coalesce(sum(record_count), 0) '
            f'FROM {CATALOG}.{schema}."{name}$files" WHERE content = 0').rows[0]
        return {"files": int(n), "bytes": int(size), "mb": round(int(size) / 1e6, 3), "records": int(recs)}

    def ddl(self, table: str) -> str:
        return self.one(f"SHOW CREATE TABLE {CATALOG}.{table}")

    def layout(self, table: str) -> dict:
        ddl = self.ddl(table)
        part = re.search(r"partitioning\s*=\s*ARRAY\[(.*?)\]", ddl)
        sort = re.search(r"sorted_by\s*=\s*ARRAY\[(.*?)\]", ddl)
        fmt = re.search(r"format\s*=\s*'(\w+)'", ddl)
        return {"format": fmt.group(1) if fmt else "?",
                "partitioning": part.group(1).replace("'", "") if part else "нет",
                "sorted_by": sort.group(1).replace("'", "") if sort else "нет"}

    def biggest_file(self, table: str) -> str | None:
        schema, name = table.split(".")
        r = self.q(f'SELECT file_path FROM {CATALOG}.{schema}."{name}$files" '
                   f'WHERE content = 0 ORDER BY file_size_in_bytes DESC LIMIT 1').rows
        return r[0][0] if r else None


# ----------------------------------------------------------------------------- SQL-файлы
def statements(path: Path, **params) -> list[str]:
    text = path.read_text(encoding="utf-8")
    for k, v in params.items():
        text = text.replace("{{" + k + "}}", str(v))
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", text) if s.strip()]


def new_batch_id() -> str:
    return time.strftime("%Y%m%dT%H%M%S-") + uuid.uuid4().hex[:6]


# ----------------------------------------------------------------------------- файлы в MinIO
def s3fs():
    from pyarrow import fs
    return fs.S3FileSystem(access_key=os.getenv("S3_USER", "lakeadmin"),
                           secret_key=os.getenv("S3_PASSWORD", "lakeadmin-secret"),
                           endpoint_override=os.getenv("S3_ENDPOINT", "localhost:9000"),
                           scheme="http", region="us-east-1")


def file_codec(path: str) -> dict:
    """Читает кодек прямо из футера файла данных — независимая проверка настроек записи."""
    try:
        import pyarrow.parquet as pq
        key = re.sub(r"^s3a?://", "", path)
        fs_ = s3fs()
        if key.lower().endswith(".parquet"):
            with fs_.open_input_file(key) as f:
                md = pq.ParquetFile(f).metadata
                codecs = {md.row_group(i).column(j).compression
                          for i in range(md.num_row_groups) for j in range(md.num_columns)}
            return {"container": "PARQUET", "codec": "/".join(sorted(codecs)), "blocks": md.num_row_groups}
        if key.lower().endswith(".orc"):
            import pyarrow.orc as orc
            with fs_.open_input_file(key) as f:
                of = orc.ORCFile(f)
                return {"container": "ORC", "codec": str(of.compression), "blocks": of.nstripes}
        return {"container": "?", "codec": "неизвестное расширение"}
    except Exception as e:  # noqa: BLE001
        return {"container": "?", "codec": f"ошибка: {str(e)[:120]}"}


def parquet_row_groups(path: str, column: str) -> list[tuple]:
    """min/max колонки по row group — показывает, что дала сортировка (sorted_by).

    Источник — статистика футера. Для DECIMAL-колонок pyarrow её не отдаёт: Trino пишет только новые
    поля min_value/max_value (без устаревших min/max и без column_orders), а pyarrow в таком файле
    читает только устаревшие. Trino эту статистику видит и пропускает по ней row group. В этом случае
    диапазон считается чтением самой колонки — это те же значения, что лежат в футере."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    key = re.sub(r"^s3a?://", "", path)
    with s3fs().open_input_file(key) as f:
        pf = pq.ParquetFile(f)
        md = pf.metadata
        idx = [md.schema.column(i).name for i in range(md.num_columns)].index(column)
        out = []
        for i in range(md.num_row_groups):
            st = md.row_group(i).column(idx).statistics
            if st and st.has_min_max:
                lo, hi = str(st.min), str(st.max)
            else:
                mm = pc.min_max(pf.read_row_group(i, columns=[column]).column(0)).as_py()
                lo, hi = str(mm["min"]), str(mm["max"])
            out.append((i, md.row_group(i).num_rows, lo, hi))
        return out


def data_files(lake: "Lake", table: str) -> list[str]:
    """Пути всех файлов данных таблицы (от большего к меньшему)."""
    schema, name = table.split(".")
    return [r[0] for r in lake.q(f'SELECT file_path FROM {CATALOG}.{schema}."{name}$files" '
                                 f'WHERE content = 0 ORDER BY file_size_in_bytes DESC').rows]


def bucket_usage_mb(bucket: str = "lakehouse") -> float:
    from pyarrow import fs
    infos = s3fs().get_file_info(fs.FileSelector(bucket, recursive=True))
    return sum(i.size or 0 for i in infos if i.type == fs.FileType.File) / 1e6


# ----------------------------------------------------------------------------- вывод
def save_csv(name: str, rows: list[dict]) -> Path:
    p = OUT / name
    p.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        cols: list[str] = []
        for r in rows:
            cols += [k for k in r if k not in cols]
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        say(f"→ {p.relative_to(REPO)} ({len(rows)} строк)")
    return p


def load_csv(name: str) -> list[dict]:
    p = OUT / name
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_json(name: str, obj) -> None:
    (OUT / name).write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    say(f"→ results/{name}")


def med(values) -> float:
    v = [float(x) for x in values]
    return statistics.median(v) if v else float("nan")


# ----------------------------------------------------------------------------- запросы
def q18(src: str) -> str:
    """Q18 ровно как в варианты.md; src — префикс схемы (tpch.sf4, lake.bronze, lake.bm_orc_zstd ...)."""
    return f"""
SELECT
  c.name,
  c.custkey,
  o.orderkey,
  o.orderdate,
  o.totalprice,
  sum(l.quantity) AS total_quantity
FROM {src}.customer AS c
JOIN {src}.orders   AS o ON c.custkey = o.custkey
JOIN {src}.lineitem AS l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey
  FROM {src}.lineitem
  GROUP BY orderkey
  HAVING sum(quantity) > {Q18_THRESHOLD}
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100"""


def text_scan(src: str) -> str:
    """Агрегация с поиском по тексту: читает самую тяжёлую колонку lineitem.comment —
    максимально чувствительна к степени и скорости распаковки кодека."""
    return f"""
SELECT shipmode, count(*) AS lines, sum(quantity) AS qty
FROM {src}.lineitem
WHERE comment LIKE '%carefully%'
GROUP BY shipmode
ORDER BY shipmode"""


def q18_from_silver(facts_table: str) -> str:
    return f"""
SELECT c.customer_name, f.custkey, f.orderkey, f.orderdate, f.totalprice, f.total_quantity
FROM {facts_table} f
JOIN lake.silver.customer_dim c ON c.custkey = f.custkey
WHERE f.total_quantity > {Q18_THRESHOLD}
ORDER BY f.totalprice DESC, f.orderdate
LIMIT 100"""
