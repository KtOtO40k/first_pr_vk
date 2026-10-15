#!/usr/bin/env bash
# Бенчмарк по одному варианту за запуск: запись -> ожидание очистки бакета -> (Docker Desktop) fstrim.
# Так диск Docker не раздувается удалёнными файлами, а замеры записи не искажаются.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
T="docker compose run --rm -T tools python tools/bench.py"

trim() {  # возвращает освобождённые блоки виртуального диска Docker Desktop (вне замеров)
  if docker info --format '{{.OperatingSystem}}' 2>/dev/null | grep -qi "docker desktop"; then
    docker run --rm --privileged --pid=host alpine:3.20 nsenter -t 1 -m -- fstrim -av || true
  fi
}

for v in PARQUET:NONE PARQUET:SNAPPY PARQUET:GZIP PARQUET:ZSTD ORC:NONE ORC:SNAPPY ORC:GZIP ORC:ZSTD; do
  echo "=== запись $v"
  $T --writes-only --skip-existing --variants "$v"
  trim
done
echo "=== генератор, чтение всех вариантов, сортировка Silver"
$T --reads-only
