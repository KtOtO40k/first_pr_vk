#!/usr/bin/env bash
# Полный прогон: стек -> ELT -> Iceberg-демо -> бенчмарк -> Superset -> сводка.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
R="docker compose run --rm -T tools"

docker compose up -d
docker compose build tools
docker compose ps -a --format "table {{.Name}}\t{{.Image}}\t{{.Status}}" | tee results/docker_ps.txt

$R python tools/elt.py           2>&1 | tee results/log_elt.txt
$R python tools/iceberg_demo.py  2>&1 | tee results/log_iceberg.txt
./run_bench.sh                    2>&1 | tee results/log_bench.txt

docker compose --profile bi up -d --build superset
$R python tools/superset_setup.py 2>&1 | tee results/log_superset.txt
$R python tools/report.py        2>&1 | tee results/log_report.txt
echo "Готово: results/report.md, Superset — http://localhost:8088 (admin / admin)"
