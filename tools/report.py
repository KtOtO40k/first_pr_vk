"""Сводит results/*.csv в results/report.md и рисует графики results/charts/*.png.

    docker compose run --rm -T tools python tools/report.py
"""
from __future__ import annotations

import glob
import math
from collections import defaultdict
from pathlib import Path

from lh import OUT, Q18_THRESHOLD, load_csv, med, say

FORMATS = ["PARQUET", "ORC"]
CODECS = ["NONE", "SNAPPY", "GZIP", "ZSTD"]
CH = OUT / "charts"
CH.mkdir(exist_ok=True)
# категориальные слоты 1–2 проверенной палитры; текст — нейтральными цветами
SERIES = {"PARQUET": "#2a78d6", "ORC": "#eb6834"}
BG, INK, INK2, RULE = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"


def n(x, d=2):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{x:,.{d}f}".replace(",", " ").replace(".", ",")


def table(head, rows):
    al = ["---"] + ["---:"] * (len(head) - 1)
    return "\n".join(["| " + " | ".join(head) + " |", "|" + "|".join(al) + "|"] +
                     ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])


def collect(pattern):
    out = []
    for p in sorted(glob.glob(str(OUT / "bench" / pattern))):
        out += load_csv("bench/" + Path(p).name)
    return out


def is_true(v):
    return str(v).lower() in ("true", "1")


# ------------------------------------------------------------------ слои
def layers_md():
    rows = load_csv("layers.csv")
    if not rows:
        return "_нет results/layers.csv — сначала tools/elt.py_\n"
    parts = ["## Слои\n"]
    for layer in ["bronze", "silver", "gold"]:
        parts.append(f"### {layer}\n")
        parts.append(table(["Таблица", "Строк", "Колонок", "Файлов", "МБ", "Формат", "Кодек в файле",
                            "Row group (макс. файл)", "Партиции", "Сортировка"],
                           [[f"`{r['table']}`", n(int(r["rows"]), 0), r["columns"], r["files"],
                             n(float(r["size_mb"]), 3), r["format"], r["codec_in_file"],
                             r["row_groups_biggest_file"], r["partitioning"], r["sorted_by"]]
                            for r in rows if r["layer"] == layer]) + "\n")
    st = load_csv("elt_steps.csv")
    if st:
        parts.append("### Построение слоёв\n")
        parts.append(table(["Слой", "Таблица", "Строк", "Время, с", "CPU, с", "Пик памяти, МБ"],
                           [[r["layer"], f"`{r['table']}`", n(int(r["rows_written"]), 0),
                             n(float(r["server_s"])), n(float(r["cpu_s"]), 1), n(float(r["peak_mem_mb"]), 0)]
                            for r in st]) + "\n")
    dq = load_csv("dq_checks.csv")
    if dq:
        parts.append("### Качество данных\n")
        parts.append(table(["Проверка", "Значение"], [[d["check"], d["value"]] for d in dq]) + "\n")
    return "\n".join(parts)


# ------------------------------------------------------------------ бенчмарк
def summarize():
    sizes, writes, reads = collect("bm_*_sizes.csv"), collect("bm_*_writes.csv"), collect("bm_*_reads.csv")
    gen = load_csv("bench/generator.csv")
    gen_by = defaultdict(float)
    for g in gen:
        gen_by[g["repeat"]] += float(g["server_s"])
    gen_total = med(gen_by.values()) if gen_by else float("nan")
    out = {}
    for f in FORMATS:
        for c in CODECS:
            v = f"{f}+{c}"
            sz = [r for r in sizes if r["variant"] == v]
            if not sz:
                continue
            per_rep = defaultdict(float)
            for r in writes:
                if r["variant"] == v:
                    per_rep[r["repeat"]] += float(r["server_s"])
            wr = list(per_rep.values())

            def rd(q, key="server_s", warm=False):
                return [float(r[key]) for r in reads
                        if r["variant"] == v and r["query"] == q and is_true(r["warmup"]) == warm]
            li = next((r for r in sz if r["table"] == "lineitem"), {})
            d = {"format": f, "codec": c, "mb": sum(float(r["size_mb"]) for r in sz),
                 "li_mb": float(li.get("size_mb", "nan")), "codec_file": li.get("codec_in_file", ""),
                 "all_codecs_ok": all(r["codec_in_file"] for r in sz),
                 "w": med(wr), "w_lo": min(wr) if wr else float("nan"), "w_hi": max(wr) if wr else float("nan"),
                 "w_net": med(wr) - gen_total, "reps": len(wr)}
            for q in ["Q18", "TEXT_SCAN"]:
                xs = rd(q)
                d[q] = med(xs)
                d[q + "_lo"] = min(xs) if xs else float("nan")
                d[q + "_hi"] = max(xs) if xs else float("nan")
                d[q + "_mb"] = med(rd(q, "read_mb"))
                d[q + "_cpu"] = med(rd(q, "cpu_s"))
                cold = rd(q, warm=True)
                d[q + "_cold"] = cold[0] if cold else float("nan")
                d["runs"] = len(xs)
            out[v] = d
    for d in out.values():
        base = out.get(f"{d['format']}+NONE")
        d["ratio"] = base["mb"] / d["mb"] if base else float("nan")
    return out, gen_total


def bench_md(S, gen_total):
    if not S:
        return "_нет результатов бенчмарка — запусти run_bench.sh_\n"
    any_ = next(iter(S.values()))
    best = {k: min(S.values(), key=lambda d: d[k]) for k in ["mb", "w", "Q18", "TEXT_SCAN"]}

    def b(d, k, s):
        return f"**{s}**" if d is best[k] else s

    rows = [[f"{d['format']} + {d['codec']}", d["codec_file"], b(d, "mb", n(d["mb"], 1)), n(d["li_mb"], 1),
             n(d["ratio"], 2) + "×", b(d, "w", f"{n(d['w'], 1)} ({n(d['w_lo'], 1)}–{n(d['w_hi'], 1)})"),
             n(d["w_net"], 1), b(d, "Q18", f"{n(d['Q18'])} ({n(d['Q18_lo'])}–{n(d['Q18_hi'])})"),
             n(d["Q18_mb"], 1), b(d, "TEXT_SCAN", f"{n(d['TEXT_SCAN'])} ({n(d['TEXT_SCAN_lo'])}–{n(d['TEXT_SCAN_hi'])})"),
             n(d["TEXT_SCAN_mb"], 1)] for d in S.values()]
    md = ["## Бенчмарк Bronze (customer + orders + lineitem)\n",
          f"Записей на вариант: {any_['reps']}, замеров чтения: {any_['runs']} после прогрева. "
          "Медиана, в скобках min–max. Жирным — лучшая медиана.\n",
          table(["Вариант", "Кодек в файле", "Bronze, МБ", "lineitem, МБ", "Сжатие vs NONE", "Запись, с",
                 "Запись − генерация, с", "Q18, с", "Q18 прочитано, МБ", "TEXT_SCAN, с", "TEXT_SCAN прочитано, МБ"],
                rows), "",
          f"Генерация тех же таблиц коннектором tpch без записи: **{n(gen_total, 1)} с** (медиана).\n",
          "### Холодный (первый) запуск и CPU\n",
          table(["Вариант", "Q18 холодный, с", "Q18 CPU, с", "TEXT_SCAN холодный, с", "TEXT_SCAN CPU, с"],
                [[f"{d['format']} + {d['codec']}", n(d["Q18_cold"]), n(d["Q18_cpu"], 1),
                  n(d["TEXT_SCAN_cold"]), n(d["TEXT_SCAN_cpu"], 1)] for d in S.values()]), ""]
    t = [[c, f"{n(S[f'PARQUET+{c}']['Q18'])} с", n(S[f"PARQUET+{c}"]["mb"], 1), ""]
         for c in ["GZIP", "ZSTD"] if f"PARQUET+{c}" in S]
    if t:
        md += ["### Таблица для шаблона (Parquet)\n",
               table(["Сжатие на Bronze", "Время запроса", "Размер Bronze, МБ", "Вывод"], t), ""]
    return "\n".join(md)


def layout_md():
    rows = load_csv("bench/layout.csv")
    if not rows:
        return ""
    cases = list(dict.fromkeys(r["case"] for r in rows))
    tab = []
    for c in cases:
        rr = [r for r in rows if r["case"] == c and not is_true(r["warmup"])]
        xs = [float(r["server_s"]) for r in rr]
        tab.append([c, f"`{rr[0]['table']}`", rr[0]["table_files"], n(float(rr[0]["table_mb"]), 2),
                    f"{n(med(xs))} ({n(min(xs))}–{n(max(xs))})", n(med([r['read_mb'] for r in rr]), 2),
                    n(med([r['rows_processed'] for r in rr]), 0)])
    md = ["## Сортировка Silver и слои медальона (Q18)\n",
          table(["Источник", "Таблица", "Файлов", "МБ", "Время, с", "Прочитано, МБ", "Строк обработано"], tab), ""]
    for name, title in [("order_facts_rowgroups.csv", "silver.order_facts (sorted_by)"),
                        ("bench/unsorted_rowgroups.csv", "order_facts без сортировки")]:
        rg = load_csv(name)
        if rg:
            hit = sum(1 for r in rg if r["max_total_quantity"] not in ("", "None")
                      and float(r["max_total_quantity"]) > Q18_THRESHOLD)
            files = len({r.get("file", "") for r in rg})
            hit_rows = sum(int(r["rows"]) for r in rg if r["max_total_quantity"] not in ("", "None")
                           and float(r["max_total_quantity"]) > Q18_THRESHOLD)
            md.append(f"- {title}: в {files} файлах таблицы {len(rg)} row group ({n(sum(int(r['rows']) for r in rg), 0)} "
                      f"строк), из них max(total_quantity) > {Q18_THRESHOLD} у **{hit}** ({n(hit_rows, 0)} строк) — "
                      f"только их Trino должен читать.")
    st = load_csv("bench/storage_log.csv")
    if st:
        md += ["", "### Очистка хранилища между записями\n",
               table(["Момент", "Бакет до, МБ", "Бакет после, МБ", "Ожидание, с"],
                     [[r["moment"], r["bucket_mb_before"], r["bucket_mb_after"], r["waited_s"]] for r in st])]
    return "\n".join(md) + "\n"


# ------------------------------------------------------------------ графики
def charts(S):
    if not S:
        return []
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "text.color": INK,
                         "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
                         "axes.edgecolor": RULE})

    def panel(ax, key, title, digits, whisk=None):
        ax.set_facecolor(BG)
        w = 0.36
        for i, f in enumerate(FORMATS):
            xs, ys, lo, hi = [], [], [], []
            for j, c in enumerate(CODECS):
                d = S.get(f"{f}+{c}")
                if not d or math.isnan(d[key]):
                    continue
                xs.append(j + (i - 0.5) * (w + 0.03))
                ys.append(d[key])
                if whisk:
                    lo.append(d[key] - d[whisk[0]])
                    hi.append(d[whisk[1]] - d[key])
            bars = ax.bar(xs, ys, width=w, color=SERIES[f], label=f, zorder=3)
            if whisk:
                ax.errorbar(xs, ys, yerr=[lo, hi], fmt="none", ecolor=INK2, elinewidth=1, capsize=3, zorder=4)
            tops = [y + h for y, h in zip(ys, hi)] if whisk else ys
            for bar, y, top in zip(bars, ys, tops):
                ax.annotate(n(y, digits), (bar.get_x() + bar.get_width() / 2, top), xytext=(0, 3),
                            textcoords="offset points", ha="center", va="bottom", fontsize=8, color=INK2)
        ax.set_xticks(range(len(CODECS)), CODECS)
        ax.set_title(title, loc="left", fontsize=11, pad=8)
        ax.grid(axis="y", color=RULE, linewidth=0.8, zorder=0)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(axis="y", length=0)
        ax.margins(y=0.12)

    files = []
    for fname, title, panels in [
        ("bench_storage.png", "Bronze: размер и запись",
         [("mb", "Размер, МБ", 0, None), ("w", "Запись, с (медиана, min–max)", 1, ("w_lo", "w_hi"))]),
        ("bench_reads.png", "Bronze: чтение",
         [("Q18", "Q18, с (медиана, min–max)", 2, ("Q18_lo", "Q18_hi")),
          ("TEXT_SCAN", "Поиск по comment, с (медиана, min–max)", 2, ("TEXT_SCAN_lo", "TEXT_SCAN_hi"))]),
    ]:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.4), facecolor=BG)
        for ax, (k, t, dg, wh) in zip(axes, panels):
            panel(ax, k, t, dg, wh)
        h, lab = axes[0].get_legend_handles_labels()
        fig.legend(h, lab, loc="upper right", ncol=2, frameon=False)
        fig.suptitle(title, x=0.01, ha="left", fontsize=13)
        fig.tight_layout(rect=(0, 0, 1, 0.94))
        p = CH / fname
        fig.savefig(p, dpi=150, facecolor=BG)
        plt.close(fig)
        files.append(p)
        say(f"график {p.relative_to(OUT.parent)}")
    return files


def main():
    S, gen_total = summarize()
    parts = ["# Сводка результатов (сгенерировано tools/report.py)\n"]
    env = OUT / "environment.md"
    if env.exists():
        parts += ["## Окружение\n", env.read_text(encoding="utf-8")]
    parts += [layers_md(), bench_md(S, gen_total), layout_md()]
    if charts(S):
        parts += ["![Размер и запись](charts/bench_storage.png)\n", "![Чтение](charts/bench_reads.png)\n"]
    for extra in ["validation.md", "iceberg_demo.md"]:
        p = OUT / extra
        if p.exists():
            parts.append(p.read_text(encoding="utf-8"))
    (OUT / "report.md").write_text("\n".join(parts), encoding="utf-8")
    say("results/report.md готов")


if __name__ == "__main__":
    main()
