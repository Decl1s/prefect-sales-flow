"""Лабораторная: отдельные CSV и просмотр этапов без запуска сервера.

prepare только создаёт синтетические копии; preview вызывает реальные правила core;
inspect читает уже записанную сводку. Запуск Prefect остаётся штатной командой
python -m sales_flow run, чтобы лабораторная не становилась вторым приложением.
"""

import argparse
import csv
import io
import json
import sys
from pathlib import Path

# При python scripts/workshop.py Python начинает поиск модулей с scripts/.
# Добавляем именно корень данного проекта, а не системные пути или чужие исходники.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sales_flow.core import (aggregate, generate_demo, quality_gate, read_source,
                             reject_symlinks)

CASES = ("baseline", "extra-duplicate", "conflict", "cancelled", "bad-status", "new-product")


def write_case(path: Path, header: list, rows: list) -> None:
    """Одинаковые байты на Linux/Windows и отказ перезаписи существующего файла."""
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def prepare(folder: Path) -> None:
    """Создать шесть вариантов. Повтор сохраняет любые правки ученика."""
    reject_symlinks(folder.absolute())
    if folder.exists():
        print("Лабораторная папка уже существует: файлы оставлены как есть. Для нового набора выбери --folder.")
        return
    source = folder / "input"
    source.mkdir(parents=True)
    generate_demo(source / "baseline.csv")
    # Новые строки — копии списков. Иначе смена статуса в одном случае изменила бы другой.
    original = list(csv.reader(io.StringIO((source / "baseline.csv").read_text(encoding="utf-8"))))
    header, rows = original[0], original[1:]
    write_case(source / "extra-duplicate.csv", header, rows + [rows[1]])
    conflict = rows[0].copy()
    conflict[3] = "3"
    write_case(source / "conflict.csv", header, rows + [conflict])
    cancelled = [row.copy() for row in rows]
    for row in cancelled:
        if row[0] == "O0005":
            row[5] = "cancelled"  # Меняем обе копии ID, чтобы не получить конфликт.
    write_case(source / "cancelled.csv", header, cancelled)
    bad = [row.copy() for row in rows]
    bad[0][5] = "unknown"
    write_case(source / "bad-status.csv", header, bad)
    write_case(source / "new-product.csv", header,
               rows + [["O0007", "2026-10-02", "mug", "2", "12500", "paid"]])
    print("Создано 6 синтетических CSV в " + str(source))
    print("Начни с preview --case baseline. Задания и ожидаемые ответы: EXERCISES.md.")


def preview(source: Path) -> dict:
    """Те же преобразования, что в flow, но удобно останавливаться отладчиком.

Ничего не публикуем на диск. Эта функция не доказывает правильность настроек
Prefect: для этого существует отдельный scripts/smoke.py.
"""
    raw = read_source(source)       # F10: появятся text и два хэша.
    clean = quality_gate(raw)       # F11: зайди в цикл проверки конкретной строки.
    summary = aggregate(clean)     # F11: посмотри, как меняется groups по дням.
    return {"stage_1_read": {"source_sha256": raw["source_sha256"], "run_key": raw["run_key"]},
            "stage_2_quality": {"input_rows": clean["input_rows"], "duplicates": clean["duplicates"],
                                "unique_orders": len(clean["orders"])},
            "stage_3_summary": summary}


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Лабораторная для изучения конвейера")
    parser.add_argument("command", choices=("prepare", "preview", "inspect"))
    parser.add_argument("--folder", type=Path, default=Path("workspace/learn"))
    parser.add_argument("--case", choices=CASES, default="baseline")
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            prepare(args.folder)
        elif args.command == "preview":
            # Выбираем только одно из известных имён: --case не превращается в произвольный путь.
            result = preview(args.folder / "input" / (args.case + ".csv"))
            print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        else:
            # Вычисляем тот же ключ, а не угадываем «последнюю» папку по времени изменения.
            key = read_source(args.folder / "input" / (args.case + ".csv"))["run_key"]
            result = args.folder / "output" / key
            if not result.is_dir():
                raise ValueError("Для этого входа ещё нет результата. Сначала выполни штатный run.")
            print("Результат: " + str(result))
            print((result / "summary.json").read_text(encoding="utf-8"))
            print((result / "report.md").read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError) as exc:
        print(type(exc).__name__ + ": " + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
