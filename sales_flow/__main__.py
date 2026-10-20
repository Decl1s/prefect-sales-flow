"""Точка входа python -m sales_flow: разобрать аргументы и связать готовые части.

Здесь нет формулы дохода. Новое правило заказа ищи в core.py; новый retry —
в orchestration.py. Такое разделение уменьшает число мест, которые надо менять.
"""

import argparse
import json
import sys
from pathlib import Path

from .core import generate_demo
from .local import configure_local, local_settings


def main() -> int:
    """Код 0 — успех, 1 — ошибка работы, 2 — ошибка аргументов argparse."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    # argparse встроен в Python: для двух команд сторонний CLI-фреймворк не нужен.
    parser = argparse.ArgumentParser(description="Учебный локальный конвейер продаж на Prefect")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Создать синтетический CSV без перезаписи")
    demo.add_argument("--output", default="workspace/input/orders.csv")
    run = commands.add_parser("run", help="Прочитать, проверить и опубликовать продажи")
    run.add_argument("--input", default="workspace/input/orders.csv")
    run.add_argument("--output", default="workspace/output")
    # Разделяем результаты пользователя и техническую базу оркестратора.
    run.add_argument("--prefect-home", default="workspace/prefect")
    run.add_argument("--fail-once", action="store_true", help="Временно отказать в первой записи")
    args = parser.parse_args()  # --help завершится здесь, не импортируя тяжёлый Prefect.
    try:
        if args.command == "demo":
            generate_demo(args.output)
            print("Создано 8 строк: 6 заказов и 2 точных дубля.")
        else:
            source = Path(args.input).absolute()
            output = Path(args.output).absolute()
            if source == output or output in source.parents:
                # Входная папка не должна выглядеть как часть готовых результатов.
                raise ValueError("Вход должен находиться вне каталога результатов.")
            configure_local(args.prefect_home)
            with local_settings():
                # Поздний импорт принципиален: сначала локальные настройки, потом Prefect.
                from .orchestration import sales_pipeline
                result = sales_pipeline(str(source), str(output), args.fail_once)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    except Exception as exc:
        # Исходные строки и настройки в пользовательское сообщение не попадают.
        print("Конвейер завершился ошибкой: " + type(exc).__name__ + ". Проверьте вход и доступность рабочих папок.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
