"""Предметные правила: что считать корректным заказом и как получить отчёт.

Начни изучение с read_source → quality_gate → aggregate → publish.
quality_gate и aggregate преобразуют данные в памяти; read_source и publish
работают с файлами. Prefect здесь не импортируется: эти же правила можно вызвать
из обычного скрипта, теста или другого оркестратора. Решения разобраны в DECISIONS.md.
"""

import csv
import hashlib
import io
import json
import re
import shutil
import tempfile
from datetime import date
from pathlib import Path

# Версия СМЫСЛА обработки, а не версия приложения в pyproject.toml.
# Изменил формулу выручки или правила включения заказов — подними sales-v1 до v2.
# Тогда одинаковый CSV не будет ошибочно связан со старой по смыслу сводкой.
VERSION = "sales-v1"
# Порядок строгий: перестановка столбцов требует явного изменения контракта.
HEADER = ["order_id", "order_date", "product", "quantity", "unit_price_cents", "status"]
# Малый предел позволяет держать пакет в памяти; это не обработчик огромного потока.
MAX_BYTES = 1_000_000
MAX_ROWS = 10_000
PRODUCTS = {"book", "pen", "bag"}


class DataError(ValueError):
    """Данные нарушают договорённость; повтор той же операции не поможет."""


class PublishError(ValueError):
    """Существующий результат повреждён или путь непригоден."""


class TransientPublishError(OSError):
    """Имитируем временную недоступность приёмника до изменения файлов."""


def reject_symlinks(path: Path) -> None:
    """Не следовать ссылке, которая незаметно ведёт в другую рабочую папку.

Проверяем также родителей: ссылка может называться input/, а не orders.csv.
Это упрощение локальной модели, не защита от гонки с чужим процессом.
"""
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise PublishError("Символические ссылки в рабочих путях не поддерживаются.")


def read_source(source: str | Path) -> dict:
    """Прочитать ограниченный CSV и вернуть text, source_sha256 и run_key.

source — путь к существующему файлу. Здесь ещё не проверяются поля заказов.
Ошибка чтения или слишком большой файл останавливают весь пакет.
"""
    source = Path(source).absolute()
    reject_symlinks(source)
    if not source.is_file():
        raise DataError("Входной CSV не найден или не является обычным файлом.")
    with source.open("rb") as stream:
        # Дополнительный байт нужен, чтобы отличить ровно MAX_BYTES от превышения.
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise DataError("CSV превышает предел 1 000 000 байт.")
    try:
        text = raw.decode("utf-8-sig")  # Убирает необязательный BOM, но не меняет raw.
    except UnicodeError as exc:
        raise DataError("CSV должен быть в UTF-8.") from exc
    digest = hashlib.sha256(raw).hexdigest()
    # digest описывает только вход; run_key дополнительно учитывает смысл обработки.
    # Нулевой разделитель отделяет версию от данных, а полный хэш уменьшает риск коллизии.
    run_key = hashlib.sha256(VERSION.encode() + b"\0" + raw).hexdigest()
    return {"text": text, "source_sha256": digest, "run_key": run_key}


def quality_gate(source: dict) -> dict:
    """Проверить ВЕСЬ пакет, убрать точные дубли и вернуть типизированные заказы.

source — результат read_source. В orders quantity и unit_price_cents уже int,
остальные поля — str. input_rows считает строки до дедупликации, duplicates —
удалённые копии. При одной плохой строке никакого частичного набора не возвращаем.
"""
    try:
        rows = csv.reader(io.StringIO(source["text"], newline=""), strict=True)
        if next(rows, None) != HEADER:
            raise DataError("Заголовок CSV не совпадает с контрактом.")
        unique = {}  # Ключ — order_id. Это дедупликация внутри одного CSV, не между файлами.
        duplicates = 0
        count = 0
        for count, row in enumerate(rows, start=1):
            if count > MAX_ROWS:
                raise DataError("CSV превышает предел 10 000 строк данных.")
            if len(row) != len(HEADER):
                raise DataError(f"Строка данных {count}: неверное число полей.")
            key, day, product, quantity, price, status = row
            # fullmatch проверяет строку целиком: префикса, похожего на ID, недостаточно.
            if not re.fullmatch(r"O[0-9]{4}", key):
                raise DataError(f"Строка данных {count}: неверный order_id.")
            if not re.fullmatch(r"20[0-9]{2}-[0-9]{2}-[0-9]{2}", day):
                raise DataError(f"Строка данных {count}: неверный формат даты.")
            try:
                # Регулярное выражение не знает, сколько дней бывает в феврале.
                date.fromisoformat(day)
            except ValueError as exc:
                raise DataError(f"Строка данных {count}: несуществующая дата.") from exc
            if product not in PRODUCTS or status not in {"paid", "cancelled"}:
                raise DataError(f"Строка данных {count}: неизвестный product или status.")
            if not re.fullmatch(r"[1-9][0-9]{0,3}", quantity):
                raise DataError(f"Строка данных {count}: quantity должен быть от 1 до 9999.")
            if not re.fullmatch(r"[1-9][0-9]{0,6}", price):
                raise DataError(f"Строка данных {count}: unit_price_cents должен быть от 1 до 9999999.")
            # До int() проверили каноническую запись: 02, +2, 2.0 и пробелы запрещены.
            record = {**dict(zip(HEADER, row)), "quantity": int(quantity), "unit_price_cents": int(price)}
            if key in unique:
                # Сравниваем все поля. Статус paid/cancelled с одним ID — тоже конфликт.
                # Без версии события нельзя обоснованно выбирать «последнюю» строку.
                if unique[key] != record:
                    raise DataError(f"Строка данных {count}: конфликт order_id.")
                duplicates += 1
            else:
                unique[key] = record
        if not count:
            raise DataError("CSV не содержит заказов.")
    except csv.Error as exc:
        raise DataError("Невозможно разобрать CSV.") from exc
    return {"orders": list(unique.values()), "input_rows": count, "duplicates": duplicates,
            "source_sha256": source["source_sha256"], "run_key": source["run_key"]}


def aggregate(clean: dict) -> dict:
    """Суммировать оплаченные заказы по календарной дате.

clean — только результат quality_gate. Повторную проверку контракта здесь не делаем.
День → число заказов, количество товаров, выручка в целых копейках.
Нулевой доход допустим, если все корректные заказы отменены.
"""
    groups = {}
    paid = 0
    cancelled = 0
    for order in clean["orders"]:
        if order["status"] == "cancelled":
            # Отмена учитывается в общей статистике, но не добавляет дневную выручку.
            cancelled += 1
            continue
        paid += 1
        # setdefault заводит корзину дня при первом заказе и возвращает её же далее.
        group = groups.setdefault(order["order_date"], {"order_date": order["order_date"],
                                  "orders": 0, "units": 0, "revenue_cents": 0})
        group["orders"] += 1
        group["units"] += order["quantity"]
        # float не нужен: 2 книги по 15000 копеек дают ровно 30000 копеек.
        group["revenue_cents"] += order["quantity"] * order["unit_price_cents"]
    return {"contract_version": VERSION, "run_key": clean["run_key"],
            "source_sha256": clean["source_sha256"], "input_rows": clean["input_rows"],
            "unique_orders": len(clean["orders"]), "duplicates": clean["duplicates"],
            "paid_orders": paid, "cancelled_orders": cancelled,
            "revenue_cents": sum(g["revenue_cents"] for g in groups.values()),
            "daily": [groups[key] for key in sorted(groups)]}  # Одинаковый порядок вывода.


def json_bytes(value: dict) -> bytes:
    """Стабильный UTF-8 JSON: порядок ключей и финальный перевод строки фиксированы.

Это нужно для побайтового сравнения повторов, а не для ускорения сериализации.
"""
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def artifacts(summary: dict) -> dict[str, bytes]:
    """Подготовить три файла в памяти; запись на диск будет отдельным шагом.

summary.json нужен программам, report.md — человеку, manifest.json связывает их
с хэшами. Времён запуска здесь нет, иначе каждый повтор менял бы байты результата.
"""
    table = ["# Продажи за день", "", "Все суммы в копейках; данные синтетические.", "",
             "| Дата | Заказы | Товары | Выручка, коп. |", "| --- | --- | --- | --- |"]
    table.extend(f"| {r['order_date']} | {r['orders']} | {r['units']} | {r['revenue_cents']} |"
                 for r in summary["daily"])
    files = {"summary.json": json_bytes(summary), "report.md": ("\n".join(table) + "\n").encode("utf-8")}
    # Манифест не хэширует сам себя: это создало бы циклическую зависимость.
    files["manifest.json"] = json_bytes({"contract_version": VERSION, "run_key": summary["run_key"],
        "files": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}})
    return files


def publish(summary: dict, output: str | Path, *, fail: bool = False) -> dict:
    """Создать output/run_key либо проверить уже готовую папку без перезаписи.

summary — результат aggregate; output — корень локальных результатов.
fail — учебный временный отказ ДО записи, управляется задачей Prefect.
Возвращаем run_key и reused. Это локальные файлы, не публикация в GitHub.
"""
    root = Path(output).absolute()
    reject_symlinks(root)
    root.mkdir(parents=True, exist_ok=True)
    target = root / summary["run_key"]
    files = artifacts(summary)
    if target.exists() or target.is_symlink():
        reject_symlinks(target)
        # Проверяем и состав, и содержимое: одного наличия summary.json недостаточно.
        # Файл-ссылка тоже отклоняется. Чужие файлы не удаляем и не «ремонтируем» молча.
        if (not target.is_dir() or {p.name for p in target.iterdir()} != set(files)
                or any((target / name).is_symlink() or not (target / name).is_file()
                       or (target / name).read_bytes() != data for name, data in files.items())):
            raise PublishError("Существующий результат повреждён или содержит посторонние файлы.")
        return {"run_key": summary["run_key"], "reused": True}
    if fail:
        # Проверка существующего результата выше: корректный повтор не надо ломать.
        raise TransientPublishError("Учебный временный сбой приёмника; нужна повторная попытка.")
    # Соседняя папка гарантирует ту же файловую систему для переименования.
    # Пока запись не завершилась, читатель ещё не видит готовую папку с run_key.
    stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
    try:
        for name, data in files.items():
            (stage / name).write_bytes(data)
        stage.rename(target)
    finally:
        # Обычное исключение убирает недописанное. kill/сбой питания могут оставить staging.
        # Здесь нет fsync или блокировки для одновременных писателей — см. DECISIONS.md.
        if stage.exists():
            shutil.rmtree(stage)
    return {"run_key": summary["run_key"], "reused": False}


def generate_demo(output: str | Path) -> None:
    """Создать воспроизводимый пример: 6 заказов + 2 точных дубля = 8 строк.

Все значения вымышлены. Режим x откажется портить существующий файл.
Редактировать безопаснее отдельные копии из scripts/workshop.py prepare.
"""
    path = Path(output).absolute()
    reject_symlinks(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        ["O0001", "2026-10-01", "book", 2, 15000, "paid"],
        ["O0002", "2026-10-01", "pen", 3, 2000, "paid"],
        ["O0003", "2026-10-01", "bag", 1, 50000, "cancelled"],
        ["O0004", "2026-10-02", "book", 1, 15000, "paid"],
        ["O0005", "2026-10-02", "bag", 2, 50000, "paid"],
        ["O0006", "2026-10-02", "pen", 4, 2000, "paid"],
    ]
    # newline="" отдаёт управление переносами CSV-писателю; LF задаём явно для обеих ОС.
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(rows + [rows[0], rows[4]])
