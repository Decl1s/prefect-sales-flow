"""Диспетчер исполнения: КОГДА вызывать правила из core.py и ЧТО повторять.

Декораторы превращают обычные функции в задачи/конвейер Prefect. Их тела специально
короткие: формула дохода не должна зависеть от выбранного оркестратора.
Этот модуль импортируют только после configure_local и внутри local_settings.
"""

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.context import get_run_context
from prefect.task_runners import ThreadPoolTaskRunner

from .core import TransientPublishError, aggregate, publish, quality_gate, read_source


def retry_transient(task, task_run, state) -> bool:
    """Callback Prefect: True разрешает следующую попытку, False останавливает её.

task — описание задачи; task_run — конкретное исполнение; state — его состояние.
state.result() поднимает исключение упавшей попытки. Не маскируем ошибки данных,
прав доступа и повреждения результата под временную недоступность приёмника.
"""
    try:
        state.result()
    except TransientPublishError:
        return True
    except Exception:
        return False
    return False


# NO_CACHE и persist_result=False: содержимое заказов не попадает в файловый кэш задач.
# Повторяемость загрузки обеспечивается publish(), независимо от кэша Prefect.
@task(name="Чтение CSV", retries=0, cache_policy=NO_CACHE, persist_result=False)
def read_task(source: str) -> dict:
    """Шаг 1: ограниченное чтение, декодирование и ключ входного пакета."""
    return read_source(source)


@task(name="Проверка качества", retries=0, cache_policy=NO_CACHE, persist_result=False)
def quality_task(source: dict) -> dict:
    """Шаг 2: контракт и дедупликация. Тот же плохой CSV от retry не исправится."""
    return quality_gate(source)


@task(name="Дневная сводка", retries=0, cache_policy=NO_CACHE, persist_result=False)
def aggregate_task(clean: dict) -> dict:
    """Шаг 3: детерминированный расчёт только проверенных заказов."""
    return aggregate(clean)


# retries — число ДОПОЛНИТЕЛЬНЫХ попыток: 2 означает максимум 3, включая первую.
# Задержка 0.1 секунды ускоряет урок; для настоящего сервиса выбирают backoff и jitter.
@task(name="Публикация результата", retries=2, retry_delay_seconds=0.1,
      retry_condition_fn=retry_transient, cache_policy=NO_CACHE, persist_result=False)
def publish_task(summary: dict, output: str, fail_once: bool) -> dict:
    """Шаг 4: запись с контролируемым отказом только на первой попытке.

run_count берётся у движка Prefect. Глобальный Python-счётчик был бы неверным
для нескольких запусков и восстановления процесса.
"""
    attempt = get_run_context().task_run.run_count
    result = publish(summary, output, fail=fail_once and attempt == 1)
    # Номер попытки — технический результат запуска; в неизменяемую сводку он не входит.
    return {**result, "publish_attempts": attempt, "revenue_cents": summary["revenue_cents"]}


@flow(name="Локальные продажи", task_runner=ThreadPoolTaskRunner(max_workers=2),
      persist_result=False, log_prints=False)
def sales_pipeline(source: str, output: str, fail_once: bool = False) -> dict:
    """Собрать граф зависимостей и дождаться конечного результата.

submit возвращает Future, а не готовый dict. Передача Future в следующий шаг
говорит Prefect: «запускай его только после успешного завершения предыдущего».
Два потока не ускоряют эту последовательную цепочку; это локальный task runner.
"""
    raw = read_task.submit(source)
    clean = quality_task.submit(raw)
    summary = aggregate_task.submit(clean)
    published = publish_task.submit(summary, output, fail_once)
    # Явно получаем ошибку первого плохого шага. Зависимые задачи не получают его данные.
    clean.result()  # Ошибка чтения/качества становится причиной ошибки всего flow.
    return published.result()
