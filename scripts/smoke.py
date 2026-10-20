"""Настоящие запуски Prefect: временный сервер существует только в этом процессе."""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sales_flow.core import DataError, PublishError, generate_demo
from sales_flow.local import configure_local, local_settings


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    # Prefect держит соединения до завершения процесса, поэтому его home располагается
    # в игнорируемом workspace, а не удаляется из-под работающего сервера (Windows).
    root = Path(__file__).resolve().parents[1]
    configure_local(root / "workspace/smoke-prefect")
    with local_settings():
        run_scenarios()


def run_scenarios() -> None:
    """Проверка связки компонентов; отрицательные сценарии обязаны завершаться отказом.

Если изучаешь только формулу — начни с test_core.py или workshop.preview.
Этот сценарий отвечает на другой вопрос: соблюдает ли сам Prefect зависимости
и политику попыток, когда запускаются настоящие задачи?
"""
    from prefect.settings import get_current_settings
    from sales_flow.orchestration import sales_pipeline
    settings = get_current_settings()
    assert settings.api.url is None and settings.api.key is None
    assert not settings.server.analytics_enabled
    with tempfile.TemporaryDirectory() as folder:
        work = Path(folder)
        source = work / "orders.csv"
        output = work / "output"
        generate_demo(source)
        # 1. Временная ошибка записи: движок повторяет именно publish_task.
        first = sales_pipeline(str(source), str(output), True)
        assert first["publish_attempts"] == 2 and not first["reused"], first
        assert first["revenue_cents"] == 159000
        target = output / first["run_key"]
        before = {p.name: p.read_bytes() for p in target.iterdir()}
        # 2. Новый запуск flow с теми же байтами не создаёт вторую копию результата.
        again = sales_pipeline(str(source), str(output), True)
        assert again["publish_attempts"] == 1 and again["reused"], again
        assert before == {p.name: p.read_bytes() for p in target.iterdir()}
        # 3. Неисправимый вход: return_state=True позволяет проверить Failed как объект,
        # не прерывая сам проверяющий скрипт. Папка приёмника не должна появиться.
        broken = work / "bad.csv"
        broken.write_bytes(source.read_bytes().replace(b",paid", b",unknown", 1))
        state = sales_pipeline(str(broken), str(work / "blocked"), return_state=True)
        assert state.is_failed(), state
        try:
            state.result()
        except DataError:
            pass
        else:
            raise AssertionError("Ожидалась ошибка качества")
        assert not (work / "blocked").exists()
        assert before == {p.name: p.read_bytes() for p in target.iterdir()}
        # Повреждение не является временным сбоем и не должно повторяться.
        (target / "report.md").write_text("corrupt", encoding="utf-8")
        from sales_flow.orchestration import publish_task, retry_transient
        from sales_flow.core import aggregate, quality_gate, read_source
        attempts = []

        def observed_retry(task, task_run, state):
            # 4. Наблюдаем callback движка; не гадаем по времени доставки событий API.
            attempts.append(task_run.run_count)
            return retry_transient(task, task_run, state)

        state = publish_task.with_options(retry_condition_fn=observed_retry)(
            aggregate(quality_gate(read_source(source))), str(output), False, return_state=True)
        assert state.is_failed()
        try:
            state.result()
        except PublishError:
            pass
        else:
            raise AssertionError("Ожидалась ошибка повреждения")
        assert attempts == [1], attempts
        print(json.dumps({"smoke": "passed", "retry_attempts": first["publish_attempts"],
                          "repeat_reused": again["reused"], "bad_input_blocked": True,
                          "corrupt_output_attempts": 1, "revenue_cents": 159000}, sort_keys=True))


if __name__ == "__main__":
    main()
