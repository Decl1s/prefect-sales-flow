"""Изоляция настроек выполняется до первого импорта Prefect."""

import os
import sys
from pathlib import Path


def configure_local(home: str | Path) -> None:
    """Настроить отдельный локальный запуск до создания контекста Prefect.

home — папка технического состояния, не входы и не сводки. Переменные меняются
только в этом процессе и его дочернем сервере, настройки ОС на диске не переписываем.
"""
    if "prefect" in sys.modules:
        raise RuntimeError("Сначала configure_local, затем импорт оркестратора.")
    home = Path(home).absolute()
    home.mkdir(parents=True, exist_ok=True)
    # Убираем унаследованные PREFECT_* текущего процесса, чтобы случайно не использовать
    # адрес чужого сервера, его профиль или настройки Cloud. Другие переменные сохраняем.
    for key in list(os.environ):
        if key.startswith("PREFECT_"):
            del os.environ[key]
    os.environ.update({
        "PREFECT_HOME": str(home), "PREFECT_PROFILES_PATH": str(home / "profiles.toml"),
        # Сервер стартует по требованию на 127.0.0.1, завершается при выходе программы.
        "PREFECT_SERVER_EPHEMERAL_ENABLED": "true",
        "PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS": "90",  # Время на первый запуск и миграции.
        "PREFECT_SERVER_ANALYTICS_ENABLED": "false",
        "PREFECT_LOGGING_LEVEL": "WARNING", "PREFECT_LOGGING_TO_API_ENABLED": "false",
        "PREFECT_SERVER_UI_ENABLED": "false", "PREFECT_RESULTS_PERSIST_BY_DEFAULT": "false",
    })


def local_settings():
    """Контекст with: API URL и ключ явно None, даже если рядом настроен .env.

Не используй пустую строку вместо None: в Prefect 3.8.8 это ломало EventsWorker.
Выход из with восстанавливает контекст библиотеки; изоляция переменных процесса
делается отдельно в configure_local. Это не универсальный менеджер всех настроек.
"""
    from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL, temporary_settings
    return temporary_settings(updates={PREFECT_API_URL: None, PREFECT_API_KEY: None})
