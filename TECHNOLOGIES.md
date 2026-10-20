# Фактические технологии и зависимости

Версия проекта 0.1.1, 19.10.2026. Главный инструмент — Prefect 3.8.8.
Зависимости установлены локально; глобальные компоненты и постоянные службы не создавались.

| Компонент | Версия | Назначение | Официальный источник |
| --- | --- | --- | --- |
| Python | 3.12.x; локально 3.12.14 | CLI, контракт, целочисленная обработка, тесты | https://docs.python.org/3.12/ |
| Prefect | 3.8.8, PyPI от 06.10.2026 | Flow, задачи, futures, состояния, избирательный retry | https://pypi.org/project/prefect/3.8.8/ |
| uv | 0.13.1, PyPI от 14.10.2026 | Локальное окружение и universal lock | https://pypi.org/project/uv/0.13.1/ |
| SQLite | Через установленный Python и aiosqlite; локально 3.53.1 | Техническая база временного Prefect, не аналитическое хранилище | https://www.sqlite.org/docs.html |
| CSV, JSON, Markdown | UTF-8 | Контракт входа, машинная сводка и русский отчёт | https://docs.python.org/3.12/library/csv.html |
| SHA-256 | hashlib стандартной библиотеки | Ключ пакета, проверка файлов и ZIP | https://docs.python.org/3.12/library/hashlib.html |
| unittest, tempfile, mock | Стандартная библиотека Python 3.12 | Контракт, файловые сбои, временные примеры | https://docs.python.org/3.12/library/unittest.html |
| ZIP | zipfile стандартной библиотеки | Архив исходников по явному списку | https://docs.python.org/3.12/library/zipfile.html |
| Git | Локально 2.43.0 | Отдельная история проекта, контроль списка файлов | https://git-scm.com/docs |
| GitHub Actions | Конфигурация Ubuntu и Windows, Python 3.12 | Тесты, настоящий Prefect, архив | https://docs.github.com/ru/actions |
| actions/checkout | v4, SHA в ci.yml | Получение исходников без сохранения учётных данных | https://github.com/actions/checkout |
| actions/setup-python | v5, SHA в ci.yml | Python 3.12 на runner | https://github.com/actions/setup-python |
| actions/upload-artifact | v4, SHA в ci.yml | ZIP и SHA256SUMS как результат проверки | https://github.com/actions/upload-artifact |
| Mermaid | Разметка диаграммы в NOTES.md | Объяснение цепочки обработки | https://mermaid.js.org/ |

Модули приложения из стандартной библиотеки: argparse, csv, datetime, hashlib, io,
json, os, pathlib, re, shutil, sys и tempfile. Проверки и упаковка дополнительно
используют importlib.util, subprocess, tomllib, unittest, unittest.mock и zipfile.
Установка uv в bootstrap использует pip, поставляемый с Python; он не является
зависимостью исполнения приложения. Версия uv закреплена в pyproject.toml и CI.
Python ограничен веткой 3.12, чтобы учебный пример имел одну проверяемую среду.

## Полный набор сторонних пакетов

Прямая зависимость приложения — только prefect==3.8.8. Все остальные строки ниже —
транзитивные зависимости Prefect, включая условные зависимости разных ОС.
Версии, маркеры платформ, публичные URL и хэши дистрибутивов содержатся в uv.lock.
Таблица показывает весь lock, а не обещает установку каждого пакета на каждой ОС.
На Linux фактически установлено 105 пакетов; uv разрешил 107 записей вместе с проектом.
Для восстановления используйте uv sync --frozen --no-dev: pip install prefect без lock
не воспроизводит этот набор. Изменение lock требует повторить проверки.

Наличие библиотек Redis, PostgreSQL, Docker и аналитики в зависимостях Prefect
не означает запуск этих сервисов. Используется встроенный временный сервер и SQLite;
аналитика отключена настройкой PREFECT_SERVER_ANALYTICS_ENABLED=false.
API URL и API key в контексте локального запуска явно равны None.
Полные промежуточные результаты задач не сохраняются средствами Prefect.
Техническая история находится только в игнорируемом workspace.

| Пакет | Зафиксированная версия | Назначение в наборе Prefect / официальный источник |
| --- | --- | --- |
| aiosqlite | 0.22.1 | Асинхронный доступ к локальной SQLite: https://pypi.org/project/aiosqlite/0.22.1/ |
| alembic | 1.20.0 | Миграции схемы технической базы: https://pypi.org/project/alembic/1.20.0/ |
| amplitude-analytics | 1.3.0 | Клиент аналитики Prefect; отправка отключена: https://pypi.org/project/amplitude-analytics/1.3.0/ |
| annotated-doc | 0.0.5 | Метаданные документации типов: https://pypi.org/project/annotated-doc/0.0.5/ |
| annotated-types | 0.8.0 | Ограничения типов для Pydantic: https://pypi.org/project/annotated-types/0.8.0/ |
| anyio | 4.15.1 | Совместимость асинхронного исполнения: https://pypi.org/project/anyio/4.15.1/ |
| apprise | 1.13.1 | Поддержка уведомлений Prefect; в примере не настроена: https://pypi.org/project/apprise/1.13.1/ |
| asgi-lifespan | 2.1.0 | Жизненный цикл ASGI-приложения: https://pypi.org/project/asgi-lifespan/2.1.0/ |
| asyncpg | 0.32.0 | Неиспользуемый здесь драйвер PostgreSQL: https://pypi.org/project/asyncpg/0.32.0/ |
| attrs | 26.1.0 | Декларативные классы данных: https://pypi.org/project/attrs/26.1.0/ |
| beartype | 0.23.1 | Проверки типов во время исполнения: https://pypi.org/project/beartype/0.23.1/ |
| burner-redis | 0.1.7 | Временный Redis для возможностей Prefect; здесь не запускается: https://pypi.org/project/burner-redis/0.1.7/ |
| cachetools | 7.2.3 | Вспомогательные кэши в зависимостях: https://pypi.org/project/cachetools/7.2.3/ |
| certifi | 2026.10.18 | Набор доверенных сертификатов для HTTPS: https://pypi.org/project/certifi/2026.10.18/ |
| cffi | 2.1.1 | Интерфейс вызова нативного кода: https://pypi.org/project/cffi/2.1.1/ |
| charset-normalizer | 3.5.2 | Определение кодировки HTTP-ответов: https://pypi.org/project/charset-normalizer/3.5.2/ |
| click | 8.5.0 | Основа CLI зависимостей: https://pypi.org/project/click/8.5.0/ |
| cloudpickle | 3.1.2 | Сериализация Python-объектов в Prefect: https://pypi.org/project/cloudpickle/3.1.2/ |
| colorama | 0.4.6 | Цветной вывод терминала, включая Windows: https://pypi.org/project/colorama/0.4.6/ |
| coolname | 5.0.0 | Генерация имён запусков Prefect: https://pypi.org/project/coolname/5.0.0/ |
| cronsim | 2.7 | Разбор cron для планирования; расписание здесь не создаётся: https://pypi.org/project/cronsim/2.7/ |
| cryptography | 50.0.2 | Криптографические функции технического стека: https://pypi.org/project/cryptography/50.0.2/ |
| cyclopts | 5.2.0 | Разбор аргументов CLI Prefect: https://pypi.org/project/cyclopts/5.2.0/ |
| dateparser | 1.4.3 | Разбор дат в возможностях Prefect: https://pypi.org/project/dateparser/1.4.3/ |
| docker | 7.2.0 | Неиспользуемый здесь SDK Docker: https://pypi.org/project/docker/7.2.0/ |
| docstring-parser | 0.18.0 | Разбор описаний функций: https://pypi.org/project/docstring-parser/0.18.0/ |
| exceptiongroup | 1.3.1 | Группы исключений и совместимость: https://pypi.org/project/exceptiongroup/1.3.1/ |
| fastapi | 0.147.0 | HTTP API локального Prefect: https://pypi.org/project/fastapi/0.147.0/ |
| fsspec | 2026.9.0 | Абстракции файловых хранилищ Prefect: https://pypi.org/project/fsspec/2026.9.0/ |
| graphviz | 0.21 | Поддержка графов; внешний Graphviz не установлен: https://pypi.org/project/graphviz/0.21/ |
| greenlet | 3.5.6 | Контексты исполнения SQLAlchemy: https://pypi.org/project/greenlet/3.5.6/ |
| griffe | 2.3.2 | Интроспекция Python и документация: https://pypi.org/project/griffe/2.3.2/ |
| griffecli | 2.3.2 | CLI интроспекции Griffe: https://pypi.org/project/griffecli/2.3.2/ |
| griffelib | 2.3.2 | Библиотека интроспекции Griffe: https://pypi.org/project/griffelib/2.3.2/ |
| h11 | 0.16.0 | Протокол HTTP/1.1: https://pypi.org/project/h11/0.16.0/ |
| h2 | 4.4.1 | Протокол HTTP/2: https://pypi.org/project/h2/4.4.1/ |
| hpack | 4.2.0 | Сжатие заголовков HTTP/2: https://pypi.org/project/hpack/4.2.0/ |
| httpcore | 1.0.9 | Транспорт HTTPX: https://pypi.org/project/httpcore/1.0.9/ |
| httpx | 0.28.1 | HTTP-клиент Prefect: https://pypi.org/project/httpx/0.28.1/ |
| humanize | 4.16.0 | Читаемое представление чисел и времени: https://pypi.org/project/humanize/4.16.0/ |
| hyperframe | 6.1.0 | Кадры HTTP/2: https://pypi.org/project/hyperframe/6.1.0/ |
| idna | 3.20 | Доменные имена Unicode: https://pypi.org/project/idna/3.20/ |
| jinja2 | 3.1.6 | Шаблоны технического стека: https://pypi.org/project/jinja2/3.1.6/ |
| jinja2-humanize-extension | 0.4.0 | Форматирование в шаблонах: https://pypi.org/project/jinja2-humanize-extension/0.4.0/ |
| jsonpatch | 1.36 | Операции JSON Patch: https://pypi.org/project/jsonpatch/1.36/ |
| jsonpointer | 3.4 | Адресация частей JSON: https://pypi.org/project/jsonpointer/3.4/ |
| jsonschema | 4.26.0 | Проверка схем JSON: https://pypi.org/project/jsonschema/4.26.0/ |
| jsonschema-specifications | 2025.9.1 | Стандартные метасхемы JSON Schema: https://pypi.org/project/jsonschema-specifications/2025.9.1/ |
| mako | 1.4.3 | Шаблоны миграций Alembic: https://pypi.org/project/mako/1.4.3/ |
| markdown | 3.11.1 | Разбор Markdown: https://pypi.org/project/markdown/3.11.1/ |
| markdown-it-py | 4.2.0 | Парсер Markdown для Rich: https://pypi.org/project/markdown-it-py/4.2.0/ |
| markupsafe | 3.0.4 | Безопасные строки шаблонов: https://pypi.org/project/markupsafe/3.0.4/ |
| mdurl | 0.1.2 | Разбор URL в Markdown: https://pypi.org/project/mdurl/0.1.2/ |
| oauthlib | 4.0.0 | Поддержка OAuth; авторизация здесь не используется: https://pypi.org/project/oauthlib/4.0.0/ |
| opentelemetry-api | 1.45.1 | Интерфейсы инструментирования Prefect: https://pypi.org/project/opentelemetry-api/1.45.1/ |
| orjson | 3.13.0 | Быстрый JSON в техническом стеке: https://pypi.org/project/orjson/3.13.0/ |
| packaging | 26.3 | Версии пакетов и маркеры платформ: https://pypi.org/project/packaging/26.3/ |
| pathspec | 1.1.1 | Сопоставление файловых шаблонов: https://pypi.org/project/pathspec/1.1.1/ |
| pendulum | 3.3.0 | Даты и часовые пояса в Prefect: https://pypi.org/project/pendulum/3.3.0/ |
| pluggy | 1.7.0 | Механизм плагинов: https://pypi.org/project/pluggy/1.7.0/ |
| prefect | 3.8.8 | Прямая зависимость: оркестратор: https://pypi.org/project/prefect/3.8.8/ |
| prometheus-client | 0.26.0 | Поддержка метрик; отдельный сервер метрик не запускается: https://pypi.org/project/prometheus-client/0.26.0/ |
| py-key-value-aio | 0.4.6 | Асинхронные интерфейсы key-value: https://pypi.org/project/py-key-value-aio/0.4.6/ |
| pycparser | 3.11 | Парсер C для CFFI: https://pypi.org/project/pycparser/3.11/ |
| pydantic | 2.14.1 | Проверка технических моделей Prefect: https://pypi.org/project/pydantic/2.14.1/ |
| pydantic-core | 2.50.1 | Ядро проверки Pydantic: https://pypi.org/project/pydantic-core/2.50.1/ |
| pydantic-extra-types | 2.11.1 | Дополнительные типы Pydantic: https://pypi.org/project/pydantic-extra-types/2.11.1/ |
| pydantic-settings | 2.16.0 | Загрузка настроек Prefect: https://pypi.org/project/pydantic-settings/2.16.0/ |
| pydocket | 0.27.0 | Внутреннее выполнение фоновых задач Prefect: https://pypi.org/project/pydocket/0.27.0/ |
| pygments | 2.21.0 | Подсветка синтаксиса в выводе: https://pypi.org/project/pygments/2.21.0/ |
| python-dateutil | 2.9.0.post0 | Вспомогательная обработка дат: https://pypi.org/project/python-dateutil/2.9.0.post0/ |
| python-dotenv | 1.2.4 | Поддержка env-файлов настроек: https://pypi.org/project/python-dotenv/1.2.4/ |
| python-json-logger | 4.2.0 | Форматирование технических JSON-логов: https://pypi.org/project/python-json-logger/4.2.0/ |
| python-slugify | 9.0.0 | Создание строковых идентификаторов: https://pypi.org/project/python-slugify/9.0.0/ |
| pytz | 2026.5 | База часовых поясов для зависимостей: https://pypi.org/project/pytz/2026.5/ |
| pywin32 | 312 | Условная интеграция Prefect с Windows API: https://pypi.org/project/pywin32/312/ |
| pyyaml | 6.0.3 | Чтение YAML в Prefect: https://pypi.org/project/pyyaml/6.0.3/ |
| readchar | 4.2.2 | Интерактивный ввод CLI: https://pypi.org/project/readchar/4.2.2/ |
| redis | 8.1.0 | Клиент Redis; сервер в этом проекте не используется: https://pypi.org/project/redis/8.1.0/ |
| referencing | 0.37.0 | Ссылки в JSON Schema: https://pypi.org/project/referencing/0.37.0/ |
| regex | 2026.9.29 | Расширенные регулярные выражения для зависимостей: https://pypi.org/project/regex/2026.9.29/ |
| requests | 2.34.2 | HTTP-клиент транзитивных библиотек: https://pypi.org/project/requests/2.34.2/ |
| requests-oauthlib | 2.0.0 | Интеграция OAuth с Requests; здесь не настроена: https://pypi.org/project/requests-oauthlib/2.0.0/ |
| rfc3339-validator | 0.1.4 | Проверка временных меток: https://pypi.org/project/rfc3339-validator/0.1.4/ |
| rich | 15.0.0 | Оформление терминала: https://pypi.org/project/rich/15.0.0/ |
| rich-rst | 2.2.0 | Отображение reStructuredText: https://pypi.org/project/rich-rst/2.2.0/ |
| rpds-py | 2026.9.1 | Структуры данных JSON Schema: https://pypi.org/project/rpds-py/2026.9.1/ |
| ruamel-yaml | 0.19.1 | Обработка YAML в Prefect: https://pypi.org/project/ruamel-yaml/0.19.1/ |
| ruamel-yaml-clib | 0.2.15 | Нативное ускорение YAML: https://pypi.org/project/ruamel-yaml-clib/0.2.15/ |
| semver | 3.1.0 | Семантические версии: https://pypi.org/project/semver/3.1.0/ |
| shellingham | 1.5.4 | Определение оболочки CLI: https://pypi.org/project/shellingham/1.5.4/ |
| six | 1.17.0 | Совместимость библиотек: https://pypi.org/project/six/1.17.0/ |
| sniffio | 1.3.1 | Определение асинхронной среды: https://pypi.org/project/sniffio/1.3.1/ |
| sqlalchemy | 2.1.5 | Доступ Prefect к технической базе: https://pypi.org/project/sqlalchemy/2.1.5/ |
| starlette | 1.9.0 | ASGI-основа HTTP API: https://pypi.org/project/starlette/1.9.0/ |
| text-unidecode | 1.3 | Транслитерация для slugify: https://pypi.org/project/text-unidecode/1.3/ |
| toml | 0.10.2 | Разбор настроек TOML: https://pypi.org/project/toml/0.10.2/ |
| typer | 0.27.3 | CLI-компоненты зависимостей: https://pypi.org/project/typer/0.27.3/ |
| typing-extensions | 4.16.0 | Совместимость типов Python: https://pypi.org/project/typing-extensions/4.16.0/ |
| typing-inspection | 0.4.4 | Интроспекция аннотаций типов: https://pypi.org/project/typing-inspection/0.4.4/ |
| tzdata | 2026.5 | Данные часовых поясов: https://pypi.org/project/tzdata/2026.5/ |
| tzlocal | 5.4.4 | Определение локального часового пояса: https://pypi.org/project/tzlocal/5.4.4/ |
| uncalled-for | 0.4.1 | Внутренний вспомогательный пакет Prefect: https://pypi.org/project/uncalled-for/0.4.1/ |
| urllib3 | 2.8.0 | Транспорт Requests: https://pypi.org/project/urllib3/2.8.0/ |
| uvicorn | 0.54.0 | Временный ASGI-сервер на loopback: https://pypi.org/project/uvicorn/0.54.0/ |
| websockets | 16.1.1 | Локальная доставка событий Prefect: https://pypi.org/project/websockets/16.1.1/ |

## Необязательная учебная IDE

| Компонент | Фактически установленная версия | Назначение / официальный источник |
| --- | --- | --- |
| Visual Studio Code | 1.140.0 | Просмотр пяти проектов, задачи и отладка: https://code.visualstudio.com/docs |
| ms-python.python | 2026.6.0 | Выбор Python и поиск unittest: https://marketplace.visualstudio.com/items?itemName=ms-python.python |
| ms-python.debugpy | 2026.6.0 | Отладчик Python и конфигурации F5: https://marketplace.visualstudio.com/items?itemName=ms-python.debugpy |
| ms-python.vscode-pylance | 2026.4.1 | Подсказки и переходы по Python-коду: https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance |

Эти компоненты уже были установлены на текущем компьютере; новых расширений или
пакетов ради IDE не добавляли. Они не являются зависимостями приложения и не входят
в uv.lock. CLI работает без редактора. Конфигурации .vscode используют отдельный
интерпретатор .venv проекта; tasks имеют type=process и передают аргументы массивом.
Лабораторная scripts/workshop.py использует только стандартную библиотеку Python.
Общий workspace серии хранится локально отдельно от исходников приложения.
