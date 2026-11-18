# Autocash

Мониторинг Telegram-чатов касс: приём → SQLite-буфер → PostgreSQL → сортировка → модули → отправка через очередь исходящих.
ТЗ — [`docs/TZ.md`](docs/TZ.md), правила работы — [`AGENTS.md`](AGENTS.md), решения — [`docs/decisions.md`](docs/decisions.md),
эксплуатация на сервере — [`docs/ops.md`](docs/ops.md), приёмка этапов — [`docs/acceptance/`](docs/acceptance).

## Что НЕ лежит в git

| Что | Где на старой машине | Как перенести |
|---|---|---|
| Ключ шифрования сессий | `Documents\AutoCash Secrets\session.key` | отдельно от сессий (менеджер паролей, флешка) |
| Зашифрованные сессии | `Documents\AutoCash Secrets\sessions\*.session.enc` | любым способом: без ключа не читаются |
| Рабочая конфигурация | `config/` (пример — `config.example/`) | создать заново из примера |
| Папки прототипа `accounts/<имя>/` | `account.env`, `session.session` — **незашифрованы** | не переносить; нужны только для `--import-folder` |

Одну сессию нельзя запускать в двух процессах или на двух машинах одновременно: перед запуском на новой машине остановите Autocash на старой.

## Запуск на новой машине

Нужны Python 3.12, PostgreSQL 16+ (для тестов — бинарники `initdb`/`pg_ctl`), git.

```bash
git clone <URL репозитория> autocash && cd autocash
python -m venv .venv
.venv/bin/pip install -r requirements-dev.txt          # Windows: .venv\Scripts\pip ...
```

1. Скопируйте `session.key` и каталог `sessions/` из «AutoCash Secrets» в каталог вне репозитория
   (на сервере — `/etc/autocash/session.key` с правами `600` и `/var/lib/autocash/sessions`, см. `docs/ops.md`).
2. Конфигурация: `cp -r config.example config` и поправьте `config/autocash.yaml`:
   `pool.sessions_dir` — путь к каталогу сессий, при необходимости `web.enabled: true`.
   Пользователи, группы и роли — в PostgreSQL (модуль RM): первого Dev занесите `python -m autocash.modules.role_manager.RM_cli user <TGID> dev`, дальше — `/urole`.
3. База — отдельная, не общая с другими проектами: `createdb autocash` (схема создаётся миграциями при старте).
   На Windows-машине разработки — свой сервер на порту 5434: `deploy/windows/autocash-pg.ps1 start` (docs/ops.md).
4. Переменные окружения (не в git):
   ```bash
   export AUTOCASH_PG_DSN='postgresql://<user>:<password>@127.0.0.1:5432/autocash'
   export AUTOCASH_SESSION_KEY_FILE=/путь/к/session.key
   ```
5. Запуск: `python -m autocash.cli.run --config config --data data` (буфер SQLite — в `data/`; на сервере — systemd,
   `deploy/systemd/autocash.service`). `main.py` — старый прототип, не для работы.
6. Админка (по желанию): `python -m autocash.cli.webuser set <Telegram ID>`, затем http://127.0.0.1:8088.

Новый аккаунт без прототипа: `python -m autocash.cli.login --sessions <каталог сессий> --name <имя>`.

## Модули

Каждый модуль — папка `autocash/modules/<имя>/` с префиксом в именах файлов и своим `<ПРЕФИКС>_readme.md` (AGENTS.md, правило 11).

| Модуль | Префикс | Что делает |
|---|---|---|
| `role_manager` | `RM` | пользователи, группы, роли и права в PostgreSQL: `/urole`, `/grole`, `/rm set`, `/rm ?` |
| `autosup` | `AS` | ID игрока в личных сообщениях: просьба, распознавание, игрок, закрепление |
| `deposits`, `players` | — | черновики по предположениям о формате касс, выключены |

Отправку в Telegram делает только Outbox ядра (`autocash/core/outbox/`, префикс `OBX`).

## Проверка

```bash
timeout 1800 python -m pytest -q          # тестовый PostgreSQL поднимается сам; путь к бинарникам — AUTOCASH_PG_BIN
ruff check autocash tests && mypy autocash
```

Один тест — не дольше 5 минут (pytest-timeout); долгие прогоны — в фоне. Три теста скорости
(p95 задержки, `/status` во время backfill, изоляция медленного модуля) на перегруженной машине не проходят —
проверять на целевом сервере.

## Состояние (2026-11-17)

Этапы 1a–5 написаны и закоммичены. Отложено: бизнес-логика на реальных сообщениях касс,
импорт сырых сообщений из Cash Monitor Dev (только чтение, в Autocash как `source=backfill`),
проверка на живом Telegram и админки в браузере.
