# Эксплуатация Autocash

Целевая ОС — Ubuntu (второй дистрибутив не выбран, НФТ-2). Пути ниже — рекомендуемые; их можно менять, сохраняя права доступа.

## Раскладка на сервере

| Путь | Что лежит | Права |
|---|---|---|
| `/opt/autocash` | код и `.venv` | root, чтение для `autocash` |
| `/etc/autocash/config/*.yaml` | конфигурация (под git) | root, чтение для `autocash` |
| `/etc/autocash/autocash.env` | `AUTOCASH_PG_DSN`, `AUTOCASH_SESSION_KEY_FILE` | `600`, владелец `autocash` |
| `/etc/autocash/backup.env` | `AUTOCASH_BACKUP_DIR`, `AUTOCASH_DB`, `AUTOCASH_KEEP` | `600` |
| `/var/lib/autocash` | буфер SQLite, каталог сессий | `700`, владелец `autocash` |

Сессии хранятся зашифрованными, вне git, доступны только пользователю `autocash` (ФТ-1.9). Вход в новый аккаунт — `python -m autocash.cli.login`, результат — файл сессии в каталоге сессий (`pool.sessions_dir`); появление файла добавляет аккаунт в пул без рестарта.

## systemd и watchdog (НФТ-16, НФТ-17)

Unit: [`deploy/systemd/autocash.service`](../deploy/systemd/autocash.service).

```bash
sudo useradd --system --home /var/lib/autocash --shell /usr/sbin/nologin autocash
sudo install -d -o autocash -g autocash -m 700 /var/lib/autocash
sudo cp deploy/systemd/autocash.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now autocash
```

- `Type=notify`: процесс шлёт `READY=1`, когда поднят весь конвейер. Приём в SQLite начинается раньше, сразу после запуска, и от PostgreSQL не зависит.
- `WatchdogSec=30`: сигнал `WATCHDOG=1` отправляет задача основного цикла событий каждые 15 с. Если цикл завис, сигнал прекращается и systemd перезапускает процесс (`Restart=always`).
- Время от запуска до полной готовности видно в `/status` («полная готовность за …»).

Проверка: `systemctl status autocash` (строка `Status:`), журнал — `journalctl -u autocash -f`.

## Аварийный канал (НФТ-18)

При недоступном PostgreSQL:
- приём продолжается в SQLite, обработка ждёт, входящие помечаются `DB_UNAVAILABLE`;
- `/status` и `/accounts` отвечают из памяти через аккаунт пула напрямую, минуя Outbox; в ответе есть пометка «аварийный канал»;
- тревоги уходят в группу `logs` тем же путём.

Права на эти команды проверяются по конфигурации (роли из `users` и `sources[].senders`), поэтому база для проверки не нужна. Когда PostgreSQL возвращается, команды проходят обычную цепочку и получают трассу, но повторно не отвечают.

## Команды оператора

| Команда | Что делает |
|---|---|
| `/status` | сводка: время работы и готовности, PostgreSQL, буфер, пул, покрытие, очереди, зависшие |
| `/accounts` | аккаунты: состояние, паузы FloodWait, бюджет, обязанности |
| `/coverage` | недопокрытые чаты и чаты с приблизительной дедупликацией |
| `/queue` | очереди модулей и Outbox |
| `/trace <trace_id \| ссылка t.me/c/… \| Deposit ID>` | все этапы задачи, связанные задачи, попытки отправки |
| `/stuck [этап]` | зависшие задачи по этапам и кодам причин |
| `/retry <trace_id \| key=value …>` | перезапуск с текущего этапа |
| `/cancel <trace_id \| key=value …>` | отмена с записью автора |
| `/metrics [часы] [all\|source\|module\|account]` | медиана и p95 этапов, финалы по кодам (тяжёлый отчёт: сначала подтверждение) |
| `/reload` | перезагрузка конфигурации без рестарта |

Фильтр `/retry` и `/cancel`: ключи `reason`, `module`, `stage`, `chat`, `kind` (`module` или `outbound`), например `/retry kind=outbound reason=NO_ACCOUNT_IN_CHAT`. Фильтр выбирает только ожидающие задачи; уже завершённые неуспешно перезапускаются только по явному `trace_id`. Задача на этапе «отправка» не отменяется и не перезапускается: её исход может быть уже известен Telegram.

Права задаёт матрица `permissions` в конфигурации («роль пользователя → роль источника → команды»). Без прав — отказ и финал «отменено» с `ACCESS_DENIED`.

## Пользователи, группы и роли (модуль Role Manager)

Роли лежат в PostgreSQL (таблицы `rm_*`), копия — в памяти и в файле `directory.json` рядом с буфером SQLite.
- `/urole <TGID> <роль>`, `/grole <ID группы> <роль>`, `/rm set <ID> <роль>` (по роли понятно, кто это), `/rm ?` в группе.
- Группа без роли — `other`: известна, не читается. Пользователь без записи — `guest`.
- Команды сейчас только у Admin; Dev может всё всегда. Подробно — `autocash/modules/role_manager/RM_readme.md`.
- Первый Dev и импорт пользователей из Cash Monitor Dev: `python -m autocash.modules.role_manager.RM_cli` (`user`, `import-cmd`).
- Новый человек в личке: протокол первого или второго контакта, один раз на TGID (`protocol_runs`).

## PostgreSQL на машине разработки (Windows)

Отдельный сервер Autocash, не общий с другими проектами (D-RM-9): порт 5434, данные `Documents\AutoCash Data\pg`, DSN в `Documents\AutoCash Secrets\pg.env`.
`.\deploy\windowsutocash-pg.ps1 start | stop | status`. Перед запуском скрипт проверяет, что на диске больше 500 МБ.
Сам сервис: `deploy/windows/autocash-run.ps1 start | stop | status | log`, журнал — `Documents/AutoCash Data/logs/autocash.log`.

## Обратная связь от команд (AGENTS.md, правило 9)

Аккаунты пула — пользовательские, каждое их сообщение в группе видят кассиры и игроки, и каждое тратит лимиты Telegram. Поэтому:
- на нашу команду всегда есть ответ — автору, в чат команды; без прав — в чат ничего, отказ в `logs` и журнал монитора;
- на чужие команды (`/start` и т. п.) реакции нет;
- побочные эффекты (роль, конфиг, санкции) пишутся в группу `logs`;
- затронутой группе об изменении ничего не пишется;
- служебные команды лучше давать из лички с аккаунтом пула или из служебной группы: тогда ответ не увидят участники рабочей группы.

## Резервное копирование (НФТ-14)

Скрипты: [`deploy/backup/`](../deploy/backup). `AUTOCASH_BACKUP_DIR` должен указывать на **другой носитель или сервер** (смонтированный диск, NFS, SMB).

1. **Дамп** — `pg_dump.sh`, ежедневно. Пишет `*.dump` в формате custom под временным именем и переименовывает только целиком записанный файл; хранит `AUTOCASH_KEEP` последних, удаляя сначала самые старые.
2. **Архив WAL** — `archive_wal.sh` как `archive_command` PostgreSQL:
   ```
   archive_mode = on
   archive_command = '/opt/autocash/deploy/backup/archive_wal.sh %p %f'
   archive_timeout = 300
   ```
   Повтор сегмента после сбоя допустим только с тем же содержимым; иначе скрипт возвращает ошибку и PostgreSQL повторит архивацию.
3. **Проверка восстановления** — `verify_restore.sh`, еженедельно. Разворачивает последний дамп во временную базу, проверяет схему и основные таблицы и удаляет её. Ненулевой код возврата означает, что бэкапу доверять нельзя: его нужно разобрать в тот же день.

Пример таймеров (cron пользователя `postgres`):

```
15 3 * * *  /opt/autocash/deploy/backup/pg_dump.sh       >> /var/log/autocash-backup.log 2>&1
45 4 * * 0  /opt/autocash/deploy/backup/verify_restore.sh >> /var/log/autocash-backup.log 2>&1
```

### Восстановление

- **Из дампа:** остановить сервис, `createdb autocash && pg_restore --no-owner -d autocash <файл>.dump`, запустить сервис. События, принятые после дампа, остались в буфере SQLite (срок `buffer.retention_hours`) и выгрузятся повторно; повтор идемпотентен.
- **На момент времени (PITR):** базовая копия `pg_basebackup` + архив WAL, `restore_command = 'cp $AUTOCASH_BACKUP_DIR/wal/%f %p'` и `recovery_target_time`. Базовую копию делать после каждого изменения схемы.

## Жёсткая перезагрузка сервера (п. 3.4 ТЗ)

| Компонент | Что происходит после старта | Где проверено |
|---|---|---|
| События за время простоя | догон по чатам с разрывом через лимитер; «difference too long» → пометка разрыва и хук сверки | `test_pool_dynamics.py` |
| SQLite | подтверждённое цело (WAL, `synchronous=FULL`), Uploader продолжает | `test_crash.py`, `test_hard_reset.py` |
| Пачка Uploader | выгружается повторно, upsert даёт тот же результат | `test_pipeline.py`, `test_crash.py` |
| Очереди, горячие данные | пересборка из PostgreSQL от отметок модулей; приём новых событий идёт сразу | `test_crash.py`, `test_hard_reset.py` |
| Исходящее «отправка» | повтор тем же аккаунтом с тем же `random_id`, дубль отклоняется | `test_hard_reset.py` |
| Паузы FloodWait | читаются из БД до первого активного вызова | `test_pool_failures.py` |
| Состояния диалогов | хранятся в БД как есть (используются с этапа 3) | — |
| Трассы | незавершённые задачи продолжаются с последнего этапа; перезапущенные `/retry` — тоже | `test_hard_reset.py`, `test_commands.py` |
| Пул | аккаунты подключаются независимо, планировщик распределяет по мере готовности | `test_pool_dynamics.py` |
| Зависший процесс | watchdog systemd | `tests/unit/test_watchdog.py` (сигналы), unit-файл |

## Диск

Буфер SQLite ограничен `buffer.soft_limit_mb` / `hard_limit_mb`: на подходе — тревога, на пределе принимаются только источники высшего приоритета. Детальные трассы хранятся `retention.trace_days` дней, почасовые агрегаты — бессрочно. Текст сообщений удаляется по сроку типа (`text_ttl_days`), извлечённые поля остаются.
