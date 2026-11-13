# CLAUDE.md — autocash support (agent context)

Read this file only. Goal: enough context to work on the project without opening other files first.
Language: user writes Russian, answer in Russian. Code, comments, identifiers: English.

## 0. TL;DR
- Python 3.10+ app on Telethon 1.44.0 (pinned). Logs in to Telegram via MTProto API with many accounts (user accounts and bots).
- Each account = one folder in `accounts/`, config in `account.env`, session in the same folder.
- Pipeline plan: (1) capture raw Telegram text, (2) process it, (3) emit output. Stage 1 is the only one implemented.
- Targets: Windows 11 and Ubuntu, same behaviour on both.
- Old project `C:\Users\Den\Documents\ChatGPT\CASH MONITOR DEV` is a separate reference codebase. Do not import from it.

## 1. Principles
1. Module rule: all files of a module live in the module folder. Code that spans several modules goes in the shared package `autocash/`. Entry points that orchestrate all modules stay in the root (`main.py`).
2. Root stays minimal: `main.py`, `requirements.txt`, `autocash/`, `accounts/`, `.venv/` (gitignored), `.gitignore`, `CLAUDE.md`.
3. Cross-platform: every feature works on Windows and Ubuntu. Shared logic is Python. Only launchers differ.
4. Platform-specific files are suffixed `[win]` or `[lin]`: `add account [win].cmd`, `add account [lin].sh`.
5. Launchers work from any current directory: resolve paths from the script's own location.
6. `.cmd` files: ASCII-only text, CRLF line endings. cmd.exe reads the file in OEM codepage, so Cyrillic becomes gibberish and the script breaks. Console messages in `.cmd` are English.
7. Secrets never printed: API HASH and bot token are never echoed. Phone is echoed only in the add_account summary. `account.env` gets mode 600 (no-op on Windows). Never commit `account.env`, `*.session`, or account folders.
8. Accounts are data, not code. Adding an account must not require code changes.
9. Prefer simple code. No frameworks, no DB until a stage needs it. No comments unless the why is non-obvious.
10. Sending messages (not implemented yet): inherited requirement from the old project — account-wide ceiling 10 msg/s, all sends through one queue. Respect it when sending is added.
11. One account (one `session.session`) = one running process. Two processes on the same session file contend for SQLite locks; never run two copies.

## 2. Layout
```
autocash support/
  CLAUDE.md                  this file
  .gitignore                 ignores .venv/, __pycache__/, accounts/*/ except account_example/
  requirements.txt           telethon==1.44.0, python-dotenv>=1.0
  main.py                    run all accounts (listener stage)
  autocash/                  shared package
    __init__.py              empty
    accounts.py              account discovery and validation
    tg_client.py             Telegram client creation and login
  accounts/                  account tooling + account folders (module: accounts)
    add account [win].cmd    Windows launcher for add_account.py
    add account [lin].sh     Ubuntu launcher for add_account.py
    add_account.py           interactive console: create account folder
    login.py                 interactive login for an existing account
    account_example/         template folder (skipped by loader)
      account.env.example    template of account.env with comments
    <account folders>/       one per account: account.env, session.session (created at login)
```
Not in git yet: repository is initialized, no commits.

## 3. Modules and functions

### autocash/accounts.py
Constants: `ACCOUNTS_DIR` (= root/accounts), `ENV_FILE = "account.env"`, `TEMPLATE_DIR = "account_example"`.
- `@dataclass(frozen) Account`: `name` (folder name), `folder` (Path), `kind` ("user"|"bot"), `api_id` (int), `api_hash`, `phone`, `bot_token`. Property `session_path` -> `str(folder / "session")` (Telethon appends `.session`).
- `load_accounts(root=ACCOUNTS_DIR) -> list[Account]`: iterates sorted subfolders. Skips: files, names starting with `_` or `.`, `TEMPLATE_DIR`, folders without `account.env`. Invalid folders print `Пропущен: <reason>` and are skipped (never crash the whole run).
- `find_account(folder_name, root=ACCOUNTS_DIR) -> Account`: exact name match from `load_accounts`; raises `SystemExit` if not found or invalid.
- `_parse(folder, values) -> Account` (private): validates `TG_KIND` in {user, bot}; required: `TG_API_ID`, `TG_API_HASH`, plus `TG_BOT_TOKEN` (bot) or `TG_PHONE` (user). Raises `ValueError` on problems.

### autocash/tg_client.py
- `async connect(account, *, interactive=False) -> TelegramClient`
  - bot: `client.start(bot_token=...)`, always works non-interactively.
  - user: `connect()`; if already authorized, return. If not authorized and not `interactive`: disconnect and raise `RuntimeError` (caller tells user to run login). If `interactive`: `start(phone=...)` (Telegram sends code to console).

### main.py
- `run_account(account)`: non-interactive `connect`; on `RuntimeError` prints message and returns. Attaches `events.NewMessage` handler that prints `[<account>] <chat_id>: <raw_text>`. Then `run_until_disconnected()`.
- `main()`: `load_accounts()`; `SystemExit` if none; `asyncio.gather` over all accounts. Only `RuntimeError` is caught per account; any other exception (network, Telethon errors) propagates and stops `main()`.
- Current behaviour: console output only. No storage yet.

### accounts/login.py (CLI)
- `python accounts/login.py "<folder name>"`: `find_account` -> `connect(interactive=True)` -> prints `id`. Usage message if args missing.
- Adds project root to `sys.path` so `autocash` imports work when run from `accounts/`.

### accounts/add_account.py (CLI, interactive)
Flow (all prompts in console, validated with re-ask on error):
1. `kind`: user | bot (default user).
2. `tg_id`: optional digits. Folder name = `acc_<tg_id> <name>` if given, else `<name>`.
3. `name`: no `\ / : * ? " < > |`.
4. Folder must not exist (else exit).
5. `api_id` (digits), `api_hash` (any text).
6. user: `phone` (`+` and 7–15 digits). bot: `bot_token` (`digits:token`).
7. Summary: kind, folder, API ID, phone (shown in full), `API HASH: задан`, `токен: задан` (hash and token never shown). Confirm `[Y/n]`.
8. Creates folder, writes `account.env` (values double-quoted, escaped), chmod 600.
9. Optional: `check_connection` runs `connect(interactive=True)` and prints the id. Failure prints the `login.py` command to retry. For bots this only validates the token; the prompt text mentions a code, which bots never receive.
Helpers: `ask`, `yes`, `check_digits`, `check_name`, `check_phone`, `check_token`, `check_kind`, `quote`, `write_env`, `check_connection`, `main`.

### Launchers (accounts/)
- `add account [win].cmd`: `HERE` = script dir, `ROOT` = parent. Creates `ROOT\.venv` with `py -3` (fallback `python`) and installs `requirements.txt` only if `.venv\Scripts\python.exe` is missing. Runs `add_account.py`, then `pause`.
- `add account [lin].sh`: same logic, `ROOT/.venv/bin/python`, uses `python3 -m venv` (needs `sudo apt install python3-venv` on Ubuntu). Ends with `exec`.

## 4. Data contracts
`accounts/<folder>/account.env`:
```
TG_KIND=user|bot
TG_API_ID=<int>           from https://my.telegram.org
TG_API_HASH=<str>
TG_PHONE=+<country><number>   user only
TG_BOT_TOKEN=<digits:token>   bot only
```
Unused keys are written as empty strings by `add_account.py`.
`accounts/<folder>/session.session`: Telethon SQLite session, created at login. Treat as secret.
Folder name convention: `acc_<telegram id> <display name>`, example `acc_74738286 Jonny Cash`.

## 5. Flows
- Add account: `add account [win].cmd` (or `[lin].sh`) -> `add_account.py` -> folder + `account.env`.
- Add account by hand: copy `accounts/account_example`, rename, fill `account.env`, run `accounts/login.py "<name>"` once (user accounts only).
- Run: `.venv\Scripts\python.exe main.py` (Windows) or `.venv/bin/python main.py` (Ubuntu). Starts every valid bot account and every logged-in user account. Not-logged-in user accounts print a message and are not started. Invalid folders print `Пропущен: ...` at load.

## 6. Platform notes
- Windows venv: `.venv\Scripts\python.exe`. Ubuntu venv: `.venv/bin/python`.
- Ubuntu launcher needs `chmod +x` or run as `bash "accounts/add account [lin].sh"`.
- Python on this machine is a cache-based runtime referenced from `.venv/pyvenv.cfg`. If `.venv` breaks, delete `.venv` and rerun the launcher.

## 7. Known gaps / next steps
- Stage 1 (raw text capture) is only console output. Next: persist raw messages (per account, timestamp, chat_id, sender, text) in a local store. Choose storage before writing (SQLite per account is the simplest candidate).
- Stage 2 (processing) and stage 3 (output) not designed.
- `accounts/account_example — копия/` has an `account.env` with empty fields. Its name is not `account_example`, so the loader does not exclude it by name; it prints `Пропущен: ...` on every run. Delete it if unwanted (user decision, not done).
- Linux launcher not tested on Ubuntu.
- No automated tests yet.
- `load_accounts` raises `FileNotFoundError` if `accounts/` is missing.
- Non-numeric `TG_API_ID`: `int()` raises a bare `ValueError` that lacks the folder name in its message (the `Пропущен:` line is less clear).
- `write_env` writes before `chmod 600`, so the file briefly has default permissions.
- Sending messages: none implemented. When added, go through one outbound queue with the 10 msg/s ceiling.

## 8. Verification done
- Windows: launcher creates an account folder from project root and from `accounts/`; `login.py` usage prints; `main.py` loads and skips invalid folders.
- Commands for checks: `.venv\Scripts\python.exe -m py_compile <file>`; test account creation with piped input, then delete the test folder.
