import asyncio
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from autocash.accounts import ACCOUNTS_DIR, ENV_FILE, find_account  # noqa: E402
from autocash.modules.autosup.AS_accounts import FILE as PARAMS_FILE  # noqa: E402
from autocash.modules.autosup.AS_accounts import TEMPLATE as PARAMS_TEMPLATE  # noqa: E402
from autocash.tg_client import connect  # noqa: E402

INVALID_NAME_CHARS = re.compile(r'[\\/:*?"<>|]')


def ask(prompt: str, *, required: bool = True, default: str = "", check=None) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{prompt}{suffix}: ").strip() or default
        if not value and required:
            print("  Поле обязательно.")
            continue
        error = check(value) if (check and value) else None
        if error:
            print(f"  {error}")
            continue
        return value


def yes(prompt: str, default: bool = True) -> bool:
    answer = input(f"{prompt} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
    if not answer:
        return default
    return answer in ("y", "yes", "д", "да")


def check_digits(value: str) -> str | None:
    return None if value.isdigit() else "Нужны только цифры."


def check_name(value: str) -> str | None:
    if INVALID_NAME_CHARS.search(value):
        return 'Недопустимые символы: \\ / : * ? " < > |'
    return None


def check_phone(value: str) -> str | None:
    return None if re.fullmatch(r"\+\d{7,15}", value) else "Формат: +79991234567"


def check_token(value: str) -> str | None:
    return None if re.fullmatch(r"\d+:[\w-]+", value) else "Формат токена: 123456:ABC..."


def check_kind(value: str) -> str | None:
    return None if value in ("user", "bot") else "Введите user или bot."


def quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def write_env(folder: Path, values: dict[str, str]) -> None:
    path = folder / ENV_FILE
    text = "".join(f"{key}={quote(value)}\n" for key, value in values.items())
    path.write_text(text, encoding="utf-8")
    os.chmod(path, 0o600)


async def check_connection(folder_name: str) -> None:
    account = find_account(folder_name)
    client = await connect(account, interactive=True)
    me = await client.get_me()
    print(f"Вход выполнен, id {me.id}")
    await client.disconnect()


def main() -> int:
    print("Добавление аккаунта в пул. Ctrl+C — отмена.\n")
    kind = ask("Тип: user или bot", default="user", check=check_kind)
    tg_id = ask("Telegram ID (цифры, Enter — пропустить)", required=False, check=check_digits)
    name = ask("Имя аккаунта", check=check_name)
    folder_name = f"acc_{tg_id} {name}" if tg_id else name
    folder = ACCOUNTS_DIR / folder_name
    if folder.exists():
        print(f"Папка уже существует: {folder_name}")
        return 1

    api_id = ask("API ID (my.telegram.org)", check=check_digits)
    api_hash = ask("API HASH (my.telegram.org)")
    values = {"TG_KIND": kind, "TG_API_ID": api_id, "TG_API_HASH": api_hash, "TG_PHONE": "", "TG_BOT_TOKEN": ""}
    if kind == "user":
        values["TG_PHONE"] = ask("Телефон (+79991234567)", check=check_phone)
    else:
        values["TG_BOT_TOKEN"] = ask("Токен бота (от @BotFather)", check=check_token)

    print(f"\nПапка: accounts/{folder_name}")
    print(f"Тип: {kind}, API ID: {api_id}, API HASH: задан" + (f", телефон: {values['TG_PHONE']}" if kind == "user" else ", токен: задан"))
    if not yes("Создать аккаунт?"):
        print("Отменено.")
        return 0

    folder.mkdir(parents=True)
    write_env(folder, values)
    print(f"Создано: accounts/{folder_name}/{ENV_FILE}")
    # Parameters of the account (Autosup: working hours, D-AS-8); read when the account joins the pool.
    (folder / PARAMS_FILE).write_text(PARAMS_TEMPLATE, encoding="utf-8")
    print(f"Создано: accounts/{folder_name}/{PARAMS_FILE} — часы работы по умолчанию: ежедневно 11:00–23:00")

    if yes("Проверить вход сейчас? (код придёт в Telegram)"):
        try:
            asyncio.run(check_connection(folder_name))
        except Exception as exc:
            print(f"Вход не выполнен: {exc}. Повторить: python accounts/login.py \"{folder_name}\"")
            return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyboardInterrupt, EOFError):
        print("\nОтменено.")
        raise SystemExit(130)
