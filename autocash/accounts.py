from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

ACCOUNTS_DIR = Path(__file__).resolve().parent.parent / "accounts"
ENV_FILE = "account.env"
TEMPLATE_DIR = "account_example"


@dataclass(frozen=True)
class Account:
    name: str
    folder: Path
    kind: str
    api_id: int
    api_hash: str
    phone: str
    bot_token: str

    @property
    def session_path(self) -> str:
        return str(self.folder / "session")


def load_accounts(root: Path = ACCOUNTS_DIR) -> list[Account]:
    accounts = []
    for folder in sorted(root.iterdir()):
        if not folder.is_dir() or folder.name.startswith(("_", ".")) or folder.name == TEMPLATE_DIR:
            continue
        env_path = folder / ENV_FILE
        if not env_path.exists():
            continue
        try:
            accounts.append(_parse(folder, dotenv_values(env_path)))
        except ValueError as exc:
            print(f"Пропущен: {exc}")
    return accounts


def find_account(folder_name: str, root: Path = ACCOUNTS_DIR) -> Account:
    for account in load_accounts(root):
        if account.name == folder_name:
            return account
    raise SystemExit(f"Аккаунт не найден или заполнен не полностью: {folder_name}")


def _parse(folder: Path, values: dict[str, str | None]) -> Account:
    kind = (values.get("TG_KIND") or "user").strip().lower()
    if kind not in ("user", "bot"):
        raise ValueError(f"{folder.name}: TG_KIND должен быть user или bot")
    required = ["TG_API_ID", "TG_API_HASH"] + (["TG_BOT_TOKEN"] if kind == "bot" else ["TG_PHONE"])
    missing = [key for key in required if not (values.get(key) or "").strip()]
    if missing:
        raise ValueError(f"{folder.name}: не заполнено в {ENV_FILE}: {', '.join(missing)}")
    return Account(
        name=folder.name,
        folder=folder,
        kind=kind,
        api_id=int(values["TG_API_ID"]),
        api_hash=values["TG_API_HASH"].strip(),
        phone=(values.get("TG_PHONE") or "").strip(),
        bot_token=(values.get("TG_BOT_TOKEN") or "").strip(),
    )
