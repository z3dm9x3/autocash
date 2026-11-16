"""Pydantic schema of the YAML configuration (FT-13.1)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Policy = Literal["full", "meta", "presence", "ignore"]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PatternCfg(Strict):
    name: str
    regex: str


class SubfieldCfg(Strict):
    """Splits the value of a label into named sub-fields (FT-3.5)."""

    regex: str


class CommandsCfg(Strict):
    # Commands accepted without a leading slash (FT-12.3).
    bare: list[str] = []


class ManifestCfg(Strict):
    """Forms of features for the Extractor (FT-3.4, FT-3.7). No meaning here."""

    number_min_len: int = 4
    number_max_len: int = 20
    amount_regex: str = r"\d{1,3}(?: \d{3})+[.,]\d{2}|\d+[.,]\d{2}"
    amount_int_regex: str = r"\d{1,3}(?: \d{3})+"
    phone_regex: str = r"\+\d{10,15}"
    card_regex: str = r"\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}"
    username_regex: str = r"@[A-Za-z][A-Za-z0-9_]{3,31}"
    url_regex: str = r"https?://[^\s<>()\[\]]+|t\.me/[^\s<>()\[\]]+"
    header_regex: str = r"^\s*\[([^\]\n]{1,80})\]"
    pair_regex: str = r"(?m)^[ \t]*([^:\n]{1,40}?)[ \t]*:[ \t]*(.*?)[ \t]*$"
    patterns: list[PatternCfg] = []
    dictionaries: dict[str, list[str]] = {}
    markers: list[str] = ["✅", "❌", "🔴", "🟢", "🟡", "⚠️"]
    subfields: dict[str, SubfieldCfg] = {}
    commands: CommandsCfg = CommandsCfg()


class WhenCfg(Strict):
    """Conditions of a sorting rule (FT-6.1). All given conditions must hold."""

    source_role: list[str] = []
    sender_role: list[str] = []
    origin: list[str] = []
    event: list[Literal["new", "edit"]] = []
    header: list[str] = []
    header_regex: str | None = None
    words_all: list[str] = []
    words_any: list[str] = []
    labels_all: list[str] = []
    labels_any: list[str] = []
    markers_any: list[str] = []
    has_all: list[str] = []
    command: list[str] = []
    text_regex: str | None = None


class RuleCfg(Strict):
    id: str
    type: str
    enabled: bool = True
    when: WhenCfg = WhenCfg()
    # field name -> source: "Label", "Label.subfield", "$header", "$command", "$args", "$markers"
    fields: dict[str, str] = {}
    # Field names forming the ordering key; falls back to the chat (FT-6.2).
    order_key: list[str] = []
    priority: int = 5
    text_ttl_days: int | None = None


class TemplateCfg(Strict):
    rules: list[RuleCfg] = []


class SourceCfg(Strict):
    chat_id: int
    name: str = ""
    roles: list[str] = []
    policy: Policy | None = None
    template: str | None = None
    priority: int = 5
    # user_id -> sender role inside this source (cashier, kassa bot, ...)
    senders: dict[int, str] = {}
    disabled_rules: list[str] = []
    rule_overrides: dict[str, dict[str, Any]] = {}
    extra_rules: list[RuleCfg] = []


class DefaultsCfg(Strict):
    unknown_chat_policy: Policy = "meta"
    # Private dialogs (players writing to a pool account) are not listed as sources one by one:
    # they share this policy, the role "dm" and, optionally, a template (docs/decisions.md, D-1b-6).
    private_chat_policy: Policy = "full"
    private_template: str | None = None
    policy_by_role: dict[str, Policy] = {"channel": "presence", "logs": "ignore", "debug": "ignore"}


class ServiceChatsCfg(Strict):
    logs: int | None = None
    debug: int | None = None


class BufferCfg(Strict):
    journal_mode: str = "WAL"
    synchronous: str = "FULL"
    retention_hours: float = 72  # OPEN-QUESTION: FT-4.3
    soft_limit_mb: int = 512
    hard_limit_mb: int = 1024
    # Sources with priority <= this value are still accepted at the hard limit (FT-4.6).
    top_priority: int = 1


class UploaderCfg(Strict):
    batch_interval_ms: int = 100
    batch_max: int = 500
    retry_max_s: float = 5.0


class WriterCfg(Strict):
    flush_interval_ms: int = 20
    batch_max: int = 500


class QueueCfg(Strict):
    maxsize: int = 10000
    live_threshold: int = 100
    backfill_rate_per_s: float = 50
    # Events of one module that may wait for their commit at the same time (A-4).
    commit_window: int = 500
    warn_len: int = 5000
    warn_lag_s: float = 5.0


class ModuleCfg(Strict):
    enabled: bool = True
    concurrency: int | None = None
    timeout_s: float | None = None
    maxsize: int | None = None
    config: dict[str, Any] = {}


class PacingCfg(Strict):
    """Send tempo of one account (FT-9.7). Zero switches a limit off."""

    per_chat_interval_s: float
    per_account_interval_s: float
    group_per_minute: int


class PoolCfg(Strict):
    # Directory with encrypted session files; a file appearing or vanishing adds / removes an account.
    sessions_dir: str | None = None
    scan_interval_s: float = 2.0
    api_concurrency: int = Field(default=1, ge=1, le=2)  # FT-10.1
    # Active API calls per second by account kind. Telegram publishes no limits for user
    # accounts, so the default is deliberately low (docs/decisions.md, D-1b-4).
    api_rate_per_s: dict[str, float] = {"user": 1.0, "bot": 10.0}
    backfill_share: float = Field(default=0.5, gt=0, le=1)  # backfill runs this much slower
    flood_slowdown: float = 2.0
    flood_recovery_s: float = 600
    reconnect_s: float = 2.0
    rebalance_s: float = 5.0  # OPEN-QUESTION: FT-1.7 — new duties take effect within N seconds


class CoverageCfg(Strict):
    # OPEN-QUESTION: FT-1.5 — required number of pool accounts in a chat, by source role.
    required_by_role: dict[str, int] = {"kassa": 2}
    default: int = 1


class PollCfg(Strict):
    chats_per_account: int = 10  # FT-2.5
    silence_norm_s: float = 600  # a chat silent for longer is polled first
    difference_limit: int = 100
    idle_interval_s: float = 30  # used when the server names no timeout


class OutputCfg(Strict):
    pacing: dict[str, PacingCfg] = {
        # Bots: published guidance - ~1 msg/s per chat, 20 per minute to a group, ~30 msg/s overall.
        "bot": PacingCfg(per_chat_interval_s=1.0, per_account_interval_s=0.04, group_per_minute=20),
        # User accounts: nothing is published; conservative (docs/decisions.md, D-1b-4).
        "user": PacingCfg(per_chat_interval_s=3.0, per_account_interval_s=3.0, group_per_minute=10),
    }
    max_attempts: int = 5
    retry_base_s: float = 2.0
    # OPEN-QUESTION: FT-9.6 — lifetime of outbound tasks by kind, seconds.
    ttl_s: dict[str, float] = {
        "command_reply": 300,
        "client_reply": 1800,
        "notify": 3600,
        "bulk": 86400,
    }


class RetentionCfg(Strict):
    default_text_ttl_days: int = 90  # OPEN-QUESTION: FT-5.3
    trace_days: int = 30  # OPEN-QUESTION: FT-11.11


class SorterCfg(Strict):
    unknown_summary_interval_s: float = 3600
    queue_batches: int = 64


class OperatorCommandsCfg(Strict):
    # Heavy reports: confirmed at once, the result is delivered when ready (FT-12.6).
    heavy: list[str] = ["metrics"]
    ack_text: str = "Команда принята, отчёт готовится и придёт отдельным сообщением."


class TraceCfg(Strict):
    stuck_check_s: float = 5.0
    # OPEN-QUESTION: FT-11.7 — this many stuck tasks on one stage raise an alarm.
    mass_stuck_threshold: int = 20
    rollup_interval_s: float = 60.0
    purge_interval_s: float = 3600.0


class ReportsCfg(Strict):
    # NFT-15: reports and read commands use their own connections with a statement timeout.
    statement_timeout_ms: int = 1000
    pool_size: int = 2


DEFAULT_USER_ROLES = ["player", "casher", "manager", "admin", "support", "dev"]


class CommandArgCfg(Strict):
    name: str
    # int: digits; ident: TGID / MelID, "tg:" or "mel:" prefix allowed (FT-12.5); word: one word;
    # text: the rest of the line.
    type: Literal["int", "ident", "word", "text"] = "text"
    optional: bool = False


class ReactionWhenCfg(Strict):
    type: list[str] = []  # message types, masks allowed ("order.*")
    command: str | None = None  # a command rule (FT-12.3)
    args: list[CommandArgCfg] = []
    timer: str | None = None  # fired by `schedule`


class ReactionRuleCfg(Strict):
    """FT-8.5: when -> if -> then, with priority, on/off and version."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    version: int = 1
    enabled: bool = True
    mode: Literal["live", "dry_run"] = "live"  # FT-8.6: dry_run plans and logs, sends nothing
    priority: int = 5
    autoreply: bool = False  # an automatic answer to a player: human pause and limits apply (FT-8.10)
    when: ReactionWhenCfg = ReactionWhenCfg()
    if_: Any = Field(default=None, alias="if")  # JSONLogic condition (docs/decisions.md D-3-1)
    then: list[dict[str, Any]] = []


class ReactionsCfg(Strict):
    autoreply_pause_min: float = 30  # OPEN-QUESTION: FT-8.10 — no automatic answer N minutes after a human one
    # NFT-12: automatic answers of user accounts are limited.
    per_dialog_per_hour: int = 3
    per_account_per_hour: int = 30
    ask_timeout_s: float = 600
    rules: list[ReactionRuleCfg] = []


class BackfillCfg(Strict):
    days: float = 30  # OPEN-QUESTION: stage 4 — how far back the history is loaded
    batch: int = Field(default=100, ge=1, le=100)  # messages per request
    interval_s: float = 1.0  # pause of a chat between its requests: the live flow comes first
    chats_at_once: int = 4
    # The next batch waits while this many events are still in the pipeline (buffer, sorter,
    # module queues): the history must not stand in front of the live flow (NFT-6, NFT-8).
    max_backlog: int = 60


class Config(Strict):
    service_chats: ServiceChatsCfg = ServiceChatsCfg()
    # user_id -> global roles (FT-12.1); source-bound roles live in sources[].senders
    users: dict[int, list[str]] = {}
    # FT-12.1, 12.2: the list of user roles, extended by configuration.
    user_roles: list[str] = DEFAULT_USER_ROLES
    # OPEN-QUESTION: FT-12.4 — user role -> source role ("*" = any) -> commands ("*" = all).
    permissions: dict[str, dict[str, list[str]]] = {
        "admin": {"*": ["*"]},
        "dev": {"*": ["*"]},
        "manager": {"*": ["status", "accounts", "coverage", "queue", "trace", "stuck", "metrics"]},
        "support": {"*": ["status", "trace"]},
    }
    commands: OperatorCommandsCfg = OperatorCommandsCfg()
    trace: TraceCfg = TraceCfg()
    reports: ReportsCfg = ReportsCfg()
    reactions: ReactionsCfg = ReactionsCfg()
    backfill: BackfillCfg = BackfillCfg()
    defaults: DefaultsCfg = DefaultsCfg()
    sources: list[SourceCfg] = []
    manifest: ManifestCfg = ManifestCfg()
    templates: dict[str, TemplateCfg] = {}
    buffer: BufferCfg = BufferCfg()
    uploader: UploaderCfg = UploaderCfg()
    writer: WriterCfg = WriterCfg()
    queue: QueueCfg = QueueCfg()
    modules: dict[str, ModuleCfg] = {}
    output: OutputCfg = OutputCfg()
    pool: PoolCfg = PoolCfg()
    coverage: CoverageCfg = CoverageCfg()
    poll: PollCfg = PollCfg()
    retention: RetentionCfg = RetentionCfg()
    sorter: SorterCfg = SorterCfg()
    # OPEN-QUESTION: FT-11.7 — norm of time a task may wait for the next stage, seconds
    # (stage names: autocash/core/trace/monitor.py). Over the norm the task is marked "stuck".
    stage_norms_s: dict[str, float] = Field(
        default={"in_pg": 2.0, "sorted": 2.0, "module_done": 30.0, "out_assigned": 60.0, "out_sending": 60.0,
                 "out_delivered": 30.0}
    )
    # NFT-19: day boundary for reports; all stored timestamps are UTC.
    report_timezone: str = "Europe/Moscow"

    @model_validator(mode="after")
    def _check_refs(self) -> Config:
        seen: set[int] = set()
        for src in self.sources:
            if src.chat_id in seen:
                raise ValueError(f"duplicate source chat_id {src.chat_id}")
            seen.add(src.chat_id)
            if src.template is not None and src.template not in self.templates:
                raise ValueError(f"source {src.chat_id}: unknown template {src.template!r}")
        if self.defaults.private_template is not None and self.defaults.private_template not in self.templates:
            raise ValueError(f"defaults.private_template: unknown template {self.defaults.private_template!r}")
        known_roles = set(self.user_roles)
        for user_id, roles in self.users.items():
            unknown_roles = set(roles) - known_roles
            if unknown_roles:
                raise ValueError(f"users.{user_id}: unknown roles {sorted(unknown_roles)} (see user_roles)")
        unknown_roles = set(self.permissions) - known_roles
        if unknown_roles:
            raise ValueError(f"permissions: unknown user roles {sorted(unknown_roles)} (see user_roles)")
        rule_ids = [r.id for r in self.reactions.rules]
        if len(rule_ids) != len(set(rule_ids)):
            raise ValueError("reactions: duplicate rule ids")
        for name, tpl in self.templates.items():
            ids = [r.id for r in tpl.rules]
            if len(ids) != len(set(ids)):
                raise ValueError(f"template {name}: duplicate rule ids")
        return self
