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
    # field name -> source: "Label", "Label.subfield", "$header", "$command", "$args"
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
    warn_len: int = 5000
    warn_lag_s: float = 5.0


class ModuleCfg(Strict):
    enabled: bool = True
    concurrency: int | None = None
    timeout_s: float | None = None
    maxsize: int | None = None
    config: dict[str, Any] = {}


class OutputCfg(Strict):
    concurrency: int = 1
    min_interval_ms: int = 1000
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


class Config(Strict):
    service_chats: ServiceChatsCfg = ServiceChatsCfg()
    # user_id -> global roles (FT-12.1); source-bound roles live in sources[].senders
    users: dict[int, list[str]] = {}
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
    retention: RetentionCfg = RetentionCfg()
    sorter: SorterCfg = SorterCfg()
    # OPEN-QUESTION: FT-11.7 — time norms per stage, seconds (used from stage 1c).
    stage_norms_s: dict[str, float] = Field(
        default={"buffered": 0.2, "in_pg": 0.5, "sorted": 0.7, "module_queued": 1.0, "module_done": 10.0}
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
        for name, tpl in self.templates.items():
            ids = [r.id for r in tpl.rules]
            if len(ids) != len(set(ids)):
                raise ValueError(f"template {name}: duplicate rule ids")
        return self
