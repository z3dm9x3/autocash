"""Demo module: counts typed messages per source and answers /ping. Exercises the pipeline."""

from __future__ import annotations

from typing import ClassVar

from autocash.modules.api import Context, EntityDecl, Event, Module


class DemoModule(Module):
    name: ClassVar[str] = "demo"
    subscriptions: ClassVar[list[str]] = ["*"]
    entities: ClassVar[list[EntityDecl]] = [EntityDecl("demo.counter")]
    commands: ClassVar[list[str]] = ["ping"]

    async def handle(self, event: Event, ctx: Context) -> None:
        ctx.execute(
            "INSERT INTO entities (entity_type, entity_key, attrs) VALUES ('demo.counter', $1, '{\"count\": 1}') "
            "ON CONFLICT (entity_type, entity_key) DO UPDATE SET "
            "attrs = jsonb_set(entities.attrs, '{count}', to_jsonb((entities.attrs->>'count')::bigint + 1)), "
            "updated_at = now()",
            str(event.chat_id),
        )
        # Never react to our own automatic messages (FT-2.10).
        if event.msg_type == "command.ping" and event.record.origin != "system":
            ctx.reply("pong", rule="ping", kind="command_reply")
