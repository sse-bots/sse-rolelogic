from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

from activation import setup_activation, _configure_srv_dns
from help_menu import setup_help
from heartbeat_client import start_heartbeat
from keep_alive import keep_alive

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("sse-rolelogic")

MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
DATABASE_NAME = os.getenv("DATABASE", "sse_rolelogic").strip() or "sse_rolelogic"
TOKEN = os.getenv("DISCORD_TOKEN", "").strip()

intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # Enable Server Members Intent in the Discord Developer Portal.


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RoleLogicStore:
    """MongoDB persistence for logic definitions and roles added by this bot."""

    def __init__(self, client: MongoClient):
        self.db = client[DATABASE_NAME]
        self.logics = self.db.role_logics
        self.managed = self.db.rolelogic_managed_assignments

    def initialize(self) -> None:
        self.logics.create_index(
            [("guild_id", ASCENDING), ("name_key", ASCENDING)], unique=True
        )
        self.logics.create_index([("guild_id", ASCENDING), ("enabled", ASCENDING)])
        self.managed.create_index(
            [("guild_id", ASCENDING), ("member_id", ASCENDING), ("role_id", ASCENDING)],
            unique=True,
        )

    def create_logic(self, document: dict[str, Any]) -> None:
        self.logics.insert_one(document)

    def get_logic(self, guild_id: int, name: str) -> dict[str, Any] | None:
        return self.logics.find_one(
            {"guild_id": str(guild_id), "name_key": name.casefold().strip()}
        )

    def list_logics(self, guild_id: int, *, enabled: bool | None = None) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"guild_id": str(guild_id)}
        if enabled is not None:
            query["enabled"] = enabled
        return list(self.logics.find(query).sort("created_at", ASCENDING))

    def update_logic(self, logic_id: str, guild_id: int, changes: dict[str, Any]) -> bool:
        result = self.logics.update_one(
            {"_id": logic_id, "guild_id": str(guild_id)},
            {"$set": {**changes, "updated_at": utcnow()}},
        )
        return result.modified_count > 0

    def delete_logic(self, guild_id: int, name: str) -> dict[str, Any] | None:
        return self.logics.find_one_and_delete(
            {"guild_id": str(guild_id), "name_key": name.casefold().strip()}
        )

    def get_member_managed(self, guild_id: int, member_id: int) -> list[dict[str, Any]]:
        return list(self.managed.find({
            "guild_id": str(guild_id),
            "member_id": str(member_id),
        }))

    def mark_managed(self, guild_id: int, member_id: int, role_id: int) -> None:
        self.managed.update_one(
            {
                "guild_id": str(guild_id),
                "member_id": str(member_id),
                "role_id": str(role_id),
            },
            {"$setOnInsert": {"added_at": utcnow()}},
            upsert=True,
        )

    def unmark_managed(self, guild_id: int, member_id: int, role_id: int) -> None:
        self.managed.delete_one({
            "guild_id": str(guild_id),
            "member_id": str(member_id),
            "role_id": str(role_id),
        })


class RoleLogicEngine:
    def __init__(self, bot: "RoleLogicBot", store: RoleLogicStore):
        self.bot = bot
        self.store = store
        self._guild_locks: dict[int, asyncio.Lock] = {}

    def _lock_for(self, guild_id: int) -> asyncio.Lock:
        return self._guild_locks.setdefault(guild_id, asyncio.Lock())

    @staticmethod
    def _matches(member: discord.Member, logic: dict[str, Any]) -> bool:
        role_ids = {int(value) for value in logic.get("condition_role_ids", [])}
        member_role_ids = {role.id for role in member.roles}
        if not role_ids:
            return False
        if logic.get("operator") == "AND":
            return role_ids.issubset(member_role_ids)
        return bool(role_ids.intersection(member_role_ids))

    async def reconcile_member(self, member: discord.Member) -> tuple[int, int]:
        """Apply all active logics; remove only roles this bot previously added."""
        guild = member.guild
        async with self._lock_for(guild.id):
            logics = await asyncio.to_thread(self.store.list_logics, guild.id)
            desired: set[int] = set()
            paused_targets: set[int] = set()
            for logic in logics:
                target_id = int(logic["target_role_id"])
                if logic.get("enabled", True):
                    if self._matches(member, logic):
                        desired.add(target_id)
                else:
                    paused_targets.add(target_id)

            managed_docs = await asyncio.to_thread(
                self.store.get_member_managed, guild.id, member.id
            )
            managed_ids = {int(item["role_id"]) for item in managed_docs}
            current_ids = {role.id for role in member.roles}
            # Pausing a logic freezes its existing bot-managed assignment. It
            # will be evaluated again when the logic is resumed or deleted.
            preserve_while_paused = paused_targets & managed_ids & current_ids
            added = removed = 0

            for role_id in sorted(desired):
                if role_id in current_ids:
                    continue
                role = guild.get_role(role_id)
                if role is None or role.is_default() or role.managed:
                    continue
                if guild.me is None or role >= guild.me.top_role:
                    log.warning(
                        "Cannot assign role %s in guild %s: role hierarchy",
                        role_id, guild.id,
                    )
                    continue
                try:
                    await member.add_roles(role, reason="SSE RoleLogic automatic assignment")
                    await asyncio.to_thread(
                        self.store.mark_managed, guild.id, member.id, role_id
                    )
                    current_ids.add(role_id)
                    managed_ids.add(role_id)
                    added += 1
                    log.info(
                        "Assigned role %s to member %s in guild %s",
                        role_id, member.id, guild.id,
                    )
                except (discord.Forbidden, discord.HTTPException):
                    log.exception("Could not assign role %s to member %s", role_id, member.id)

            # Only remove roles that RoleLogic itself added and that no enabled
            # logic currently requires.
            for role_id in sorted(managed_ids - desired - preserve_while_paused):
                role = guild.get_role(role_id)
                try:
                    if role is not None and role_id in current_ids:
                        if guild.me is None or role >= guild.me.top_role:
                            log.warning("Cannot remove managed role %s due to role hierarchy", role_id)
                        else:
                            await member.remove_roles(
                                role, reason="SSE RoleLogic condition no longer matches"
                            )
                            removed += 1
                    await asyncio.to_thread(
                        self.store.unmark_managed, guild.id, member.id, role_id
                    )
                    log.info(
                        "Removed managed role %s from member %s in guild %s",
                        role_id, member.id, guild.id,
                    )
                except (discord.Forbidden, discord.HTTPException):
                    log.exception("Could not remove role %s from member %s", role_id, member.id)

            return added, removed

    async def reconcile_guild(self, guild: discord.Guild) -> tuple[int, int, int]:
        """Chunk the guild, then reconcile each member. Returns scanned/added/removed."""
        if not guild.chunked:
            await guild.chunk(cache=True)
        scanned = added = removed = 0
        for member in list(guild.members):
            try:
                a, r = await self.reconcile_member(member)
                scanned += 1
                added += a
                removed += r
            except Exception:
                log.exception("Member reconciliation failed for %s in guild %s", member.id, guild.id)
        log.info(
            "Guild reconciliation finished: guild=%s scanned=%s added=%s removed=%s",
            guild.id, scanned, added, removed,
        )
        return scanned, added, removed

    async def reconcile_all(self) -> None:
        for guild in list(self.bot.guilds):
            try:
                await self.reconcile_guild(guild)
            except Exception:
                log.exception("Guild reconciliation failed for %s", guild.id)



class LogicWizard:
    """Single-message, user-bound RoleLogic editor."""

    PAGE_SIZE = 25
    TIMEOUT = 600

    def __init__(
        self,
        bot: "RoleLogicBot",
        guild: discord.Guild,
        owner_id: int,
        name: str,
        existing: dict[str, Any] | None = None,
    ):
        self.bot = bot
        self.guild = guild
        self.owner_id = owner_id
        self.existing = existing
        self.name = name.strip()
        self.target_role_id: int | None = (
            int(existing["target_role_id"]) if existing else None
        )
        self.operator = existing.get("operator", "OR") if existing else "OR"
        self.condition_role_ids: set[int] = (
            {int(value) for value in existing.get("condition_role_ids", [])}
            if existing else set()
        )
        self.step = 0
        self.target_page = 0
        self.condition_page = 0
        self.message: discord.WebhookMessage | None = None
        self.active_view: LogicWizardView | None = None
        self.completed = False
        self.expires_at = time.monotonic() + 14 * 60

    def _eligible_roles(self, *, target: bool = False) -> list[discord.Role]:
        roles = [
            role for role in self.guild.roles
            if not role.is_default() and not role.managed
        ]
        if target:
            me = self.guild.me
            if me is None:
                return []
            roles = [role for role in roles if role < me.top_role]
        return sorted(roles, key=lambda role: (role.position, role.name.casefold()))

    def _page_roles(self, *, target: bool = False) -> list[discord.Role]:
        roles = self._eligible_roles(target=target)
        page = self.target_page if target else self.condition_page
        pages = max(1, (len(roles) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        if target:
            self.target_page = page
        else:
            self.condition_page = page
        return roles[page * self.PAGE_SIZE:(page + 1) * self.PAGE_SIZE]

    @staticmethod
    def _role_label(role: discord.Role) -> str:
        return discord.utils.escape_markdown(role.name)[:100]

    def _embed(self) -> discord.Embed:
        target = self.guild.get_role(self.target_role_id or 0)
        conditions = [
            self.guild.get_role(role_id)
            for role_id in sorted(self.condition_role_ids)
        ]
        conditions = [role for role in conditions if role is not None]

        embed = discord.Embed(
            title=f"{'Edit' if self.existing else 'Create'} RoleLogic: {self.name}",
            color=discord.Color.blurple(),
        )
        if self.step == 0:
            roles = self._eligible_roles(target=True)
            pages = max(1, (len(roles) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
            embed.description = (
                "**Step 1/4 — Target role**\n"
                "Choose the role the bot should assign automatically.\n\n"
                f"Currently selected: {target.mention if target else 'None'}\n"
                f"Available assignable roles: {len(roles)}"
            )
            embed.set_footer(text=f"Role page {self.target_page + 1}/{pages}")
        elif self.step == 1:
            embed.description = (
                "**Step 2/4 — Condition type**\n"
                "Choose how condition roles are evaluated.\n\n"
                "**OR:** at least one selected role is required.\n"
                "**AND:** every selected role is required.\n\n"
                f"Current selection: **{self.operator}**"
            )
        elif self.step == 2:
            roles = self._eligible_roles()
            pages = max(1, (len(roles) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
            selected = [
                self.guild.get_role(role_id)
                for role_id in sorted(self.condition_role_ids)
            ]
            selected = [role for role in selected if role is not None]
            selected_text = ", ".join(role.mention for role in selected) or "None"
            # Keep the ephemeral embed within Discord's 4096-character description limit.
            if len(selected_text) > 2500:
                selected_text = selected_text[:2450] + "…"
            embed.description = (
                "**Step 3/4 — Condition roles**\n"
                "Use the role menu to add or remove one role at a time. "
                "Change pages without losing your selections.\n\n"
                f"**Selected ({len(self.condition_role_ids)}):** {selected_text}\n\n"
                f"**Rule:** {self.operator} · **Available roles:** {len(roles)}"
            )
            embed.set_footer(text=f"Role page {self.condition_page + 1}/{pages}")
        else:
            selected = [
                self.guild.get_role(role_id)
                for role_id in sorted(self.condition_role_ids)
            ]
            selected = [role for role in selected if role is not None]
            selected_text = ", ".join(role.mention for role in selected) or "None"
            if len(selected_text) > 2500:
                selected_text = selected_text[:2450] + "…"
            embed.title = "Review RoleLogic"
            embed.description = (
                f"**Name:** {discord.utils.escape_markdown(self.name)}\n"
                f"**Assign role:** {target.mention if target else 'Unavailable'}\n"
                f"**Condition:** **{self.operator}**\n"
                f"**Selected condition roles ({len(self.condition_role_ids)}):** "
                f"{selected_text}\n\n"
                "Press **Save** to apply this logic, or **Back** to make changes."
            )
        return embed

    def build_view(self) -> "LogicWizardView":
        if self.active_view is not None:
            self.active_view.stop()
        self.active_view = LogicWizardView(self)
        return self.active_view

    async def start(self, interaction: discord.Interaction) -> None:
        view = self.build_view()
        await interaction.response.send_message(
            embed=self._embed(),
            view=view,
            ephemeral=True,
        )
        try:
            self.message = await interaction.original_response()
        except discord.HTTPException:
            log.warning("Could not cache RoleLogic wizard message for user %s", self.owner_id)

    async def refresh(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            embed=self._embed(),
            view=self.build_view(),
        )

    async def _show_error(self, interaction: discord.Interaction, message: str) -> None:
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="Cannot save RoleLogic",
                description=message + "\n\nYour selections are still available; correct the issue and try again.",
                color=discord.Color.orange(),
            ),
            view=self.build_view(),
        )

    async def save(self, interaction: discord.Interaction) -> None:
        if not self.name:
            return await self._show_error(interaction, "A logic name is required.")

        name_key = self.name.casefold().strip()
        existing_same_name = await asyncio.to_thread(
            self.bot.store.get_logic, self.guild.id, self.name
        )
        if existing_same_name and (
            not self.existing or existing_same_name["_id"] != self.existing["_id"]
        ):
            return await self._show_error(
                interaction, "A logic with that name already exists in this server."
            )

        target = self.guild.get_role(int(self.target_role_id or 0))
        if target is None or target.is_default() or target.managed:
            return await self._show_error(
                interaction, "That target role is no longer available."
            )
        if self.guild.me is None or target >= self.guild.me.top_role:
            return await self._show_error(
                interaction,
                "I cannot manage that role. Move my bot role above the target role and try again.",
            )
        if not self.condition_role_ids:
            return await self._show_error(
                interaction, "Select at least one condition role before saving."
            )

        # Drop roles that were deleted while the wizard was open.
        valid_condition_ids = {
            role_id for role_id in self.condition_role_ids
            if (role := self.guild.get_role(role_id)) is not None
            and not role.is_default()
            and not role.managed
        }
        if not valid_condition_ids:
            return await self._show_error(
                interaction, "None of the selected condition roles are available anymore."
            )

        now = utcnow()
        document = {
            "_id": self.existing["_id"] if self.existing else uuid.uuid4().hex,
            "guild_id": str(self.guild.id),
            "name": self.name,
            "name_key": name_key,
            "target_role_id": int(self.target_role_id),
            "operator": self.operator,
            "condition_role_ids": sorted(valid_condition_ids),
            "enabled": self.existing.get("enabled", True) if self.existing else True,
            "created_by": (
                self.existing.get("created_by", str(self.owner_id))
                if self.existing else str(self.owner_id)
            ),
            "created_at": self.existing.get("created_at", now) if self.existing else now,
            "updated_at": now,
        }
        try:
            if self.existing:
                update_fields = {key: value for key, value in document.items() if key != "_id"}
                result = await asyncio.to_thread(
                    self.bot.store.logics.update_one,
                    {"_id": self.existing["_id"], "guild_id": str(self.guild.id)},
                    {"$set": update_fields},
                )
                if result.matched_count == 0:
                    return await self._show_error(
                        interaction,
                        "This logic no longer exists. Reopen the editor and try again.",
                    )
            else:
                await asyncio.to_thread(self.bot.store.create_logic, document)
        except DuplicateKeyError:
            return await self._show_error(
                interaction, "A logic with that name already exists."
            )
        except PyMongoError:
            log.exception("Could not save RoleLogic")
            return await self._show_error(
                interaction, "Database error while saving this logic. Please try again."
            )

        self.completed = True
        self.condition_role_ids = valid_condition_ids
        success = discord.Embed(
            title="RoleLogic saved",
            description=(
                f"**{discord.utils.escape_markdown(self.name)}** was "
                f"{'updated' if self.existing else 'created'} successfully.\n\n"
                f"**Assign:** {target.mention}\n"
                f"**Condition:** {'ALL' if self.operator == 'AND' else 'ANY'} of "
                f"{len(valid_condition_ids)} selected roles"
            ),
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=success, content=None, view=None)
        await self.bot.engine.reconcile_guild(self.guild)

    async def cancel(self, interaction: discord.Interaction) -> None:
        self.completed = True
        await interaction.response.edit_message(
            content="RoleLogic setup cancelled.",
            embed=None,
            view=None,
        )


class WizardRoleSelect(discord.ui.Select):
    def __init__(self, wizard: LogicWizard):
        self.wizard = wizard
        is_target = wizard.step == 0
        roles = wizard._page_roles(target=is_target)
        selected_ids = (
            {wizard.target_role_id} if is_target and wizard.target_role_id else
            wizard.condition_role_ids if not is_target else set()
        )
        options = [
            discord.SelectOption(
                label=wizard._role_label(role),
                value=str(role.id),
                description=("Selected" if role.id in selected_ids else "Click to select"),
                default=role.id in selected_ids,
            )
            for role in roles
        ]
        super().__init__(
            placeholder=(
                "Choose the target role"
                if is_target else "Toggle a condition role on/off"
            ),
            min_values=1,
            max_values=1,
            options=options or [
                discord.SelectOption(label="No eligible roles available", value="none")
            ],
            disabled=not bool(roles),
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.wizard.owner_id:
            return await interaction.response.send_message(
                "This setup belongs to another administrator.", ephemeral=True
            )
        if self.wizard.completed:
            return await interaction.response.send_message(
                "This wizard has already finished.", ephemeral=True
            )
        value = self.values[0]
        if value == "none":
            return await interaction.response.defer()
        role_id = int(value)
        if self.wizard.step == 0:
            self.wizard.target_role_id = role_id
        elif self.wizard.step == 2:
            if role_id in self.wizard.condition_role_ids:
                self.wizard.condition_role_ids.remove(role_id)
            else:
                self.wizard.condition_role_ids.add(role_id)
        await self.wizard.refresh(interaction)


class WizardOperatorSelect(discord.ui.Select):
    def __init__(self, wizard: LogicWizard):
        self.wizard = wizard
        super().__init__(
            placeholder="Choose OR or AND",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label="OR — Any selected role",
                    value="OR",
                    description="At least one condition role is required",
                    default=wizard.operator == "OR",
                ),
                discord.SelectOption(
                    label="AND — All selected roles",
                    value="AND",
                    description="Every condition role is required",
                    default=wizard.operator == "AND",
                ),
            ],
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.wizard.owner_id:
            return await interaction.response.send_message(
                "This setup belongs to another administrator.", ephemeral=True
            )
        self.wizard.operator = self.values[0]
        await self.wizard.refresh(interaction)


class WizardButton(discord.ui.Button):
    def __init__(self, wizard: LogicWizard, action: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.wizard = wizard
        self.action = action

    async def callback(self, interaction: discord.Interaction) -> None:
        wizard = self.wizard
        if interaction.user.id != wizard.owner_id:
            return await interaction.response.send_message(
                "This setup belongs to another administrator.", ephemeral=True
            )
        if wizard.completed:
            return await interaction.response.send_message(
                "This wizard has already finished.", ephemeral=True
            )

        if self.action == "target_prev":
            wizard.target_page = max(0, wizard.target_page - 1)
        elif self.action == "target_next":
            roles = wizard._eligible_roles(target=True)
            pages = max(1, (len(roles) + wizard.PAGE_SIZE - 1) // wizard.PAGE_SIZE)
            wizard.target_page = min(pages - 1, wizard.target_page + 1)
        elif self.action == "condition_prev":
            wizard.condition_page = max(0, wizard.condition_page - 1)
        elif self.action == "condition_next":
            roles = wizard._eligible_roles()
            pages = max(1, (len(roles) + wizard.PAGE_SIZE - 1) // wizard.PAGE_SIZE)
            wizard.condition_page = min(pages - 1, wizard.condition_page + 1)
        elif self.action == "back":
            wizard.step = max(0, wizard.step - 1)
        elif self.action == "next":
            if wizard.step == 0 and not wizard.target_role_id:
                return await interaction.response.send_message(
                    "Choose a target role first.", ephemeral=True
                )
            wizard.step = min(3, wizard.step + 1)
        elif self.action == "review":
            if not wizard.condition_role_ids:
                return await interaction.response.send_message(
                    "Select at least one condition role before reviewing.",
                    ephemeral=True,
                )
            wizard.step = 3
        elif self.action == "save":
            return await wizard.save(interaction)
        elif self.action == "cancel":
            return await wizard.cancel(interaction)
        await wizard.refresh(interaction)


class LogicWizardView(discord.ui.View):
    def __init__(self, wizard: LogicWizard):
        super().__init__(timeout=max(1.0, wizard.expires_at - time.monotonic()))
        self.wizard = wizard

        if wizard.step in (0, 2):
            self.add_item(WizardRoleSelect(wizard))
        elif wizard.step == 1:
            self.add_item(WizardOperatorSelect(wizard))

        if wizard.step == 0:
            roles = wizard._eligible_roles(target=True)
            pages = max(1, (len(roles) + wizard.PAGE_SIZE - 1) // wizard.PAGE_SIZE)
            self.add_item(WizardButton(
                wizard, "target_prev", label="Previous roles",
                style=discord.ButtonStyle.secondary, row=1,
                disabled=wizard.target_page <= 0,
            ))
            self.add_item(WizardButton(
                wizard, "target_next", label="More roles",
                style=discord.ButtonStyle.secondary, row=1,
                disabled=wizard.target_page >= pages - 1,
            ))
        elif wizard.step == 2:
            roles = wizard._eligible_roles()
            pages = max(1, (len(roles) + wizard.PAGE_SIZE - 1) // wizard.PAGE_SIZE)
            self.add_item(WizardButton(
                wizard, "condition_prev", label="Previous roles",
                style=discord.ButtonStyle.secondary, row=1,
                disabled=wizard.condition_page <= 0,
            ))
            self.add_item(WizardButton(
                wizard, "condition_next", label="More roles",
                style=discord.ButtonStyle.secondary, row=1,
                disabled=wizard.condition_page >= pages - 1,
            ))

        if wizard.step > 0:
            self.add_item(WizardButton(
                wizard, "back", label="Back",
                style=discord.ButtonStyle.secondary, row=2,
            ))
        if wizard.step in (0, 1):
            self.add_item(WizardButton(
                wizard, "next", label="Next",
                style=discord.ButtonStyle.primary, row=2,
                disabled=(wizard.step == 0 and wizard.target_role_id is None),
            ))
        elif wizard.step == 2:
            self.add_item(WizardButton(
                wizard, "review", label="Review",
                style=discord.ButtonStyle.primary, row=2,
                disabled=not bool(wizard.condition_role_ids),
            ))
        elif wizard.step == 3:
            self.add_item(WizardButton(
                wizard, "save", label="Save",
                style=discord.ButtonStyle.success, emoji="💾", row=2,
            ))

        self.add_item(WizardButton(
            wizard, "cancel", label="Cancel",
            style=discord.ButtonStyle.danger, row=2,
        ))

    async def on_timeout(self) -> None:
        if self.wizard.completed:
            return
        self.wizard.completed = True
        for item in self.children:
            item.disabled = True
        try:
            if self.wizard.message:
                await self.wizard.message.edit(
                    content="This RoleLogic setup expired. Run the command again to restart.",
                    embed=None,
                    view=self,
                )
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            log.debug("Could not update expired RoleLogic wizard message")


logic_group = app_commands.Group(name="logic", description="Create and manage automatic role-assignment logics")


def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise app_commands.CheckFailure("This command can only be used inside a server.")
        if not interaction.user.guild_permissions.manage_roles and interaction.user.id != interaction.guild.owner_id:
            raise app_commands.CheckFailure("You need the Manage Roles permission to use RoleLogic.")
        return True
    return app_commands.check(predicate)


class RoleLogicBot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix=commands.when_mentioned,
            intents=intents,
            help_command=None,
        )
        self.mongo: MongoClient | None = None
        self.store: RoleLogicStore | None = None
        self.engine: RoleLogicEngine | None = None
        self._heartbeat_started = False

    async def setup_hook(self) -> None:
        if not TOKEN:
            raise RuntimeError("DISCORD_TOKEN is missing from .env")
        if not MONGODB_URI:
            raise RuntimeError("MONGODB_URI is missing from .env")

        _configure_srv_dns(MONGODB_URI)
        self.mongo = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=15000,
            connectTimeoutMS=10000,
            appname="SSE-RoleLogic",
        )
        await asyncio.to_thread(self.mongo.admin.command, "ping")
        self.store = RoleLogicStore(self.mongo)
        await asyncio.to_thread(self.store.initialize)
        self.engine = RoleLogicEngine(self, self.store)

        self.tree.add_command(logic_group)
        setup_help(self)

        # setup_activation owns global/guild command synchronization.
        await setup_activation(self)
        log.info("RoleLogic database and SSE activation initialized.")

    async def on_ready(self) -> None:
        if not self._heartbeat_started:
            start_heartbeat(self)
            self._heartbeat_started = True
        log.info("Logged in as %s (%s) | Guilds: %s", self.user, self.user.id, len(self.guilds))
        if not getattr(self, "_startup_reconciled", False):
            self._startup_reconciled = True
            await self.engine.reconcile_all()
        if not daily_reconciliation.is_running():
            daily_reconciliation.start()

    async def on_member_join(self, member: discord.Member) -> None:
        if self.engine:
            await self.engine.reconcile_member(member)

    async def on_member_update(self, before: discord.Member, after: discord.Member) -> None:
        if self.engine and {role.id for role in before.roles} != {role.id for role in after.roles}:
            await self.engine.reconcile_member(after)

    async def on_guild_role_delete(self, role: discord.Role) -> None:
        if not self.store:
            return
        await asyncio.to_thread(
            self.store.logics.update_many,
            {"guild_id": str(role.guild.id)},
            {"$pull": {"condition_role_ids": role.id}},
        )
        await asyncio.to_thread(
            self.store.logics.delete_many,
            {"guild_id": str(role.guild.id), "target_role_id": role.id},
        )
        await asyncio.to_thread(
            self.store.managed.delete_many,
            {"guild_id": str(role.guild.id), "role_id": role.id},
        )
        log.info("Cleaned up RoleLogic references for deleted role %s", role.id)

    async def close(self) -> None:
        if self.mongo:
            self.mongo.close()
        await super().close()


bot = RoleLogicBot()



async def _logic_name_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    if interaction.guild is None or bot.store is None:
        return []
    try:
        documents = await asyncio.to_thread(bot.store.list_logics, interaction.guild.id)
    except PyMongoError:
        log.exception("Could not load logic names for autocomplete")
        return []
    query = (current or "").casefold().strip()
    matches = [
        doc for doc in documents
        if query in str(doc.get("name", "")).casefold()
    ]
    return [
        app_commands.Choice(name=str(doc["name"])[:100], value=str(doc["name"])[:100])
        for doc in matches[:25]
    ]


@logic_group.command(name="create", description="Create a new automatic role-assignment logic.")
@app_commands.describe(name="A unique name for this logic.")
@admin_only()
async def logic_create(interaction: discord.Interaction, name: str) -> None:
    name = name.strip()
    if not name or len(name) > 70:
        return await interaction.response.send_message(
            "Logic names must contain between 1 and 70 characters.",
            ephemeral=True,
        )
    if await asyncio.to_thread(bot.store.get_logic, interaction.guild.id, name):
        return await interaction.response.send_message(
            "A logic with that name already exists in this server.",
            ephemeral=True,
        )
    wizard = LogicWizard(bot, interaction.guild, interaction.user.id, name)
    await wizard.start(interaction)


@logic_create.autocomplete("name")
async def logic_create_name_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    return await _logic_name_autocomplete(interaction, current)


@logic_group.command(name="edit", description="Edit an existing role-assignment logic.")
@app_commands.describe(name="Choose the logic you want to edit.")
@admin_only()
async def logic_edit(interaction: discord.Interaction, name: str) -> None:
    document = await asyncio.to_thread(bot.store.get_logic, interaction.guild.id, name)
    if not document:
        return await interaction.response.send_message(
            "I couldn't find a logic with that name.", ephemeral=True
        )
    wizard = LogicWizard(
        bot, interaction.guild, interaction.user.id,
        document["name"], existing=document,
    )
    await wizard.start(interaction)


@logic_edit.autocomplete("name")
async def logic_edit_name_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    return await _logic_name_autocomplete(interaction, current)

@logic_group.command(name="list", description="Show all RoleLogic rules in this server.")
@admin_only()
async def logic_list(interaction: discord.Interaction) -> None:
    documents = await asyncio.to_thread(bot.store.list_logics, interaction.guild.id)
    if not documents:
        return await interaction.response.send_message("No RoleLogic rules have been created in this server yet.", ephemeral=True)
    embed = discord.Embed(title="SSE RoleLogic — Server Rules", color=discord.Color.blurple())
    for doc in documents[:25]:
        target = interaction.guild.get_role(int(doc["target_role_id"]))
        conditions = [interaction.guild.get_role(int(value)) for value in doc.get("condition_role_ids", [])]
        condition_text = ", ".join(role.mention for role in conditions if role) or "Missing/deleted roles"
        embed.add_field(
            name=f"{'🟢' if doc.get('enabled', True) else '⏸️'} {doc['name']}"[:256],
            value=(
                f"**Assign:** {target.mention if target else '`Missing role`'}\n"
                f"**{doc.get('operator', 'OR')}:** {condition_text}"
            )[:1024],
            inline=False,
        )
    if len(documents) > 25:
        embed.set_footer(text=f"Showing 25 of {len(documents)} logics. Use /logic list again after removing older rules.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@logic_group.command(name="delete", description="Delete a RoleLogic rule.")
@app_commands.describe(name="The exact name of the logic to delete.")
@admin_only()
async def logic_delete(interaction: discord.Interaction, name: str) -> None:
    document = await asyncio.to_thread(bot.store.delete_logic, interaction.guild.id, name)
    if not document:
        return await interaction.response.send_message("I couldn't find a logic with that name.", ephemeral=True)
    await interaction.response.send_message(
        f"Deleted **{discord.utils.escape_markdown(document['name'])}**. Any roles previously assigned by this logic will be checked against your remaining active logics.",
        ephemeral=True,
    )
    await bot.engine.reconcile_guild(interaction.guild)



@logic_delete.autocomplete("name")
async def logic_delete_name_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    return await _logic_name_autocomplete(interaction, current)


@logic_group.command(name="pause", description="Pause a RoleLogic rule without deleting it.")
@app_commands.describe(name="The exact name of the logic to pause.")
@admin_only()
async def logic_pause(interaction: discord.Interaction, name: str) -> None:
    document = await asyncio.to_thread(bot.store.get_logic, interaction.guild.id, name)
    if not document:
        return await interaction.response.send_message("I couldn't find a logic with that name.", ephemeral=True)
    await asyncio.to_thread(bot.store.update_logic, document["_id"], interaction.guild.id, {"enabled": False})
    await interaction.response.send_message(f"Paused **{discord.utils.escape_markdown(document['name'])}**.", ephemeral=True)
    await bot.engine.reconcile_guild(interaction.guild)



@logic_pause.autocomplete("name")
async def logic_pause_name_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    return await _logic_name_autocomplete(interaction, current)


@logic_group.command(name="resume", description="Resume a paused RoleLogic rule.")
@app_commands.describe(name="The exact name of the logic to resume.")
@admin_only()
async def logic_resume(interaction: discord.Interaction, name: str) -> None:
    document = await asyncio.to_thread(bot.store.get_logic, interaction.guild.id, name)
    if not document:
        return await interaction.response.send_message("I couldn't find a logic with that name.", ephemeral=True)
    await asyncio.to_thread(bot.store.update_logic, document["_id"], interaction.guild.id, {"enabled": True})
    await interaction.response.send_message(f"Resumed **{discord.utils.escape_markdown(document['name'])}**.", ephemeral=True)
    await bot.engine.reconcile_guild(interaction.guild)



@logic_resume.autocomplete("name")
async def logic_resume_name_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    return await _logic_name_autocomplete(interaction, current)


@logic_group.command(name="sync", description="Immediately scan this server and apply all active RoleLogic rules.")
@admin_only()
async def logic_sync(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)
    scanned, added, removed = await bot.engine.reconcile_guild(interaction.guild)
    await interaction.followup.send(
        f"**RoleLogic sync complete.**\nMembers checked: **{scanned}**\nRoles assigned: **{added}**\nRoles removed: **{removed}**",
        ephemeral=True,
    )


@tasks.loop(hours=24)
async def daily_reconciliation() -> None:
    if bot.engine:
        log.info("Starting scheduled 24-hour RoleLogic reconciliation.")
        await bot.engine.reconcile_all()


@daily_reconciliation.before_loop
async def before_daily_reconciliation() -> None:
    await bot.wait_until_ready()


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    if isinstance(error, app_commands.CheckFailure):
        message = str(error) or "You don't have permission to use this command."
    else:
        log.exception("Slash command failed", exc_info=error)
        message = "Something went wrong while running that command. The error has been logged."
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def main() -> None:
    keep_alive()
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
