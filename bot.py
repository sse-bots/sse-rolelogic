from __future__ import annotations

import asyncio
import logging
import os
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


class NameModal(discord.ui.Modal):
    def __init__(self, wizard: "LogicWizard", existing: dict[str, Any] | None = None):
        super().__init__(title="Edit RoleLogic" if existing else "Create RoleLogic")
        self.wizard = wizard
        self.existing = existing
        self.logic_name = discord.ui.TextInput(
            label="Logic name",
            placeholder="e.g. SSE Staff Assignment",
            max_length=70,
            required=True,
            default=existing.get("name") if existing else None,
        )
        self.add_item(self.logic_name)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        self.wizard.name = str(self.logic_name.value).strip()
        await interaction.response.send_message(
            "**Step 2/4 — Choose the role to assign**\n"
            "Select the role RoleLogic should automatically give members.",
            view=TargetRoleView(self.wizard),
            ephemeral=True,
        )


class LogicWizard:
    def __init__(
        self,
        bot: "RoleLogicBot",
        guild: discord.Guild,
        owner_id: int,
        existing: dict[str, Any] | None = None,
    ):
        self.bot = bot
        self.guild = guild
        self.owner_id = owner_id
        self.existing = existing
        self.name = existing.get("name", "") if existing else ""
        self.target_role_id: int | None = None
        self.operator = "OR"
        self.condition_role_ids: list[int] = []

    async def save(self, interaction: discord.Interaction) -> None:
        if not self.name:
            return await interaction.response.send_message("A logic name is required.", ephemeral=True)

        name_key = self.name.casefold().strip()
        existing_same_name = await asyncio.to_thread(
            self.bot.store.get_logic, self.guild.id, self.name
        )
        if existing_same_name and (
            not self.existing or existing_same_name["_id"] != self.existing["_id"]
        ):
            return await interaction.response.send_message(
                "A logic with that name already exists in this server.", ephemeral=True
            )

        target = self.guild.get_role(int(self.target_role_id or 0))
        if target is None or target.is_default() or target.managed:
            return await interaction.response.send_message("That target role is no longer available.", ephemeral=True)
        if self.guild.me is None or target >= self.guild.me.top_role:
            return await interaction.response.send_message(
                "I cannot manage that role. Move my bot role above the target role and try again.",
                ephemeral=True,
            )

        document = {
            "_id": self.existing["_id"] if self.existing else uuid.uuid4().hex,
            "guild_id": str(self.guild.id),
            "name": self.name,
            "name_key": name_key,
            "target_role_id": int(self.target_role_id),
            "operator": self.operator,
            "condition_role_ids": [int(value) for value in self.condition_role_ids],
            "enabled": self.existing.get("enabled", True) if self.existing else True,
            "created_by": self.existing.get("created_by", str(self.owner_id)) if self.existing else str(self.owner_id),
            "created_at": self.existing.get("created_at", utcnow()) if self.existing else utcnow(),
            "updated_at": utcnow(),
        }
        try:
            if self.existing:
                update_fields = {key: value for key, value in document.items() if key != "_id"}
                await asyncio.to_thread(
                    self.bot.store.logics.update_one,
                    {"_id": self.existing["_id"], "guild_id": str(self.guild.id)},
                    {"$set": update_fields},
                )
            else:
                await asyncio.to_thread(self.bot.store.create_logic, document)
        except DuplicateKeyError:
            return await interaction.response.send_message(
                "A logic with that name already exists.", ephemeral=True
            )
        except PyMongoError:
            log.exception("Could not save RoleLogic")
            return await interaction.response.send_message(
                "Database error while saving this logic. Please try again.", ephemeral=True
            )

        await interaction.response.send_message(
            embed=discord.Embed(
                title="RoleLogic saved",
                description=(
                    f"**{discord.utils.escape_markdown(self.name)}** is now "
                    f"{'updated' if self.existing else 'created'}.\n\n"
                    f"**Assign:** {target.mention}\n"
                    f"**Condition:** {'ALL' if self.operator == 'AND' else 'ANY'} of "
                    f"{len(self.condition_role_ids)} selected roles"
                ),
                color=discord.Color.green(),
            ),
            ephemeral=True,
        )
        await self.bot.engine.reconcile_guild(self.guild)


class TargetRoleView(discord.ui.View):
    def __init__(self, wizard: LogicWizard):
        super().__init__(timeout=300)
        self.wizard = wizard
        self.add_item(TargetRoleSelect(wizard))


class TargetRoleSelect(discord.ui.RoleSelect):
    def __init__(self, wizard: LogicWizard):
        super().__init__(placeholder="Select the role to assign", min_values=1, max_values=1)
        self.wizard = wizard

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.wizard.owner_id:
            return await interaction.response.send_message("This setup belongs to another administrator.", ephemeral=True)
        self.wizard.target_role_id = self.values[0].id
        await interaction.response.send_message(
            "**Step 3/4 — Choose the condition type**\n"
            "OR = member needs at least one selected role.\n"
            "AND = member needs every selected role.",
            view=OperatorView(self.wizard),
            ephemeral=True,
        )


class OperatorSelect(discord.ui.Select):
    def __init__(self, wizard: LogicWizard):
        self.wizard = wizard
        super().__init__(
            placeholder="Choose OR or AND",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="OR — Any selected role", value="OR", description="At least one condition role is required"),
                discord.SelectOption(label="AND — All selected roles", value="AND", description="Every condition role is required"),
            ],
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.wizard.owner_id:
            return await interaction.response.send_message("This setup belongs to another administrator.", ephemeral=True)
        self.wizard.operator = self.values[0]
        await interaction.response.send_message(
            "**Step 4/4 — Select condition roles**\n"
            "Choose one or more roles. Members matching the selected OR/AND rule will receive the target role.",
            view=ConditionRolesView(self.wizard),
            ephemeral=True,
        )


class OperatorView(discord.ui.View):
    def __init__(self, wizard: LogicWizard):
        super().__init__(timeout=300)
        self.add_item(OperatorSelect(wizard))


class ConditionRolesView(discord.ui.View):
    def __init__(self, wizard: LogicWizard):
        super().__init__(timeout=300)
        self.add_item(ConditionRoleSelect(wizard))


class ConditionRoleSelect(discord.ui.RoleSelect):
    def __init__(self, wizard: LogicWizard):
        super().__init__(
            placeholder="Select one or more condition roles",
            min_values=1,
            max_values=25,
        )
        self.wizard = wizard

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.wizard.owner_id:
            return await interaction.response.send_message("This setup belongs to another administrator.", ephemeral=True)
        self.wizard.condition_role_ids = [role.id for role in self.values]
        target = self.wizard.guild.get_role(int(self.wizard.target_role_id or 0))
        conditions = [self.wizard.guild.get_role(role_id) for role_id in self.wizard.condition_role_ids]
        condition_text = ", ".join(role.mention for role in conditions if role) or "Unavailable roles"
        embed = discord.Embed(
            title="Confirm RoleLogic",
            description=(
                f"**Name:** {discord.utils.escape_markdown(self.wizard.name)}\n"
                f"**Assign role:** {target.mention if target else 'Unavailable'}\n"
                f"**Condition:** {self.wizard.operator}\n"
                f"**Required roles:** {condition_text}"
            ),
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(
            embed=embed,
            view=ConfirmLogicView(self.wizard),
            ephemeral=True,
        )


class ConfirmLogicView(discord.ui.View):
    def __init__(self, wizard: LogicWizard):
        super().__init__(timeout=300)
        self.wizard = wizard

    @discord.ui.button(label="Save logic", style=discord.ButtonStyle.success, emoji="💾")
    async def save_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.wizard.owner_id:
            return await interaction.response.send_message("This setup belongs to another administrator.", ephemeral=True)
        await self.wizard.save(interaction)
        for item in self.children:
            item.disabled = True
        try:
            await interaction.edit_original_response(view=self)
        except discord.HTTPException:
            pass

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.wizard.owner_id:
            return await interaction.response.send_message("This setup belongs to another administrator.", ephemeral=True)
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="RoleLogic setup cancelled.", embed=None, view=self)


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


@logic_group.command(name="create", description="Create a new automatic role-assignment logic.")
@admin_only()
async def logic_create(interaction: discord.Interaction) -> None:
    wizard = LogicWizard(bot, interaction.guild, interaction.user.id)
    await interaction.response.send_modal(NameModal(wizard))


@logic_group.command(name="edit", description="Edit an existing role-assignment logic.")
@app_commands.describe(name="The exact name of the logic you want to edit.")
@admin_only()
async def logic_edit(interaction: discord.Interaction, name: str) -> None:
    document = await asyncio.to_thread(bot.store.get_logic, interaction.guild.id, name)
    if not document:
        return await interaction.response.send_message("I couldn't find a logic with that name.", ephemeral=True)
    wizard = LogicWizard(bot, interaction.guild, interaction.user.id, existing=document)
    await interaction.response.send_modal(NameModal(wizard, existing=document))


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
