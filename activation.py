"""Plug-and-play activation logic for SSE Discord bots.

This module contains activation validation and enforcement only. It does NOT
create keys, process support requests, or approve requests; those belong to
SSE Bots Hub.

Integration (after creating your commands and bot instance):
    from activation import setup_activation
    await setup_activation(bot)

Required .env values:
    MONGODB_URI=...
    BOT_ACTIVATION_ID=SSE-YOURBOT-XXXXXXXX
    ACTIVATION_DATABASE=sse_bots_user   # optional; this is the default

The Discord bot ID is read from bot.user.id at runtime and checked against
its registered Hub record, so it does not need a separate .env variable.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import discord
from discord import app_commands
from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("sse-activation")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def hash_activation_key(key: str) -> str:
    normalized = "".join((key or "").strip().upper().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _configure_srv_dns(uri: str) -> None:
    if not uri.lower().startswith("mongodb+srv://"):
        return
    try:
        import dns.resolver
    except ImportError as exc:
        raise RuntimeError("Install dnspython: pip install dnspython") from exc
    configured = os.getenv("MONGODB_DNS_NAMESERVERS", "").strip()
    if configured:
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = [x.strip() for x in configured.split(",") if x.strip()]
        resolver.timeout = 5
        resolver.lifetime = 10
        dns.resolver.default_resolver = resolver


class ActivationService:
    """Database layer for checking and consuming activation keys."""

    def __init__(self, mongo_client: MongoClient, activation_id: str, discord_bot_id: Optional[int] = None):
        self.client = mongo_client
        self.db = mongo_client[os.getenv("ACTIVATION_DATABASE", "sse_bots_user")]
        self.activation_id = activation_id.strip()
        # Optional compatibility for bots already integrated with the earlier API.
        self.discord_bot_id = str(discord_bot_id) if discord_bot_id is not None else None
        self.bots = self.db.bots
        self.keys = self.db.activation_keys
        self.servers = self.db.activated_servers
        self.logs = self.db.activation_logs

    def initialize_indexes(self) -> None:
        self.bots.create_index([("activation_id", ASCENDING)], unique=True)
        self.bots.create_index([("discord_bot_id", ASCENDING)], unique=True)
        self.keys.create_index([("key_hash", ASCENDING)], unique=True)
        self.servers.create_index(
            [("bot_activation_id", ASCENDING), ("guild_id", ASCENDING)], unique=True
        )
        self.logs.create_index([("bot_activation_id", ASCENDING), ("created_at", ASCENDING)])

    def _registered_bot(self, discord_bot_id: int) -> Optional[dict]:
        return self.bots.find_one({
            "activation_id": self.activation_id,
            "discord_bot_id": str(discord_bot_id),
            "status": "active",
        })

    def is_server_activated(self, guild_id: int, discord_bot_id: Optional[int] = None) -> bool:
        discord_bot_id = discord_bot_id if discord_bot_id is not None else self.discord_bot_id
        if not self.activation_id or not discord_bot_id or not self._registered_bot(discord_bot_id):
            return False
        return self.servers.find_one({
            "bot_activation_id": self.activation_id,
            "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id),
            "status": "active",
        }) is not None

    def server_status(self, guild_id: int, discord_bot_id: Optional[int] = None) -> str:
        """Return active, inactive, or removed for this bot in a Discord server."""
        discord_bot_id = discord_bot_id if discord_bot_id is not None else self.discord_bot_id
        if not self.activation_id or not discord_bot_id or not self._registered_bot(discord_bot_id):
            return "removed"
        record = self.servers.find_one({
            "bot_activation_id": self.activation_id,
            "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id),
        })
        if not record:
            return "removed"
        return "active" if record.get("status") == "active" else "inactive"

    def deactivate_server(self, guild_id: int, user_id: int, discord_bot_id: Optional[int] = None) -> tuple[bool, str]:
        discord_bot_id = discord_bot_id if discord_bot_id is not None else self.discord_bot_id
        query = {
            "bot_activation_id": self.activation_id,
            "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id),
            "status": "active",
        }
        record = self.servers.find_one_and_update(
            query,
            {"$set": {"status": "inactive", "deactivated_at": utcnow(), "deactivated_by": str(user_id)}},
        )
        if not record:
            return False, "This server does not have an active activation to deactivate."
        self.logs.insert_one({
            "bot_activation_id": self.activation_id, "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id), "user_id": str(user_id),
            "event": "deactivated", "created_at": utcnow(),
        })
        return True, "SSE Tournament Bot has been deactivated in this server."

    def reactivate_server(self, guild_id: int, user_id: int, discord_bot_id: Optional[int] = None) -> tuple[bool, str]:
        discord_bot_id = discord_bot_id if discord_bot_id is not None else self.discord_bot_id
        if not self._registered_bot(discord_bot_id):
            return False, "This bot is currently disabled in SSE Bots Hub."
        query = {
            "bot_activation_id": self.activation_id,
            "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id),
            "status": "inactive",
        }
        record = self.servers.find_one_and_update(
            query,
            {"$set": {"status": "active", "reactivated_at": utcnow(), "reactivated_by": str(user_id)},
             "$unset": {"deactivated_at": "", "deactivated_by": ""}},
        )
        if not record:
            return False, "No deactivated server activation was found. If the server was removed, use `/activate` with a new key."
        self.logs.insert_one({
            "bot_activation_id": self.activation_id, "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id), "user_id": str(user_id),
            "event": "reactivated", "created_at": utcnow(),
        })
        return True, "SSE Tournament Bot has been reactivated in this server."

    def remove_server(self, guild_id: int, user_id: int, discord_bot_id: Optional[int] = None) -> tuple[bool, str]:
        discord_bot_id = discord_bot_id if discord_bot_id is not None else self.discord_bot_id
        query = {
            "bot_activation_id": self.activation_id,
            "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id),
        }
        record = self.servers.find_one_and_delete(query)
        if not record:
            return False, "No activation record was found for this server."
        self.logs.insert_one({
            "bot_activation_id": self.activation_id, "discord_bot_id": str(discord_bot_id),
            "guild_id": str(guild_id), "user_id": str(user_id),
            "event": "server_removed", "created_at": utcnow(),
            "key_id": record.get("key_id"),
        })
        return True, "This server's activation record has been permanently removed. A new activation key is required to use the bot again."

    def activate(self, key: str, guild_id: int, user_id: int, discord_bot_id: Optional[int] = None) -> tuple[bool, str]:
        discord_bot_id = discord_bot_id if discord_bot_id is not None else self.discord_bot_id
        if not self.activation_id:
            return False, "BOT_ACTIVATION_ID is missing from this bot's .env file."
        if not discord_bot_id:
            return False, "Could not identify this Discord bot. Restart it and try again."
        bot_doc = self._registered_bot(discord_bot_id)
        if not bot_doc:
            return False, "This bot is not registered or enabled in SSE Bots Hub."
        if len((key or "").strip()) < 16:
            return False, "That activation key is invalid."
        guild_filter = {
            "bot_activation_id": self.activation_id,
            "guild_id": str(guild_id),
            "status": "active",
        }
        if self.servers.find_one(guild_filter):
            return False, "This server has already activated this bot."
        key_hash = hash_activation_key(key)
        try:
            with self.client.start_session() as session:
                def transaction(s):
                    current_bot = self._registered_bot(discord_bot_id)
                    if not current_bot:
                        raise ValueError("This bot has been disabled in SSE Bots Hub.")
                    key_doc = self.keys.find_one({
                        "key_hash": key_hash,
                        "bot_activation_id": self.activation_id,
                        "status": "unused",
                    }, session=s)
                    if not key_doc:
                        raise ValueError("Key is invalid, already used, revoked, or belongs to another bot.")
                    if self.servers.find_one(guild_filter, session=s):
                        raise ValueError("This server has already activated this bot.")
                    consumed = self.keys.update_one(
                        {"_id": key_doc["_id"], "status": "unused"},
                        {"$set": {
                            "status": "used", "used_at": utcnow(),
                            "used_by_guild_id": str(guild_id), "used_by_user_id": str(user_id),
                        }}, session=s,
                    )
                    if consumed.modified_count != 1:
                        raise ValueError("This key has already been used.")
                    self.servers.insert_one({
                        "bot_activation_id": self.activation_id,
                        "bot_name": current_bot.get("name", "SSE Bot"),
                        "discord_bot_id": str(discord_bot_id),
                        "guild_id": str(guild_id),
                        "activated_by": str(user_id),
                        "activated_at": utcnow(),
                        "status": "active",
                        "key_id": str(key_doc["_id"]),
                    }, session=s)
                    self.logs.insert_one({
                        "bot_activation_id": self.activation_id,
                        "discord_bot_id": str(discord_bot_id),
                        "guild_id": str(guild_id), "user_id": str(user_id),
                        "event": "activated", "created_at": utcnow(),
                    }, session=s)
                session.with_transaction(transaction)
            return True, f"Successfully activated **{bot_doc.get('name', 'this bot')}** in this server."
        except ValueError as exc:
            return False, str(exc)
        except DuplicateKeyError:
            return False, "This server or activation key has already been used."
        except PyMongoError:
            log.exception("Activation database transaction failed")
            return False, "Activation service is temporarily unavailable. Please try again later."



async def setup_activation(bot: discord.Client) -> ActivationService:
    """Install three-state activation and guild-specific command visibility."""

    uri = os.getenv("MONGODB_URI", "").strip()
    activation_id = os.getenv("BOT_ACTIVATION_ID", "").strip()

    if not uri:
        raise RuntimeError("MONGODB_URI is required for activation.")
    if not activation_id:
        raise RuntimeError("BOT_ACTIVATION_ID is required for activation.")

    _configure_srv_dns(uri)

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=10000,
        appname="SSE-Bot-Activation",
    )

    service = ActivationService(client, activation_id)

    await asyncio.to_thread(client.admin.command, "ping")
    await asyncio.to_thread(service.initialize_indexes)

    # --------------------------------------------------
    # COMMAND SYNCHRONIZATION
    # --------------------------------------------------

    tree = bot.tree

    async def sync_guild_commands(guild: discord.Guild):
        """Synchronize commands according to this server's activation state."""

        bot_user_id = bot.user.id if bot.user else None
        if not bot_user_id:
            return

        status = await asyncio.to_thread(
            service.server_status, guild.id, bot_user_id
        )

        # Rebuild this guild's command list from the bot's
        # complete locally registered command list.
        tree.clear_commands(guild=guild)
        tree.copy_global_to(guild=guild)

        commands = list(tree.get_commands(guild=guild))

        if status == "active":
            allowed = None  # All normal commands and management commands.
        elif status == "inactive":
            allowed = {"reactivate"}
        else:
            allowed = {"activation"}

        for command in commands:
            if allowed is None:
                # Active: hide activation and reactivation.
                if command.name in {"activation", "reactivate"}:
                    tree.remove_command(command.name, guild=guild)
            elif command.name not in allowed:
                tree.remove_command(command.name, guild=guild)

        await tree.sync(guild=guild)

        log.info(
            "Synced commands for guild %s (activation status: %s)",
            guild.id,
            status,
        )

    async def sync_all_guilds():
        """Synchronize every server the bot currently belongs to."""

        for guild in list(bot.guilds):
            try:
                await sync_guild_commands(guild)
            except Exception:
                log.exception(
                    "Could not synchronize commands for guild %s",
                    guild.id,
                )

    # --------------------------------------------------
    # ACTIVATION COMMAND
    # --------------------------------------------------

    @app_commands.describe(
        key="Enter the activation key provided by SSE Bots Hub."
    )
    async def activation_command(
        interaction: discord.Interaction,
        key: str,
    ):
        if interaction.guild is None:
            return await interaction.response.send_message(
                "Use this command inside the Discord server you want to activate.",
                ephemeral=True,
            )

        if interaction.guild.owner_id != interaction.user.id:
            return await interaction.response.send_message(
                "Only the actual Discord server owner can activate this bot.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True, thinking=True)

        bot_user_id = bot.user.id if bot.user else None

        if not bot_user_id:
            return await interaction.followup.send(
                "Bot identity is not ready. Try again shortly.",
                ephemeral=True,
            )

        success, message = await asyncio.to_thread(
            service.activate,
            key,
            interaction.guild.id,
            interaction.user.id,
            bot_user_id,
        )

        await interaction.followup.send(message, ephemeral=True)

        if success:
            await sync_guild_commands(interaction.guild)
            log.info(
                "Activated bot %s in guild %s",
                activation_id,
                interaction.guild.id,
            )

    # --------------------------------------------------
    # DEACTIVATE COMMAND
    # --------------------------------------------------

    async def deactivate_command(interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message(
                "This command can only be used inside a server.",
                ephemeral=True,
            )

        if interaction.guild.owner_id != interaction.user.id:
            return await interaction.response.send_message(
                "Only the actual server owner can deactivate this bot.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True, thinking=True)

        bot_user_id = bot.user.id if bot.user else None

        success, message = await asyncio.to_thread(
            service.deactivate_server,
            interaction.guild.id,
            interaction.user.id,
            bot_user_id,
        )

        await interaction.followup.send(message, ephemeral=True)

        if success:
            await sync_guild_commands(interaction.guild)
            log.info("Deactivated guild %s", interaction.guild.id)

    # --------------------------------------------------
    # REACTIVATE COMMAND
    # --------------------------------------------------

    async def reactivate_command(interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message(
                "This command can only be used inside a server.",
                ephemeral=True,
            )

        if interaction.guild.owner_id != interaction.user.id:
            return await interaction.response.send_message(
                "Only the actual server owner can reactivate this bot.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True, thinking=True)

        bot_user_id = bot.user.id if bot.user else None

        success, message = await asyncio.to_thread(
            service.reactivate_server,
            interaction.guild.id,
            interaction.user.id,
            bot_user_id,
        )

        await interaction.followup.send(message, ephemeral=True)

        if success:
            await sync_guild_commands(interaction.guild)
            log.info("Reactivated guild %s", interaction.guild.id)

    # --------------------------------------------------
    # PERMANENT SERVER REMOVAL COMMAND
    # --------------------------------------------------

    async def remove_server_command(interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message(
                "This command can only be used inside a server.",
                ephemeral=True,
            )

        if interaction.guild.owner_id != interaction.user.id:
            return await interaction.response.send_message(
                "Only the actual server owner can remove this server's activation.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True, thinking=True)

        bot_user_id = bot.user.id if bot.user else None

        success, message = await asyncio.to_thread(
            service.remove_server,
            interaction.guild.id,
            interaction.user.id,
            bot_user_id,
        )

        await interaction.followup.send(message, ephemeral=True)

        if success:
            await sync_guild_commands(interaction.guild)
            log.info("Permanently removed guild %s", interaction.guild.id)

    # --------------------------------------------------
    # REGISTER COMMANDS
    # --------------------------------------------------

    command_definitions = [
        app_commands.Command(
            name="activation",
            description="Activate this SSE bot using an activation key.",
            callback=activation_command,
        ),
        app_commands.Command(
            name="deactivate",
            description="Temporarily deactivate this bot in this server.",
            callback=deactivate_command,
        ),
        app_commands.Command(
            name="reactivate",
            description="Reactivate this bot in this server.",
            callback=reactivate_command,
        ),
        app_commands.Command(
            name="remove_server",
            description="Permanently remove this server's activation.",
            callback=remove_server_command,
        ),
    ]

    for command in command_definitions:
        try:
            tree.add_command(command)
        except app_commands.CommandAlreadyRegistered:
            log.warning(
                "Command /%s is already registered.",
                command.name,
            )

    # --------------------------------------------------
    # GLOBAL COMMAND REMOVAL
    # --------------------------------------------------
    #
    # Important:
    # We first save the locally registered commands.
    # Then remove all remote global commands.
    # Finally restore the local definitions so they can
    # be copied into individual guilds.
    #
    # This prevents global commands from bypassing the
    # three-state visibility system.

    original_commands = list(tree.get_commands())

    tree.clear_commands(guild=None)
    await tree.sync()

    for command in original_commands:
        try:
            tree.add_command(command)
        except app_commands.CommandAlreadyRegistered:
            log.warning(
                "Could not restore local command /%s",
                command.name,
            )

    # --------------------------------------------------
    # GLOBAL INTERACTION ENFORCEMENT
    # --------------------------------------------------

    previous_check = getattr(tree, "interaction_check", None)

    async def activation_interaction_check(
        interaction: discord.Interaction,
    ) -> bool:

        if previous_check is not None:
            allowed = await previous_check(interaction)
            if not allowed:
                return False

        if interaction.guild is None:
            return False

        command_name = (
            interaction.command.name
            if interaction.command is not None
            else None
        )

        bot_user_id = bot.user.id if bot.user else None

        if not bot_user_id:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Activation verification is not ready. Try again shortly.",
                    ephemeral=True,
                )
            return False

        try:
            status = await asyncio.to_thread(
                service.server_status,
                interaction.guild.id,
                bot_user_id,
            )
        except Exception:
            log.exception("Activation verification failed")
            status = "removed"

        # Never activated or permanently removed.
        if status == "removed":
            if command_name == "activation":
                return True

            await interaction.response.send_message(
                "This server has not activated this SSE bot. "
                "Use `/activation` to activate it.",
                ephemeral=True,
            )
            return False

        # Temporarily deactivated.
        if status == "inactive":
            if command_name == "reactivate":
                return True

            await interaction.response.send_message(
                "This bot is currently deactivated in this server. "
                "Use `/reactivate` to restore access.",
                ephemeral=True,
            )
            return False

        # Active.
        if command_name in {"activation", "reactivate"}:
            await interaction.response.send_message(
                "This command is not available in the current activation state.",
                ephemeral=True,
            )
            return False

        return True

    tree.interaction_check = activation_interaction_check

    # --------------------------------------------------
    # AUTOMATIC GUILD SYNCHRONIZATION
    # --------------------------------------------------

    async def activation_on_ready():
        await sync_all_guilds()

    async def activation_on_guild_join(guild: discord.Guild):
        await sync_guild_commands(guild)

    async def activation_on_guild_remove(guild: discord.Guild):
        try:
            tree.clear_commands(guild=guild)
            await tree.sync(guild=guild)
        except Exception:
            log.exception(
                "Could not clean up commands for guild %s",
                guild.id,
            )

    bot.add_listener(activation_on_ready, "on_ready")
    bot.add_listener(activation_on_guild_join, "on_guild_join")
    bot.add_listener(activation_on_guild_remove, "on_guild_remove")

    # Keep references alive and expose synchronization
    # for future activation-management integrations.
    setattr(bot, "_sse_activation_client", client)
    setattr(bot, "_sse_activation_service", service)
    setattr(bot, "_sse_activation_sync_guild", sync_guild_commands)
    setattr(bot, "_sse_activation_sync_all", sync_all_guilds)

    log.info(
        "Three-state activation system initialized for %s",
        activation_id,
    )

    return service
