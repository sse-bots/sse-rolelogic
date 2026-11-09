"""Reusable interactive slash-command help center for discord.py 2.x.

Usage:
    from help_menu import setup_help
    setup_help(bot)

Call setup_help(bot) after registering the bot's commands and before syncing
the command tree. The menu derives its branding and command list from that bot.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import discord
from discord import app_commands

log = logging.getLogger(__name__)


def _children(command: Any) -> list[Any]:
    """Return a command group's immediate children."""
    if isinstance(command, app_commands.Group):
        return list(command.commands)
    return []


def _description(command: Any) -> str:
    value = getattr(command, "description", None)
    return (str(value).strip() if value else "No description provided.")[:100]


def _command_emoji_keys(command: Any) -> list[str]:
    name = str(getattr(command, "name", "")).lower().replace("-", "_")
    keys = [name] if name else []
    root = name.split("_", 1)[0] if name else ""
    if root and root not in keys:
        keys.append(root)
    return keys


async def _ensure_app_emojis(bot: Any) -> dict[str, str]:
    """Fetch this bot's application emojis once and cache them on the bot."""
    if getattr(bot, "_help_app_emojis_loaded", False):
        cached = getattr(bot, "_help_app_emojis", {})
        return cached if isinstance(cached, dict) else {}

    task = getattr(bot, "_help_app_emojis_task", None)
    if task is None:
        async def fetch() -> dict[str, str]:
            fetcher = getattr(bot, "fetch_application_emojis", None)
            if not callable(fetcher):
                return {}
            try:
                fetched = await fetcher()
                return {
                    str(item.name).lower(): str(item)
                    for item in (fetched or [])
                    if getattr(item, "name", None)
                }
            except Exception as exc:
                log.debug("Could not fetch application emojis for %s: %s", _bot_name(bot), exc)
                return {}

        task = asyncio.create_task(fetch())
        try:
            setattr(bot, "_help_app_emojis_task", task)
        except Exception:
            pass

    try:
        result = await task
    except Exception as exc:
        log.debug("Application emoji loading failed: %s", exc)
        result = {}

    try:
        setattr(bot, "_help_app_emojis", result)
        setattr(bot, "_help_app_emojis_loaded", True)
    except Exception:
        pass
    return result


def _lookup_emoji(mapping: Any, keys: list[str]) -> str | None:
    if not isinstance(mapping, dict):
        return None
    normalized = {str(key).lower().replace("-", "_"): value for key, value in mapping.items()}
    for key in keys:
        value = normalized.get(key)
        if value:
            return str(value)
    return None


def _resolve_command_emoji(
    bot: Any, command: Any, explicit_emojis: dict[str, str] | None = None
) -> str:
    keys = _command_emoji_keys(command)
    value = _lookup_emoji(explicit_emojis, keys)
    if value:
        return value

    app_emojis = getattr(bot, "_help_app_emojis", {})
    value = _lookup_emoji(app_emojis, keys)
    if value:
        return value

    guild_emojis: dict[str, str] = {}
    for item in getattr(bot, "emojis", []) or []:
        if getattr(item, "name", None):
            guild_emojis[str(item.name).lower()] = str(item)
    value = _lookup_emoji(guild_emojis, keys)
    if value:
        return value

    default = app_emojis.get("default") if isinstance(app_emojis, dict) else None
    if default:
        return str(default)
    return "📂" if _children(command) else "📌"


def _select_emoji(value: str) -> Any:
    """Parse custom emoji markup safely; Unicode emoji can be passed directly."""
    if isinstance(value, str) and value.startswith(("<:", "<a:")):
        try:
            return discord.PartialEmoji.from_str(value)
        except (TypeError, ValueError, AttributeError):
            return None
    return value or None


def _qualified_name(command: Any) -> str:
    return str(getattr(command, "qualified_name", getattr(command, "name", "command")))


def _parameter_lines(command: Any) -> list[str]:
    """Format slash-command parameters without assuming every option has choices."""
    result: list[str] = []
    for parameter in getattr(command, "parameters", []) or []:
        name = getattr(parameter, "display_name", None) or getattr(parameter, "name", "option")
        description = getattr(parameter, "description", None) or "No description provided."
        required = bool(getattr(parameter, "required", False))
        option_type = getattr(parameter, "type", None)
        type_name = getattr(option_type, "name", str(option_type or "option")).replace("_", " ").title()

        line = f"**{name}** (`{type_name}`, {'Required' if required else 'Optional'})\n{description}"
        choices = getattr(parameter, "choices", None) or []
        if choices:
            choice_names = [str(getattr(choice, "name", choice)) for choice in choices[:10]]
            if len(choices) > 10:
                choice_names.append("…")
            line += "\nChoices: " + ", ".join(choice_names)

        if getattr(parameter, "autocomplete", False):
            line += "\nAutocomplete: enabled"

        channel_types = getattr(parameter, "channel_types", None) or []
        if channel_types:
            line += "\nChannel types: " + ", ".join(
                str(getattr(item, "name", item)).replace("_", " ").title()
                for item in channel_types
            )

        minimum = getattr(parameter, "min_value", None)
        maximum = getattr(parameter, "max_value", None)
        if minimum is not None or maximum is not None:
            line += f"\nRange: {minimum if minimum is not None else '—'} to {maximum if maximum is not None else '—'}"

        result.append(line[:950])
    return result


def _split_fields(lines: list[str], limit: int = 1000) -> list[str]:
    chunks: list[str] = []
    current = ""
    for line in lines:
        addition = ("\n\n" if current else "") + line
        if current and len(current) + len(addition) > limit:
            chunks.append(current)
            current = line
        else:
            current += addition
    if current:
        chunks.append(current)
    return chunks


def _bot_name(bot: Any) -> str:
    user = getattr(bot, "user", None)
    return getattr(user, "name", None) or getattr(bot, "name", None) or "Discord Bot"


def _bot_avatar(bot: Any) -> str | None:
    user = getattr(bot, "user", None)
    avatar = getattr(user, "display_avatar", None)
    return getattr(avatar, "url", None)


def _registered_commands(bot: Any) -> list[Any]:
    """Read commands currently registered on this bot's local command tree."""
    commands = list(bot.tree.get_commands())
    # Include guild-scoped commands too, without duplicating names already found.
    seen = {(getattr(command, "name", ""), getattr(command, "type", None)) for command in commands}
    for guild in getattr(bot, "guilds", []):
        try:
            guild_commands = bot.tree.get_commands(guild=discord.Object(id=guild.id))
        except (TypeError, AttributeError):
            continue
        for command in guild_commands:
            key = (getattr(command, "name", ""), getattr(command, "type", None))
            if key not in seen:
                commands.append(command)
                seen.add(key)
    return sorted(
        (command for command in commands if getattr(command, "name", None) != "help"),
        key=lambda item: getattr(item, "name", "").casefold(),
    )


def _base_embed(
    bot: Any,
    commands: list[Any],
    page: int = 0,
    emojis: dict[str, str] | None = None,
) -> discord.Embed:
    name = _bot_name(bot)
    embed = discord.Embed(
        title=f"📚 {name} — Help Center"[:256],
        description=(
            "Browse this bot's registered slash commands below.\n"
            "Select a command or category to view its description, subcommands and options."
        ),
        color=discord.Color.blurple(),
    )
    if not commands:
        embed.add_field(
            name="No commands found",
            value="No slash commands are currently registered on this bot's command tree.",
            inline=False,
        )
    else:
        page_size = 21
        page = max(0, page)
        page_commands = commands[page * page_size:(page + 1) * page_size]
        for command in page_commands:
            child_count = len(_children(command))
            emoji = _resolve_command_emoji(bot, command, emojis)
            title = str(getattr(command, "name", _qualified_name(command)))
            title = title.replace("-", " ").replace("_", " ").title()
            command_name = str(getattr(command, "name", _qualified_name(command)))
            detail = f"`/{command_name}`"
            detail += f" · {child_count} subcommands" if child_count else " · Command"
            embed.add_field(
                name=f"{emoji} {title}"[:256],
                value=f"{_description(command)}\n{detail}"[:1024],
                inline=True,
            )

        remainder = len(page_commands) % 3
        if remainder:
            for _ in range(3 - remainder):
                embed.add_field(name="\\u200b", value="\\u200b", inline=True)

        total_pages = max(1, (len(commands) + page_size - 1) // page_size)
        embed.set_footer(
            text=f"{name} • Interactive Help" +
                 (f" • Page {page + 1}/{total_pages}" if total_pages > 1 else "")
        )

    avatar = _bot_avatar(bot)
    if avatar:
        embed.set_thumbnail(url=avatar)
    if not commands:
        embed.set_footer(text=f"{name} • Interactive Help")
    return embed


class HelpCenterView(discord.ui.View):
    """Paginated, user-bound command browser."""

    def __init__(self, bot: Any, owner_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.bot = bot
        self.owner_id = owner_id
        self.emojis = getattr(bot, "_help_explicit_emojis", {})
        self.commands = _registered_commands(bot)
        self.path: list[Any] = []
        self.page = 0
        self._build()

    @property
    def current_nodes(self) -> list[Any]:
        return _children(self.path[-1]) if self.path else self.commands

    @property
    def current(self) -> Any | None:
        return self.path[-1] if self.path else None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "This help menu belongs to another user. Run `/help` to open your own.",
                ephemeral=True,
            )
            return False
        return True

    def _build(self) -> None:
        self.clear_items()
        nodes = self.current_nodes
        page_size = 21 if not self.path else 25
        max_page = max(0, (len(nodes) - 1) // page_size)
        self.page = min(self.page, max_page)
        start = self.page * page_size
        page_nodes = nodes[start:start + page_size]

        if page_nodes:
            options = []
            for node in page_nodes:
                is_group = bool(_children(node))
                label = (f"/{_qualified_name(node)}")[:100]
                description = _description(node)
                if is_group:
                    description = f"{len(_children(node))} subcommands · {description}"
                options.append(discord.SelectOption(
                    label=label,
                    value=str(start + page_nodes.index(node)),
                    description=description[:100],
                    emoji=_select_emoji(_resolve_command_emoji(self.bot, node, self.emojis)),
                ))
            select = discord.ui.Select(
                placeholder="Choose a command or subcommand…",
                min_values=1,
                max_values=1,
                options=options,
                row=0,
            )
            select.callback = self._on_select
            self.add_item(select)
        else:
            self.add_item(discord.ui.Button(
                label="No commands in this section",
                style=discord.ButtonStyle.secondary,
                disabled=True,
                row=0,
            ))

        if self.path:
            back = discord.ui.Button(
                label="Back",
                emoji="↩️",
                style=discord.ButtonStyle.secondary,
                row=1,
            )
            back.callback = self._on_back
            self.add_item(back)

        previous = discord.ui.Button(
            label="Previous",
            emoji="◀️",
            style=discord.ButtonStyle.secondary,
            disabled=self.page <= 0,
            row=1,
        )
        previous.callback = self._on_previous
        self.add_item(previous)

        next_button = discord.ui.Button(
            label="Next",
            emoji="▶️",
            style=discord.ButtonStyle.secondary,
            disabled=(self.page >= max_page),
            row=1,
        )
        next_button.callback = self._on_next
        self.add_item(next_button)

        home = discord.ui.Button(
            label="Main menu",
            emoji="🏠",
            style=discord.ButtonStyle.primary,
            disabled=not self.path,
            row=1,
        )
        home.callback = self._on_home
        self.add_item(home)

    def _current_embed(self) -> discord.Embed:
        if not self.path:
            return _base_embed(self.bot, self.commands, page=self.page, emojis=self.emojis)

        command = self.current
        name = _bot_name(self.bot)
        children = _children(command)
        qualified = _qualified_name(command)
        embed = discord.Embed(
            title=f"{_resolve_command_emoji(self.bot, command, self.emojis)} /{qualified}"[:256],
            description=_description(command),
            color=discord.Color.blurple(),
        )
        avatar = _bot_avatar(self.bot)
        if avatar:
            embed.set_thumbnail(url=avatar)

        if children:
            start = self.page * 25
            visible = children[start:start + 25]
            lines = [
                f"• **/{_qualified_name(child)}** — {_description(child)}"
                for child in visible
            ]
            embed.add_field(
                name="Subcommands",
                value="\n".join(lines)[:1024] or "No subcommands.",
                inline=False,
            )
            embed.set_footer(text="Select a subcommand above to open its details.")
        elif isinstance(command, app_commands.Command):
            parameters = _parameter_lines(command)
            if not parameters:
                embed.add_field(
                    name="Options",
                    value="This command has no additional parameters.",
                    inline=False,
                )
            else:
                for index, chunk in enumerate(_split_fields(parameters)[:5], start=1):
                    embed.add_field(
                        name="Available options" if index == 1 else f"Available options · {index}",
                        value=chunk,
                        inline=False,
                    )
                if len(_split_fields(parameters)) > 5:
                    embed.add_field(
                        name="More options",
                        value="This command has more options than can fit in one embed. See the command's Discord autocomplete/options when running it.",
                        inline=False,
                    )
            embed.set_footer(text=f"{name} • Use Back to return to the command list.")
        else:
            embed.add_field(name="Details", value="No additional command details are available.", inline=False)
        return embed

    async def _on_select(self, interaction: discord.Interaction) -> None:
        try:
            index = int(interaction.data["values"][0])
            node = self.current_nodes[index]
        except (KeyError, IndexError, ValueError, TypeError):
            await interaction.response.send_message(
                "That menu selection is no longer valid. Please try again.",
                ephemeral=True,
            )
            return

        if _children(node):
            self.path.append(node)
            self.page = 0
        else:
            self.path.append(node)
            self.page = 0
        self._build()
        await interaction.response.edit_message(embed=self._current_embed(), view=self)

    async def _on_back(self, interaction: discord.Interaction) -> None:
        if self.path:
            self.path.pop()
        self.page = 0
        self._build()
        await interaction.response.edit_message(embed=self._current_embed(), view=self)

    async def _on_home(self, interaction: discord.Interaction) -> None:
        self.path.clear()
        self.page = 0
        self._build()
        await interaction.response.edit_message(embed=self._current_embed(), view=self)

    async def _on_previous(self, interaction: discord.Interaction) -> None:
        self.page = max(0, self.page - 1)
        self._build()
        await interaction.response.edit_message(embed=self._current_embed(), view=self)

    async def _on_next(self, interaction: discord.Interaction) -> None:
        page_size = 21 if not self.path else 25
        max_page = max(0, (len(self.current_nodes) - 1) // page_size)
        self.page = min(max_page, self.page + 1)
        self._build()
        await interaction.response.edit_message(embed=self._current_embed(), view=self)

    async def on_timeout(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True
        # The original response can be unavailable/deleted; timeout cleanup is best effort.
        message = getattr(self, "message", None)
        if message is not None:
            try:
                await message.edit(view=self)
            except (discord.HTTPException, discord.NotFound):
                pass


def setup_help(
    bot: Any,
    *,
    command_name: str = "help",
    ephemeral: bool = True,
    timeout: float = 300,
    replace_existing: bool = True,
    emojis: dict[str, str] | None = None,
):
    """Register a reusable /help command on any discord.py Bot or AutoShardedBot.

    Call after registering the bot's commands and before syncing its tree.
    """
    if not hasattr(bot, "tree"):
        raise TypeError("setup_help(bot) requires a discord.py bot with an application-command tree.")

    setattr(bot, "_help_explicit_emojis", emojis or {})

    existing = bot.tree.get_command(command_name)
    if existing is not None:
        if not replace_existing:
            raise ValueError(f"The /{command_name} command is already registered.")
        bot.tree.remove_command(command_name)

    async def help_callback(interaction: discord.Interaction) -> None:
        await _ensure_app_emojis(bot)
        view = HelpCenterView(bot, interaction.user.id, timeout=timeout)
        embed = _base_embed(bot, view.commands, page=view.page, emojis=emojis)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=ephemeral)
        try:
            view.message = await interaction.original_response()
        except discord.HTTPException:
            pass

    help_command = app_commands.Command(
        name=command_name,
        description=f"Open the { _bot_name(bot)[:70] } interactive help menu"[:100],
        callback=help_callback,
    )
    bot.tree.add_command(help_command)
    return help_command