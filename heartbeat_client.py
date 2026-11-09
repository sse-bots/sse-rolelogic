"""Small heartbeat client for an SSE bot using the SSE Bots Hub API.

Add this file to each bot project and call start_heartbeat(bot) after creating
its discord.py Bot/Client instance. It reports only liveness; it does not
send announcements or expose Discord tokens.
"""

from __future__ import annotations

import asyncio
import logging
import os

import aiohttp

log = logging.getLogger("sse-heartbeat")

HUB_HEARTBEAT_URL = os.getenv("HUB_HEARTBEAT_URL", "").strip().rstrip("/")
BOT_ACTIVATION_ID = os.getenv("BOT_ACTIVATION_ID", "").strip()
HUB_HEARTBEAT_SECRET = os.getenv("HUB_HEARTBEAT_SECRET", "").strip()
HEARTBEAT_INTERVAL = max(30, int(os.getenv("HUB_HEARTBEAT_INTERVAL", "45")))


async def heartbeat_loop(client) -> None:
    """Send a heartbeat while the Discord client remains connected."""
    if not HUB_HEARTBEAT_URL or not BOT_ACTIVATION_ID or not HUB_HEARTBEAT_SECRET:
        log.warning(
            "Heartbeat disabled: set HUB_HEARTBEAT_URL, BOT_ACTIVATION_ID, "
            "and HUB_HEARTBEAT_SECRET in this bot's environment."
        )
        return

    endpoint = f"{HUB_HEARTBEAT_URL}/heartbeat"
    timeout = aiohttp.ClientTimeout(total=10)
    payload = {
        "activation_id": BOT_ACTIVATION_ID,
        "secret": HUB_HEARTBEAT_SECRET,
    }

    # Wait for the bot's initial Discord connection before reporting online.
    await client.wait_until_ready()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not client.is_closed():
            try:
                # Reporting guild IDs lets the Hub announce to servers even when
                # this bot does not use activation keys.
                payload["guild_ids"] = [str(guild.id) for guild in getattr(client, "guilds", [])]
                async with session.post(endpoint, json=payload) as response:
                    if response.status == 200:
                        log.debug("SSE Hub heartbeat accepted")
                    elif response.status in (401, 404):
                        log.error(
                            "SSE Hub rejected heartbeat (HTTP %s). Check the "
                            "activation ID and rotate/configure the heartbeat secret.",
                            response.status,
                        )
                    else:
                        log.warning("SSE Hub heartbeat returned HTTP %s", response.status)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                log.exception("Could not send heartbeat to SSE Bots Hub")
            await asyncio.sleep(HEARTBEAT_INTERVAL)


def start_heartbeat(client) -> asyncio.Task | None:
    """Start one heartbeat task for a discord.py Bot/Client instance."""
    existing = getattr(client, "_sse_heartbeat_task", None)
    if existing and not existing.done():
        return existing
    task = asyncio.create_task(heartbeat_loop(client), name="sse-hub-heartbeat")
    setattr(client, "_sse_heartbeat_task", task)
    return task