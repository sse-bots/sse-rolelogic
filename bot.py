from __future__ import annotations

from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

from help_menu import setup_help
from activation import setup_activation, _configure_srv_dns

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Import after loading .env because heartbeat_client reads its settings at import time.
from heartbeat_client import start_heartbeat

