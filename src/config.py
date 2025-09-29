from __future__ import annotations

"""
Project configuration loader and small helpers.

Loads environment variables (via `.env` when present), exposes the values as
module-level constants, and provides utilities to parse/validate environment
values in a consistent way (paths, booleans, lists, sanitization).

"""

import re, os

from typing import Iterable, List, Optional
from dotenv import load_dotenv

load_dotenv()

# ============================== Config / Globals ==============================

LIBAPI_ABS_PATH = os.getenv("LIBAPI_ABS_PATH")

RECAP_DATA_ABS_DIR = os.getenv("RECAP_DATA_ABS_DIR")
RECAP_EMAIL_ABS_DIR = os.getenv("RECAP_EMAIL_ABS_DIR")
RECAP_RAW_DATA_ABS_DIR = os.getenv("RECAP_RAW_DATA_ABS_DIR")

EMAIL_DEFAULT_TO = os.getenv("EMAIL_DEFAULT_TO")
EMAIL_DEFAULT_CC = os.getenv("EMAIL_DEFAULT_CC")
EMAIL_DEFAULT_SUBJECT = os.getenv("EMAIL_DEFAULT_SUBJECT")
EMAIL_DEFAULT_FROM = os.getenv("EMAIL_DEFAULT_FROM")
EMAIL_DEFAULT_BODY_INTRO = os.getenv("EMAIL_DEFAULT_BODY_INTRO")

SANITIZE_RX = re.compile(r"[^0-9A-Za-z_]+")

DEFAULT_EXCLUDED_BOOKS = os.getenv("DEFAULT_EXCLUDED_BOOKS")
DEFAULT_EXCLUDED_BOOKS_LIST = [b.strip() for b in DEFAULT_EXCLUDED_BOOKS.split(";") if b.strip()]

# column namespace separator
SEP = "."