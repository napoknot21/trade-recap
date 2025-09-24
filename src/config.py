from __future__ import annotations

import re, os
from dotenv import load_dotenv

load_dotenv()

# ============================== Config / Globals ==============================

LIBAPI_ABS_PATH = os.getenv("LIBAPI_ABS_PATH")
FILE_BASENAME_EXCEL_SRC = os.getenv("FILE_BASENAME_EXCEL_SRC")
FILE_BASENAME_EXCEL_TARGET = os.getenv("FILE_BASENAME_EXCEL_TARGET")

RECAP_DATA_ABS_DIR = os.getenv("RECAP_DATA_ABS_DIR")
RECAP_EMAIL_ABS_DIR = os.getenv("RECAP_EMAIL_ABS_DIR")
RECAP_RAW_DATA_ABS_DIR = os.getenv("RECAP_RAW_DATA_ABS_DIR")

EMAIL_DEFAULT_TO = os.getenv("EMAIL_DEFAULT_TO")
EMAIL_DEFAULT_CC = os.getenv("EMAIL_DEFAULT_CC")
EMAIL_DEFAULT_SUBJECT = os.getenv("EMAIL_DEFAULT_SUBJECT")
EMAIL_DEFAULT_FROM = os.getenv("EMAIL_DEFAULT_FROM")
EMAIL_DEFAULT_BODY_INTRO = os.getenv("EMAIL_DEFAULT_BODY_INTRO")

_SANITIZE_RX = re.compile(r"[^0-9A-Za-z_]+")

# column namespace separator
SEP = "."