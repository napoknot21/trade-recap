from __future__ import annotations

import re, os
from dotenv import load_dotenv

load_dotenv()

# ============================== Config / Globals ==============================

LIBAPI_ABS_PATH = os.getenv("LIBAPI_ABS_PATH")
DIRECTORY_DATA_ABS_PATH = os.getenv("DIRECTORY_DATA_ABS_PATH")
FILE_BASENAME_EXCEL_SRC = os.getenv("FILE_BASENAME_EXCEL_SRC")
FILE_BASENAME_EXCEL_TARGET = os.getenv("FILE_BASENAME_EXCEL_TARGET")

_SANITIZE_RX = re.compile(r"[^0-9A-Za-z_]+")

# column namespace separator
SEP = "."