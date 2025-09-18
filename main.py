from __future__ import annotations

import re
import fnmatch
import time
import os, sys
import json, ast

import polars as pl
import datetime as dt
import win32com.client as win32

from collections import deque
from typing import List, Dict, Optional, Any, Iterable, Tuple
from dotenv import load_dotenv

load_dotenv()

# ============================== Config / Globals ==============================

LIBAPI_ABS_PATH = os.getenv("LIBAPI_ABS_PATH")
DIRECTORY_DATA_ABS_PATH = os.getenv("DIRECTORY_DATA_ABS_PATH")
FILE_BASENAME_EXCEL_SRC = os.getenv("FILE_BASENAME_EXCEL_SRC")
FILE_BASENAME_EXCEL_TARGET = os.getenv("FILE_BASENAME_EXCEL_TARGET")

_SANITIZE_RX = re.compile(r"[^0-9A-Za-z_]+")
SEP = "."  # column namespace separator

# LibApi import (append path once)
if LIBAPI_ABS_PATH and LIBAPI_ABS_PATH not in sys.path : sys.path.append(LIBAPI_ABS_PATH)
from libapi.ice.trade_manager import TradeManager  # noqa: E402


# ============================== Helper functions ==============================


def _sanitize_key(key: str, sep: str = SEP) -> str:
    """
    Make an arbitrary dict key safe for a column name.
    - replace the separator with '_'
    - strip non [0-9A-Za-z_] characters
    """
    if sep in key :
        key = key.replace(sep, "_")
    
    return _SANITIZE_RX.sub("_", key)


def _py_to_jsonish(s: Optional[str]) -> Optional[str]:
    """
    Convert a pseudo-Python/JSON-ish string into valid JSON text, or None.
    Handles single quotes and Python literals (None/True/False).
    """
    if s is None:
        return None
    
    s = s.strip()
    if not s or s.lower() == "null":
        return None
    
    # Already valid JSON?
    try :
        
        json.loads(s)
        return s
    
    except Exception :
        pass

    # Python literal -> JSON
    try :
        return json.dumps(ast.literal_eval(s))
    
    except Exception :
        return None


def _looks_jsonish_expr (col: pl.Expr) -> pl.Expr :
    """
    Cheap vectorized sniff to detect JSON-like strings.
    """
    return (
        col.strip_chars().str.starts_with("{")
        | col.strip_chars().str.starts_with("[")
        | (col.str.contains(":") & (col.str.contains("{") | col.str.contains("[")))
    )

# ============================== API → DataFrame ===============================

def load_api_data(
        
        start_date: Optional[str] = None,
        excluded_books: Optional[List[str]] = None,
        date_format: str = "%Y-%m-%d",

    ) -> pl.DataFrame:
    """
    Fetch trades (HV/WR) from `start_date` up to today, then enrich each leg
    with full details. Returns a Polars DataFrame.
    """
    start = time.time()
    tm = TradeManager()

    print("\n[*] Getting Trades...")
    all_books_hv = tm.get_all_existing_hv_portfolios() or []
    all_books_wr = tm.get_all_existing_wr_portfolios() or []
    all_books = all_books_hv + all_books_wr

    excluded_set = set(excluded_books or [])
    all_books_filtered = [b for b in all_books if b not in excluded_set]
    print("\n[+] Books available:\n", all_books_filtered)

    print("\n[*] Generating dates to fetch:\n")
    dates = tm.generate_dates(start_date) or []
    print(dates)

    if not dates or not all_books_filtered:
        print("[!] No dates or no books to query. Returning empty DataFrame.")
        return pl.DataFrame()

    print("\n[*] Fetching trade legs...\n")
    data_raw: Dict = tm.get_trades_from_books_by_date(all_books_filtered, dates) or {}
    legs: List[Dict] = data_raw.get("tradeLegs") or []
    
    if not legs:
        print("[!] No trade legs returned by API.")
        return pl.DataFrame()

    trade_ids: List = [t.get("tradeLegId") for t in legs if t.get("tradeLegId")]
    if not trade_ids:
        print("[!] No valid tradeLegId found.")
        return pl.DataFrame()

    print("\n[*] Fetching trade leg details...\n")
    info = tm.get_info_trades_from_ids(trade_ids) or {}
    
    if not info.get("tradeLegs"):
        print("[!] No trade information returned by API")
        return pl.DataFrame()

    df = pl.from_dicts(info["tradeLegs"], strict=False)

    print(f"[+] Done in {time.time() - start:.2f} seconds\n")
    return df

# ============================== Misc utilities ================================

def data_trade_hierarchy_tree (trade_dict: List[Dict]) -> Dict[Any, set]:
    """
    Build a {tradeId -> set(tradeLegId)} map.
    """
    tree: Dict[Any, set] = {}

    for t in trade_dict:
    
        tid = t.get("tradeId")
        leg = t.get("tradeLegId")
    
        tree.setdefault(tid, set()).add(leg)
    
    return tree


def align_columns (df1 : pl.DataFrame, df2 : pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame] :
    """
    Align two DataFrames so they share the same set/order of columns.
    """
    all_cols = set(df1.columns) | set(df2.columns)
    
    for c in all_cols:
    
        if c not in df1.columns :
            df1 = df1.with_columns(pl.lit(None).alias(c))
    
        if c not in df2.columns :
            df2 = df2.with_columns(pl.lit(None).alias(c))
    
    cols_sorted = sorted(all_cols)
    
    return df1.select(cols_sorted), df2.select(cols_sorted)


def rerun_api_call (
        
        start_date: Optional[str] = None,
        books_excluded: Optional[List[str]] = None,
        max_retries: int = 3,
    
    ) -> pl.DataFrame:
    """
    Retry the API fetch up to `max_retries` times if the result is empty.
    """
    attempts = 0

    while attempts <= max_retries :

        df = load_api_data(start_date, books_excluded)
        
        if not df.is_empty():
            return df
        
        attempts += 1
        
        print(f"[!] Empty DataFrame; retry {attempts}/{max_retries}...")
    
    return pl.DataFrame()

# ============ fields/customFields: list[{code,value}] → wide columns ===========

def _parse_list_of_dicts (s: str) -> list[dict]:
    """
    Robustly parse strings like "[]", "[{'code':'A','value':1}]" (single quotes),
    or valid JSON. Returns a normalized list[{'code': str, 'value': str}].
    """
    if s is None :
        return []
    
    s = s.strip()
    if s == "" or s == "[]":
        return []
    
    try :
        v = ast.literal_eval(s)
    
    except Exception :
        
        try :
            v = json.loads(s.replace("'", '"'))
        
        except Exception:
            return []
    
    out = []
    if isinstance(v, list) :

        for item in v :

            if isinstance(item, dict):
                
                out.append(
                    {
                        "code": "" if item.get("code") is None else str(item.get("code")),
                        "value": "" if item.get("value") is None else str(item.get("value")),
                    }
                )

    return out


def manage_list_type_column_from_df (df: pl.DataFrame, column: str = "fields") -> pl.DataFrame:
    """
    If `column` is:
      - list[struct{code,value}]: pivot to wide (fast, pure Polars).
      - Utf8 (string/JSON): parse using _parse_list_of_dicts, then recurse once.
    """
    if df is None or df.is_empty() or column not in df.columns:
        return df

    col_dtype = dict(df.schema).get(column)

    # Case already list[struct] -> pivot to wide (no .unnest to avoid collisions)
    if isinstance(col_dtype, pl.List) and isinstance(col_dtype.inner, pl.Struct) :

        if df.select(pl.col(column).list.len().sum()).item() == 0 :
            return df

        base = df.with_row_index("_rowid").explode(column).drop_nulls(column)
        
        extracted = base.select(
            "_rowid",
            pl.col(column).struct.field("code").cast(pl.Utf8).alias("__code"),
            pl.col(column).struct.field("value").cast(pl.Utf8).alias("__value"),
        )

        wide = extracted.pivot(
            values="__value", index="_rowid", on="__code", aggregate_function="first"
        )

        out = df.with_row_index("_rowid").join(wide, on="_rowid", how="left").drop("_rowid")
        created = [c for c in out.columns if c not in df.columns]
        
        return out.rename({c: f"{column}.{c}" for c in created})

    # Case strings/JSON -> parse -> recurse
    if col_dtype == pl.Utf8 :

        target = pl.List(pl.Struct([pl.Field("code", pl.Utf8), pl.Field("value", pl.Utf8)]))
        
        df2 = df.with_columns(pl.when(pl.col(column).is_null())
                              .then(pl.lit([], dtype=target))
                              .otherwise(pl.col(column).map_elements(_parse_list_of_dicts, return_dtype=target))
                              .alias(column)
        )

        return manage_list_type_column_from_df(df2, column)

    # Other dtypes -> leave as-is
    return df

# ===================== Generic deep flattener with router =====================

def _resolve_list_policy(
        
        path: str,
        routes: Optional[List[Dict[str, Any]]],
        default_strategy: str,
        default_list_max: int,
        default_join_delim: str,

    ) -> Tuple[str, int, str]:
    """
    Resolve the LIST strategy for a given flattened path via glob patterns.
    The first matching route wins. Fallback to defaults if none match.
    """
    if routes :

        for r in routes :

            pat = r.get("pattern")
            if pat and fnmatch.fnmatchcase(path, pat) :

                return (
                    r.get("strategy", default_strategy),
                    int(r.get("list_max", default_list_max)),
                    r.get("join_delim", default_join_delim),
                )
            
    return default_strategy, default_list_max, default_join_delim


def flatten_struct_like_columns_routed (
        
        df: pl.DataFrame,
        columns: Iterable[str],
        *,
        sep: str = SEP,
        parse_strings: bool = True,
        sample_rows: int = 8,
        infer_json_rows: Optional[int] = None,
        drop_source: bool = False,
        max_depth: int = 100,

        # global defaults if a list path doesn't match any route
        default_list_strategy: str = "index",   # 'ignore'|'index'|'first'|'explode'|'join'
        default_list_max: int = 30,
        default_join_delim: str = "; ",

        # routes example: [{"pattern":"instrument.underlyingAssets", "strategy":"explode", "list_max":999}, ...]
        routes: Optional[List[Dict[str, Any]]] = None,

    ) -> pl.DataFrame:
    """
    Deeply flattens the given `columns`. Handles:
      - Structs/dicts → namespaced columns (no .unnest).
      - Strings containing JSON/pseudo-Python → decoded on demand.
      - Lists anywhere → handled via a router (glob patterns) or defaults.
    """
    if df is None or df.is_empty() :
        return df

    out = df

    # Decode JSON-ish strings only where needed (root-level columns)
    if parse_strings :

        sniff_exprs, names = [], []
        
        for col in columns :

            if col in out.columns and out.schema.get(col) == pl.Utf8 :

                sniff_exprs.append(_looks_jsonish_expr(pl.col(col)).alias(col))
                names.append(col)

        if sniff_exprs :

            sniff = out.select(sniff_exprs).head(sample_rows)
            to_decode = [col for col in names if sniff[col].any()]
            
            if to_decode :

                out = out.with_columns(
                    [
                        pl.col(c).map_elements(_py_to_jsonish, return_dtype=pl.Utf8)
                             .str.json_decode(infer_schema_length=infer_json_rows)
                             .alias(c)
                        for c in to_decode
                    ]
                )

    # BFS across all roots; re-read schema only after mutations
    queue = deque([c for c in columns if isinstance(out.schema.get(c), (pl.Struct, pl.List))])
    depth = 0

    while queue and depth < max_depth :

        cur = queue.popleft()
        dt = out.schema.get(cur)
        
        if dt is None :
            continue

        # --- Struct: expand fields with prefixed, sanitized names
        if isinstance(dt, pl.Struct) :

            fields = dt.fields
            
            if fields :
                
                exprs = [
                    pl.col(cur).struct.field(f.name).alias(f"{cur}{sep}{_sanitize_key(f.name, sep)}")
                    for f in fields
                ]

                out = out.with_columns(exprs)

                if drop_source and cur in out.columns :
                    out = out.drop(cur)

                sch = out.schema  # refresh once
                for f in fields :

                    child = f"{cur}{sep}{_sanitize_key(f.name, sep)}"
                    cdt = sch.get(child)
                    
                    if isinstance(cdt, (pl.Struct, pl.List)) :
                        queue.append(child)

        # --- List: apply resolved policy
        elif isinstance(dt, pl.List) :

            inner = dt.inner
            
            strategy, list_max, join_delim = _resolve_list_policy(
                cur, routes, default_list_strategy, default_list_max, default_join_delim
            )

            if strategy == "ignore":
                pass

            elif strategy == "join" and not isinstance(inner, (pl.Struct, pl.List)) :
                out = out.with_columns(pl.col(cur).list.join(join_delim).alias(cur))

            elif strategy in ("index", "first") :

                # avoid creating many empty columns; bound by observed max
                obs_max = out.select(pl.col(cur).list.len().max()).item() or 0
                limit = 1 if strategy == "first" else min(list_max, int(obs_max))

                if limit > 0 :

                    exprs = [pl.col(cur).list.get(i).alias(f"{cur}{sep}{i}") for i in range(limit)]
                    out = out.with_columns(exprs)
                    sch = out.schema

                    for i in range(limit) :

                        child = f"{cur}{sep}{i}"
                        cdt = sch.get(child)

                        if isinstance(cdt, (pl.Struct, pl.List)) :
                            queue.append(child)

            elif strategy == "explode" :

                out = out.explode(cur)
                new_dt = out.schema.get(cur)

                if isinstance(new_dt, (pl.Struct, pl.List)) :
                    queue.append(cur)

        depth += 1

    return out


# Convenience wrappers
def manage_struct_like_column_from_df (
        
        df: pl.DataFrame,
        column: str,
        **kwargs,

    ) -> pl.DataFrame:
    """
    Backward-compatible wrapper: flatten a single 'struct-like' column using the router.
    """
    return flatten_struct_like_columns_routed(df, [column], **kwargs)


def flatten_all_struct_like_columns_routed(
    
        df: pl.DataFrame,
        *,
        include_cols: Optional[List[str]] = None,
        exclude_cols: Optional[List[str]] = None,
        **kwargs,
    
    ) -> pl.DataFrame :
    """
    Flatten all (or a subset) of columns using the routed flattener.
    """
    if df is None or df.is_empty() :
        return df
    
    cols = include_cols or df.columns

    if exclude_cols :
    
        ex = set(exclude_cols)
        cols = [c for c in cols if c not in ex]
    
    return flatten_struct_like_columns_routed(df, cols, **kwargs)


def save_df_timestamped_excel(
        
        df: pl.DataFrame,
        base_dir: Optional[str] = None,
        *,
        base_name: str = "trade-recap",
        stamp_fmt: str = "%Y_%m_%dT%H_%M",
        tz: str = "Europe/Luxembourg",
        verbose: bool = True,

    ) -> str:
    """
    Save `df` to Excel with a timestamped filename:
      <base_name>_<YYYY_MM_DDTHH_MM>.xlsx

    Args:
        df: Polars DataFrame to export.
        base_dir: Directory to write the file into. If None, tries
                  DIRECTORY_DATA_ABS_PATH, then falls back to "./data".
        base_name: Prefix for the file name (default: "trade-recap").
        stamp_fmt: strftime format for the timestamp (default: "%Y_%m_%dT%H_%M").
        tz: IANA timezone for the timestamp (default: "Europe/Luxembourg").
        verbose: Print the final path if True.

    Returns:
        Absolute path to the written Excel file.
    """
    # Resolve output directory
    if base_dir is None:

        try:
            base_dir = DIRECTORY_DATA_ABS_PATH or "./data"  # uses your module-level var if present
        
        except NameError:
            base_dir = "./data"

    os.makedirs(base_dir, exist_ok=True)

    # Build timestamp using desired timezone (fallback to local time)
    try :

        from zoneinfo import ZoneInfo
        now = dt.datetime.now(ZoneInfo(tz))

    except Exception :

        now = dt.datetime.now()

    stamp = now.strftime(stamp_fmt)
    out_path = os.path.join(base_dir, f"{base_name}_{stamp}.xlsx")

    # Write Excel
    df.write_excel(out_path)

    if verbose:
        print(f"[+] Wrote Excel to: {out_path}")

    return out_path

# ================================ Main pipeline ===============================

if __name__ == "__main__" :

    # Example: exclude a few books
    books_excluded: List[str] = ["HV_BONDS_EXO", "HV_EXO_EQUITY", "HV_SMART_BETA"]

    # Fetch via API (for today)
    df = load_api_data(excluded_books=books_excluded)
    print(df)
    
    # fields/customFields → wide columns
    df = manage_list_type_column_from_df(df, "fields")
    df = manage_list_type_column_from_df(df, "customFields")
    print(df)

    # Deep flatten with LIST router (glob-based rules)
    LIST_ROUTES = [
        # Make one row per underlying anywhere (e.g., instrument.underlyingAssets)
        {"pattern": "*.underlyingAssets", "strategy": "explode"},
        # Join scalar tag lists to CSV
        {"pattern": "*.tags", "strategy": "join", "join_delim": ", "},
        # Index up to 10 items for any 'barriers' list
        {"pattern": "*.barriers", "strategy": "index", "list_max": 10},
    ]

    df = flatten_struct_like_columns_routed(
        df,
        columns=["instrument", "premium", "settlement"],  # roots to flatten
        sep=SEP,
        parse_strings=True,
        infer_json_rows=None,        # let Polars infer over all rows (robust)
        drop_source=False,
        max_depth=100,
        default_list_strategy="index",   # fallback for unmatched list paths
        default_list_max=30,
        routes=LIST_ROUTES,
    )

    # Optional: type casts
    df = df.with_columns(
        
        pl.col("instrument.deliveryDate").str.strptime(pl.Date, strict=False, format="%Y-%m-%d"),
        pl.col("instrument.expiryDate").str.strptime(pl.Date, strict=False, format="%Y-%m-%d"),
        pl.col("instrument.tradeDate").str.strptime(pl.Date, strict=False, format="%Y-%m-%d"),
        pl.col("instrument.premiumDate").str.strptime(pl.Date, strict=False, format="%Y-%m-%d"),
        pl.col("instrument.notional").cast(pl.Float64, strict=False),
        pl.col("instrument.strike").cast(pl.Float64, strict=False),
    
    )

    print(df)

    # Export to Excel
    out_path = save_df_timestamped_excel(df, base_dir=DIRECTORY_DATA_ABS_PATH, base_name="trade-recap")
