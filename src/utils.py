from __future__ import annotations

import re
import json, ast
import polars as pl

from collections import deque
from typing import Any, Dict, List, Optional, Tuple, Set
from src.config import SANITIZE_RX, SEP


# Heuristic: string "looks like" JSON if it begins with '{' or '['.
JSON_START_RX = re.compile(r"^\s*[\{\[]")

# Match an ID-like last segment: 'id' or '*id', case-insensitive, at end of path
IDENTIFIER_RX = re.compile(r"(?:^|\.)(?:[A-Za-z0-9_]*id)$", re.IGNORECASE)


def sanitize_key (key: str, sep: str = SEP) -> str :
    """
    Replace `sep` with '_' and strip non-alnum/underscore chars.

    Args:
        key: Original key/column name.
        sep: Namespace separator to neutralize (default '.').

    Returns:
        A sanitized token suitable for filenames / identifiers.
    """
    if sep in key :
        key = key.replace(sep, "_")

    return SANITIZE_RX.sub("_", key)


def py_to_jsonish (s : Optional[str]) -> Optional[str] :
    """
    Convert a Python- or JSON-like string into normalized JSON (string), or None.

    Strategy:
    - If `s` already parses as JSON via `json.loads`, return `s` unchanged.
    - Else try `ast.literal_eval` (for Python dict/list/tuple literals) and dump
      the result with `json.dumps`.
    - Return None on blank/'null'/'None' or when parsing fails.

    Args:
        s: Input string (may be None).

    Returns:
        JSON string if successful; otherwise None.
    """
    if s is None :
        return None
    
    s = s.strip()
    if not s or s.lower() in {"null", "none"} :
        return None
    
    # Fast path ; already valid JSON
    try :
        
        json.loads(s)
        return s
    
    except Exception :
        pass
    
    # Heuristic: only try literal_eval if it *looks* like a literal
    if JSON_START_RX.search(s) or s.startswith(("'", '"')) or s[:1].isdigit() :

        try :

            py_obj = ast.literal_eval(s)
            return json.dumps(py_obj)

        except Exception :
            return None

    return None


def looks_jsonish_expr (col : pl.Expr) -> pl.Expr :
    """
    Build a Polars expression that flags strings that *look* like JSON.

    Heuristics:
    - Starts with '{' or '[' after stripping spaces, OR
    - Contains a JSON-style key pattern (e.g., `"key":`)

    Args:
        col: A Polars Utf8 column as an expression.

    Returns:
        pl.Expr yielding a boolean Series (nulls -> False).
    """
    s = col.str.strip_chars()

    expr = (s.str.starts_with("{") 
            | s.str.starts_with("[") 
            | s.str.contains(r'"\s*[^"]+\s*"\s*:')
        )
    
    return expr.fill_null(False)


def drop_struct_and_liststruct_columns (df : pl.DataFrame, *, verbose : bool = True) -> pl.DataFrame :
    """
    Drop columns whose dtype is `Struct` or `List[Struct]`.

    This uses dtype-based selection (robust across Polars versions).

    Args:
        df: Input DataFrame (may be empty).
        verbose: If True, log dropped column names.

    Returns:
        A new DataFrame with selected columns removed (or `df` unchanged).
    """
    if df is None or df.is_empty() :
        return df
    
    # Select column names by dtype; safer than manual dtype comparisons
    struct_cols = df.select(pl.col(pl.Struct)).columns
    list_struct_cols = df.select(pl.col(pl.List(pl.Struct))).columns
    to_drop = list(dict.fromkeys(struct_cols + list_struct_cols))

    if verbose and to_drop :
        print(f"[*] Dropping Struct/List[Struct] columns: {to_drop}")
    
    return df.drop(to_drop) if to_drop else df


def data_trade_hierarchy_tree (trade_dict : List[Dict]) -> Dict[Any, set] :
    """
    
    """
    tree: Dict[Any, set] = {}

    for t in trade_dict :

        tid = t.get("tradeId")
        leg = t.get("tradeLegId")
    
        tree.setdefault(tid, set()).add(leg)
    
    return tree


def align_columns (df1 : pl.DataFrame, df2 : pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame] :
    """
    
    """
    all_cols = set(df1.columns) | set(df2.columns)

    for c in all_cols :

        if c not in df1.columns :
            df1 = df1.with_columns(pl.lit(None).alias(c))
        
        if c not in df2.columns :
            df2 = df2.with_columns(pl.lit(None).alias(c))
    
    cols_sorted = sorted(all_cols)
    
    return df1.select(cols_sorted), df2.select(cols_sorted)


def is_identifier_col (name : str) -> bool :
    """
    
    """
    nlow = name.lower()
    last = nlow.rsplit(".", 1)[-1]
    
    assertion = last == "id" or last.endswith("id") or "externalid" in nlow or "tradeid" in nlow or "tradelegid" in nlow
    
    return assertion


def split_levels_for_plan (col : str, *, sep : str, max_levels : int, general_label : str) -> tuple[list[str], str] :
    """
    
    """
    if sep not in col :

        levels = [general_label] + [""] * (max_levels - 1)
        return levels, col
    
    parts = col.split(sep)
    
    field = parts[-1]
    mids = parts[:-1]
    
    levels = (mids + [""] * max_levels)[:max_levels]
    
    return levels, field


def max_levels_before_field (df : pl.DataFrame, *, sep : str, exclude_cols : set[str]) -> int :
    """
    
    """
    max_lv = 1
    
    for name, dt in df.schema.items() :

        if name in exclude_cols :
            continue
        
        if isinstance(dt, (pl.Struct, pl.List)):
            continue

        if sep in name :
            lv = max(1, len(name.split(sep)) - 1)
        
        else :
            lv = 1
        
        if lv > max_lv:
            max_lv = lv
    
    return max_lv