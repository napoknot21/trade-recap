from __future__ import annotations

import re
import json, ast
import polars as pl

from collections import deque
from typing import Any, Dict, Iterable, List, Optional, Tuple

_SANITIZE_RX = re.compile(r"[^0-9A-Za-z_]+")
DEFAULT_SEP = "."


def sanitize_key (key: str, sep: str = DEFAULT_SEP) -> str :
    """
    
    """
    if sep in key :
        key = key.replace(sep, "_")

    return _SANITIZE_RX.sub("_", key)


def py_to_jsonish (s : Optional[str]) -> Optional[str] :
    """
    
    """
    if s is None :
        return None
    
    s = s.strip()
    if not s or s.lower() == "null" :
        return None
    
    try :
        
        json.loads(s)
        return s
    
    except Exception :
        pass
    
    try :
        return json.dumps(ast.literal_eval(s))
    
    except Exception :
        return None


def looks_jsonish_expr (col : pl.Expr) -> pl.Expr :
    """
    
    """
    expr = col.strip_chars().str.starts_with("{") | col.strip_chars().str.starts_with("[") | (col.str.contains(":") & (col.str.contains("{") | col.str.contains("[")))
    
    return expr


def drop_struct_and_liststruct_columns (df : pl.DataFrame, *, verbose : bool = True) -> pl.DataFrame :
    """
    
    """
    if df is None or df.is_empty() :
        return df
    
    to_drop: List[str] = []
    
    for name, dtype in df.schema.items() :

        if isinstance(dtype, pl.Struct) :
            to_drop.append(name)
        
        elif isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.Struct) :
            to_drop.append(name)

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