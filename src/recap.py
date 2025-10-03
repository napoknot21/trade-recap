# recap.py
from __future__ import annotations

import math, html
import polars as pl
from typing import Iterable, List, Tuple, Optional, Any, Mapping, Callable
from datetime import date, datetime

# ----------------------- Schema helpers -----------------------

def _is_struct_like(dtype: pl.DataType) -> bool:
    # True for Struct or List[Struct] (including nested List[List[Struct]], etc.)
    if dtype == pl.Struct:
        return True
    if isinstance(dtype, pl.List):
        inner = dtype.inner
        # Walk down through nested lists
        while isinstance(inner, pl.List):
            inner = inner.inner
        return inner == pl.Struct
    return False

def drop_struct_like_cols(df: pl.DataFrame) -> pl.DataFrame:
    if df is None or df.is_empty():
        return df
    keep = [c for c, t in df.schema.items() if not _is_struct_like(t)]
    return df.select(keep)

# ----------------------- Column helpers -----------------------

def _depth(name: str, sep: str = ".") -> int:
    return name.count(sep)


def _is_under_root_level1(col: str, root: str, sep: str = ".") -> bool:
    """
    Keep exactly <root>.<field> (depth==1) but NOT deeper.
    """
    return col.startswith(root + sep) and _depth(col, sep) == 1


# ----------------------- DataFrame filtering/ordering -----------------------

def build_recap_from_roots(
        
        df: pl.DataFrame,
        *,
        sep: str = ".",
        include_general: bool = True,
        roots: Optional[Iterable[str]] = ("instrument", "Premium", "Settlement"),
        id_cols: Tuple[str, str, str] = ("counterparty", "tradeId", "tradeLegId"),
        always_include: Iterable[str] = ("originatingAction",),
        sort_by: Iterable[str] = ("counterparty", "tradeId", "tradeLegId"),
        cast_ids_to_text: bool = True,
        drop_structs: bool = True,

    ) -> pl.DataFrame:
    """
    Build a recap DataFrame with:
      - 'General' columns (no dot) if include_general=True
      - level-1 columns under given roots (root.X, not root.X.Y)
      - id/always_include first, optional sorting by sort_by keys
    """
    if df is None or df.is_empty():
        return pl.DataFrame()

    # drop Struct-like cols upfront
    if drop_structs:
        df = drop_struct_like_cols(df)

    cols = df.columns
    cols_set = set(cols)

    ordered: List[str] = []
    seen = set()

    def _append_block(block: Iterable[str]):
        for c in block:
            if c in cols_set and c not in seen:
                ordered.append(c); seen.add(c)

    _append_block(id_cols)
    _append_block(always_include)

    if include_general :
        for c in cols:
            if c not in seen and _depth(c, sep) == 0:
                ordered.append(c); seen.add(c)

    if roots :
        for c in cols:
            if c in seen:
                continue
            for r in roots:
                if _is_under_root_level1(c, r, sep=sep):
                    ordered.append(c); seen.add(c)
                    break

    if not ordered:
        return pl.DataFrame()

    out = df.select(ordered)

    if cast_ids_to_text:
        present_ids = [c for c in id_cols if c in out.columns]
        if present_ids:
            out = out.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in present_ids])

    # Cast dates/datetimes to string for HTML-safety
    dateish = [c for c, t in out.schema.items() if t in (pl.Date, pl.Datetime)]
    if dateish:
        out = out.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in dateish])

    sort_keys = [c for c in sort_by if c in out.columns]
    if sort_keys:
        out = out.sort(sort_keys)

    return out


# ----------------------- HTML rendering (Outlook/Web friendly) -----------------------

_NUM_TYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)

# Inline styles → safe in Outlook/Word and browsers
_TABLE_STYLE = (
    "border-collapse:collapse;"
    "font-family:Segoe UI,Arial,sans-serif;"
    "font-size:12px;"
    "border:1px solid #D1D5DB;"
    "table-layout:auto;"
    "width:100%;"
    "mso-table-lspace:0pt;mso-table-rspace:0pt;"
)

_TH_STYLE = (
    "border:1px solid #D1D5DB;"
    "padding:8px 10px;"
    "background:#F3F4F6;"
    "text-align:left;"
    "white-space:nowrap;"
)

_TD_STYLE = (
    "border:1px solid #D1D5DB;"
    "padding:8px 10px;"
    "vertical-align:top;"
    "word-break:break-word;"
)
_TD_NUM_STYLE = _TD_STYLE + "text-align:right;"

def _default_fmt(v: Any) -> str:
    """Default cell text formatting before escaping."""
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        return f"{v:.6g}"  # tweak precision as needed
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)

def df_to_html_table(
        
        df: pl.DataFrame,
        *,
        max_rows: int = 1000,
        caption: str | None = None,
        zebra: bool = False,
        column_formatters: Optional[Mapping[str, callable]] = None,
        
        autosize: bool = True,
        min_col_ch: int = 6,
        max_col_ch: int = 60,
        truncate_text_at: int | None = None,
        min_table_px: Optional[int] = 1200,     # <- new

    ) -> str:
    """
    Convert a Polars DataFrame into an HTML <table> string.

    Args:
        max_rows: limit to the first N rows
        caption: optional <caption> content
        zebra: add manual zebra striping (no CSS selectors)
        column_formatters: optional {column_name: fn(value)->str} override
    """
    if df.is_empty():
        return "<p><em>No rows.</em></p>"

    headers = df.columns
    data = df.head(max_rows).iter_rows()  # tuple rows (fast)
    rows = list(df.head(max_rows).iter_rows())
    schema = df.schema
    num_idx = {i for i, h in enumerate(headers) if schema[h] in _NUM_TYPES}

    # Prepare per-column formatters
    fmt_map = {c: _default_fmt for c in headers}
    if column_formatters:
        for k, fn in column_formatters.items():
            if k in fmt_map and callable(fn):
                fmt_map[k] = fn

    colgroup_html = ""
    if autosize:
        widths_ch = _estimate_col_widths_in_ch(
            df,
            headers=headers,
            schema=schema,
            data_rows=rows,
            column_formatters=fmt_map,
            min_ch=min_col_ch,
            max_ch=max_col_ch,
        )
        # Use table-layout:auto and prefer width hints via colgroup
        colgroup = ['<colgroup>']
        for w in widths_ch:
            colgroup.append(f'<col style="width:{w}ch;">')
        colgroup.append('</colgroup>')
        colgroup_html = "".join(colgroup)
    
    table_style = _TABLE_STYLE
    if min_table_px:
        table_style = table_style + f"min-width:{min_table_px}px;"  # Outlook-friendly

    parts: List[str] = []
    parts.append('<table role="presentation" style="' + _TABLE_STYLE + '">')

    # Insert colgroup right after <table> for width hints
    if colgroup_html:
        parts.append(colgroup_html)

    if caption:
        parts.append(f"<caption>{html.escape(caption)}</caption>")

    # THEAD
    parts.append("<thead><tr>")
    for h in headers:
        parts.append(f'<th style="{_TH_STYLE}">{html.escape(h)}</th>')
    parts.append("</tr></thead>")

    # TBODY
    parts.append("<tbody>")
    odd = False
    for row in data:
        row_style = ""
        if zebra:
            odd = not odd
            if odd:
                row_style = ' style="background:#FAFAFA;"'
        parts.append(f"<tr{row_style}>")
        for i, v in enumerate(row):
            col = headers[i]
            txt = fmt_map[col](v)
            txt = html.escape("" if txt is None else str(txt), quote=True)
            style = _TD_NUM_STYLE if i in num_idx else _TD_STYLE
            parts.append(f'<td style="{style}">{txt}</td>')
        parts.append("</tr>")
    parts.append("</tbody></table>")

    if df.height > max_rows:
        parts.append(f"<p>Showing {max_rows} of {df.height} rows.</p>")

    return "".join(parts)


# ----------------------- High-level body builder -----------------------

def build_email_body_from_df(
        
        df: pl.DataFrame,
        *,
        intro_text: str = "Please find a quick recap below. Full file attached.",
        caption: str | None = None,
        max_rows: int = 1000,
        zebra: bool = False,
        column_formatters : Optional[Mapping[str, callable]] = None,

    ) -> str:
    """
    Build a full HTML fragment (paragraph + table) from a Polars DataFrame.
    This returns a string ready to be embedded into an email body (in outlook.py)
    or into any HTML page.

    Note: does NOT include <html>/<body> wrapper to let callers merge with signatures.
    """
    table_html = df_to_html_table(
        df,
        max_rows=max_rows,
        caption=caption,
        zebra=zebra,
        column_formatters=column_formatters,
    )
    return f"{(intro_text)}<p>{table_html}<p>"



def _estimate_col_widths_in_ch(
    df: pl.DataFrame,
    *,
    headers: List[str],
    schema: dict[str, pl.DataType],
    data_rows: Iterable[tuple],
    column_formatters: Mapping[str, Callable[[Any], str]] | None,
    min_ch: int = 6,
    max_ch: int = 60,
) -> List[int]:
    """
    Very simple heuristic:
      width = clamp( max(len(header), max(len(formatted cell) for sampled rows)) )
    """
    # Prepare per-column formatters
    fmt_map: dict[str, Callable[[Any], str]] = {c: _default_fmt for c in headers}
    if column_formatters:
        for k, fn in column_formatters.items():
            if k in fmt_map and callable(fn):
                fmt_map[k] = fn

    # Initialize with header lengths
    max_lens = [len(h) for h in headers]

    # Sample rows to refine widths
    for row in data_rows:
        for i, v in enumerate(row):
            col = headers[i]
            txt = fmt_map[col](v)
            # Use the *visible* length before HTML escaping; good enough for ch units.
            L = len("" if txt is None else str(txt))
            if L > max_lens[i]:
                max_lens[i] = L

    # Clamp into a reasonable range to avoid wild widths
    widths = [max(min_ch, min(max_ch, L)) for L in max_lens]
    return widths