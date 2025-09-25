from __future__ import annotations

import os
import math
import xlsxwriter
import polars as pl
import datetime as dt

from zoneinfo import ZoneInfo
from typing import Optional, Any , Dict, Tuple, List

from src.config import RECAP_DATA_ABS_DIR
from src.utils import is_identifier_col, max_levels_before_field, split_levels_for_plan


def save_df_timestamped_excel (
        
        df: pl.DataFrame,
        base_dir: Optional[str] = None,
        *,
        base_name: str = "trade_recap",
        stamp_fmt: str = "%Y_%m_%dT%H_%M",
        tz: str = "Europe/Luxembourg",
        verbose: bool = True,
        raw : bool = True
    
    ) -> str :
    """
    
    """
    if base_dir is None :
        base_dir = RECAP_DATA_ABS_DIR or "./data" if base_dir is None else base_dir
    
    os.makedirs(base_dir, exist_ok=True)

    try :
        now = dt.datetime.now(ZoneInfo(tz))
    
    except Exception :
        now = dt.datetime.now()

    stamp = now.strftime(stamp_fmt)

    if raw :
        out_path = os.path.join(base_dir, f"{base_name}_{stamp}_raw.xlsx")
    else :
        out_path = os.path.join(base_dir, f"{base_name}_{stamp}.xlsx")

    df.write_excel(out_path)

    if verbose:
        print(f"[+] Wrote Excel to: {out_path}")
    
    return out_path


def build_hierarchy_maps (
        
        df: pl.DataFrame,
        counterparty_col: str,
        trade_id_col: str,
        leg_id_col: str,
    
    ) -> Tuple[List[Any], Dict[Any, List[Any]], Dict[Tuple[Any, Any], List[Any]]]:
    """
    
    """
    # Banks
    banks = (
        df.select(pl.col(counterparty_col).cast(pl.Utf8))
          .unique()
          .to_series()
          .to_list()
    )

    banks = sorted(banks, key=lambda x: (x is None, "" if x is None else str(x)))

    # trade IDs by banque + legs by (banks, trade IDs)
    trade_ids_by_bank: dict[Any, list[Any]] = {}
    legs_by_bank_tid: dict[tuple[Any, Any], list[Any]] = {}

    for bk in banks :

        tids = (
            df.filter(pl.col(counterparty_col) == bk)
              .select(pl.col(trade_id_col))
              .unique()
              .to_series()
              .to_list()
        )
        
        tids = sorted(tids, key=lambda x: (x is None, x))
        trade_ids_by_bank[bk] = tids

        for tid in tids :
            
            legs = (
                df.filter((pl.col(counterparty_col) == bk) & (pl.col(trade_id_col) == tid))
                  .select(pl.col(leg_id_col))
                  .unique()
                  .to_series()
                  .to_list()
            )

            legs = sorted(legs, key=lambda x: (x is None, x))
            legs_by_bank_tid[(bk, tid)] = legs

    return banks, trade_ids_by_bank, legs_by_bank_tid


# --- Wide export helpers (split by originatingAction) -------------------------


def auto_column_widths (sample_rows: list[dict[str, Any]], headers : list[str], min_w: int = 6, max_w: int = 42) -> list[int] :
    """
    Compute simple column widths (character-based) from a sample of rows.
    """
    widths = [max(len(h), min_w) for h in headers]
    
    for row in sample_rows :

        for i, h in enumerate(headers) :

            s = "" if row.get(h) is None else str(row.get(h))
            widths[i] = min(max(widths[i], len(s)), max_w)
    
    return widths


def write_polars_df_sheet(
        
        ws,
        df : pl.DataFrame,
        *,
        header_fmt,
        cell_fmt_text,
        cell_fmt_num,
        cell_fmt_date
    
    ) -> None:
    """
    Write a Polars DataFrame to an xlsxwriter worksheet with basic formatting.
    """
    headers = df.columns

    # Header row
    for c, h in enumerate(headers) :
        ws.write(0, c, h, header_fmt)

    if not df.is_empty() :

        # Basic auto-widths from a small sample
        sample = df.head(200).to_dicts()
        widths = auto_column_widths(sample, headers)

        for c, w in enumerate(widths):
            ws.set_column(c, c, w)

        # Type buckets
        schema = df.schema
        date_like = (pl.Date, pl.Datetime)
        
        num_like = (
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
            pl.Float32, pl.Float64
        )

        date_cols = {i for i, h in enumerate(headers) if schema[h] in date_like}
        num_cols  = {i for i, h in enumerate(headers) if schema[h] in num_like}

        # Data rows
        for r_idx, row in enumerate(df.iter_rows(named=True), start=1) :

            for c_idx, h in enumerate(headers) :

                v = row[h]
                
                if v is None :
                    ws.write(r_idx, c_idx, "", cell_fmt_text)
                
                elif c_idx in date_cols :
                    ws.write(r_idx, c_idx, str(v), cell_fmt_date)  # safe-as-text
                
                elif c_idx in num_cols and isinstance(v, (int, float)) :

                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) :
                        ws.write(r_idx, c_idx, "", cell_fmt_text)
                    
                    else :
                        ws.write_number(r_idx, c_idx, float(v), cell_fmt_num)
                
                else :
                    ws.write(r_idx, c_idx, str(v), cell_fmt_text)

    ws.freeze_panes(1, 0)  # freeze header


def build_rows_plan (
        
        df : pl.DataFrame,
        *,
        sep : str,
        counterparty_col : str,
        trade_id_col : str,
        leg_id_col : str,
        general_section_name : str,
    
    ) -> Tuple[List[Tuple[List[str], str, str]], int, int, int, List[str]] :
    """
    
    """
    exclude_cols = {counterparty_col, trade_id_col, leg_id_col}
    max_levels = max_levels_before_field(df, sep=sep, exclude_cols=exclude_cols)
    
    label_cols_count = max_levels + 1
    field_col_idx = label_cols_count - 1
    first_value_col = label_cols_count

    # Scalar columns for the left side
    scalar_cols = [
        name for name, dt in df.schema.items()
        if name not in exclude_cols and not isinstance(dt, (pl.Struct, pl.List))
    ]

    general : List[Tuple[List[str], str, str]] = []
    namespaced : List[Tuple[List[str], str, str]] = []

    for col in scalar_cols :

        lv, fld = split_levels_for_plan(col, sep=sep, max_levels=max_levels, general_label=general_section_name)
        (namespaced if sep in col else general).append((lv, fld, col))

    general.sort(key=lambda t: t[1])
    namespaced.sort(key=lambda t: (t[0], t[1]))
    
    rows_plan = general + namespaced

    return rows_plan, max_levels, field_col_idx, first_value_col, scalar_cols


def index_records (
        
        df: pl.DataFrame,
        *,
        counterparty_col : str,
        trade_id_col : str,
        leg_id_col : str,
    
    ) -> Dict[Tuple[Any, Any, Any], Dict[str, Any]]:
    """
    
    """
    row_by_key: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    
    for rec in df.to_dicts() :
    
        key = (rec.get(counterparty_col), rec.get(trade_id_col), rec.get(leg_id_col))
        
        if key not in row_by_key :
            row_by_key[key] = rec

    return row_by_key


def create_formats (wb) :
    """
    
    """
    fmt_header_bank = wb.add_format(
                        {
                            "bold": True, "align": "center", "valign": "vcenter",
                            "border": 1, "bg_color": "#0F172A", "font_color": "#FFFFFF"
                        }
                    )
    
    fmt_header_tid  = wb.add_format(
                        {
                            "bold": True, "align": "center", "valign": "vcenter",
                            "border": 1, "bg_color": "#1F2937", "font_color": "#FFFFFF"
                        }
                    )
    
    fmt_header_leg  = wb.add_format(
                        {
                            "bold": True, "align": "center", "valign": "vcenter",
                            "border": 1, "bg_color": "#374151", "font_color": "#FFFFFF"
                        }
                    )
    
    fmt_level       = wb.add_format(
                        {
                            "bold": True, "align": "center", "valign": "vcenter",
                            "border": 1, "bg_color": "#F3F4F6"
                        }
                    )
    
    fmt_field       = wb.add_format(
                        {
                            "bold": True, "align": "left",   "valign": "vcenter",
                            "border": 1, "bg_color": "#EEF2FF"
                        }
                    )
    
    fmt_value_num   = wb.add_format(
                        {
                            "align": "right", "valign": "vcenter", "border": 1
                        }
                    )
    
    fmt_value_txt   = wb.add_format(
                        {
                            "align": "right", "valign": "vcenter", "border": 1, "num_format": "@"
                        }
                    )
    
    fmt_missing     = wb.add_format(
                        {
                            "align": "center", "valign": "vcenter", "border": 1, "font_color": "#9CA3AF"
                        }
                    )
    
    colors = {

        "header_bank" : fmt_header_bank,
        "header_tid" :  fmt_header_tid,
        "header_leg" :  fmt_header_leg,
        "level" : fmt_level,
        "field" : fmt_field,
        "num" : fmt_value_num,
        "text" : fmt_value_txt,
        "missing" : fmt_missing,

    }
    
    return colors


def write_headers(
        
        ws,
        formats: Dict[str, Any],
        banks: List[Any],
        trade_ids_by_bank: Dict[Any, List[Any]],
        legs_by_bank_tid: Dict[Tuple[Any, Any], List[Any]],
        *,
        field_col_idx: int,
        header_height_rows: int,
        first_value_col: int,

    ) -> Dict[Tuple[Any, Any, Any], int] :
    """
    
    """
    # widths / heights
    for c in range(0, field_col_idx) :
        ws.set_column(c, c, 16)
    
    ws.set_column(field_col_idx, field_col_idx, 24)
    
    for r in range(header_height_rows) :
        ws.set_row(r, 20)

    col_map: dict[tuple[Any, Any, Any], int] = {}
    c = first_value_col

    for bank in banks :

        c_bank_start = c
        tids = trade_ids_by_bank.get(bank, [])

        for tid in tids :

            legs = legs_by_bank_tid.get((bank, tid), [])
            c_tid_start = c

            if legs :

                for leg in legs :

                    col_map[(bank, tid, leg)] = c
                    
                    # row 2: tradeLegId (force text)
                    ws.write_string(2, c, "" if leg is None else str(leg), formats["header_leg"])
                    c += 1

            else :
                
                # ensure at least one column for the tradeId
                col_map[(bank, tid, None)] = c
                ws.write_string(2, c, "—", formats["header_leg"]) # "-" for None or Null values
                
                c += 1

            # row 1: tradeId label (force text); avoid merging single cell
            tid_label = "—" if tid is None else str(tid)
            left, right = c_tid_start, c - 1

            if left == right :
                ws.write_string(1, left, tid_label, formats["header_tid"])
            
            else :
                ws.merge_range(1, left, 1, right, tid_label, formats["header_tid"])

        # row 0: bank/counterparty; avoid merging single cell
        if c > c_bank_start :

            bank_label = "—" if bank is None else str(bank)
            left, right = c_bank_start, c - 1
            
            if left == right :
                ws.write_string(0, left, bank_label, formats["header_bank"])
            
            else :
                ws.merge_range(0, left, 0, right, bank_label, formats["header_bank"])

    return col_map


def write_top_left_header_rows (
        
        ws,
        *,
        max_levels: int,
        field_col_idx: int,
        header_height_rows: int,
        fmt_level,   # formats["level"]
        fmt_field,   # formats["field"]
        labels = ["Counterparty", "tradeId", "tradeLegId"]

    ) -> None:
    """
    Leave L1..Lk header cells blank and put 'Counterparty', 'tradeId', 'tradeLegId'
    in the 'Field' column header rows (row 0..2).
    """
    # blank header cells for L1..Lk
    for r in range(header_height_rows):
        for lvl_idx in range(max_levels):
            ws.write(r, lvl_idx, "", fmt_level)

    # labels in the "Field" column for the 3 header rows
    for r, text in enumerate(labels[:header_height_rows]):
        ws.write(r, field_col_idx, text, fmt_field)


def write_label_block(
        
        ws,
        formats: Dict[str, Any],
        rows_plan: List[Tuple[List[str], str, str]],
        *,
        max_levels: int,
        field_col_idx: int,
        header_height_rows: int,
        general_section_name: str = "General Information",
    
    ) -> Tuple[int, int, int]:
    """
    Render the left label block body (no top-left header labels here).
    - Header rows (0..header_height_rows-1): handled elsewhere (write_top_left_header_rows).
    - Body rows: write blank L-cells and the Field label for each row in rows_plan.
    - Then perform a single rectangular merge for the contiguous top 'General Information'
      rows across L columns only (0..field_col_idx-1), merging them upward into the header rows.

    Returns:
        start_row: first body row index
        last_row: last body row index (or start_row-1 if rows_plan empty)
        general_end: last sheet row included in the GI rectangle (or start_row-1 if none)
    """
    # Header rows: L columns are already blanked by write_top_left_header_rows
    start_row = header_height_rows

    # Body rows
    r = start_row

    if rows_plan :

        for _levels, field, _full in rows_plan :

            # L columns (only if they exist)
            if field_col_idx > 0 :

                for lvl_idx in range(max_levels):
                    ws.write(r, lvl_idx, "", formats["level"])
            
            # Field column
            ws.write(r, field_col_idx, field, formats["field"])
            r += 1
        
        last_row = r - 1
    
    else :
        last_row = start_row - 1  # no body

    # Find contiguous top 'General Information' rows
    general_end = start_row - 1

    if rows_plan :

        for i, (levels, _field, _full) in enumerate(rows_plan) :

            if levels and levels[0] == general_section_name :
                general_end = start_row + i
            
            else :
                break

    # Single rectangular merge across L columns only (if any L columns)
    if general_end >= start_row and field_col_idx > 0 :
        ws.merge_range(
            0,                 # top: header row 0
            0,                 # left: first L column
            general_end,       # bottom: last 'General Information' row in sheet coords
            field_col_idx - 1, # right: last L column (exclude Field column)
            general_section_name,
            formats["level"],
        )

    return start_row, last_row, general_end


def merge_level_labels (
        
        ws,
        formats: Dict[str, Any],
        rows_plan: List[Tuple[List[str], str, str]],
        *,
        max_levels: int,
        field_col_idx: int,
        start_row: int,
        last_row: int,
        merge_start: int

    ) -> None:
    """
    Merge repeated labels in the L-columns for rows_plan[start..last], starting at merge_start
    (so we don't overlap the big 'General Information' rectangle that reached into header rows).
    Only merges columns 0..field_col_idx-1 (never the Field column).
    """
    if field_col_idx <= 0:
        return
    
    if not rows_plan or merge_start > last_row :
        return

    def deeper_nonempty (run_start : int, run_end : int, lvl_idx : int) -> bool :
        """
        
        """
        for rr in range(run_start, run_end + 1) :
            
            lv = rows_plan[rr - start_row][0]
            
            if any(x for x in lv[lvl_idx + 1:]) :
                return True
        
        return False

    for lvl_idx in range(max_levels) :

        r0 = merge_start
        curr = rows_plan[r0 - start_row][0][lvl_idx]
        
        for rr in range(merge_start, last_row + 2) :  # sentinel at last_row+1

            val = rows_plan[rr - start_row][0][lvl_idx] if rr <= last_row else None
            
            if val != curr :

                if curr:
                
                    r_end = rr - 1
                
                    # Merge across L columns only (not into Field col)
                    c0, c1 = (lvl_idx, lvl_idx) if deeper_nonempty(r0, r_end, lvl_idx) else (lvl_idx, field_col_idx - 1)
                
                    # avoid "Can't merge single cell"
                    if not (r0 == r_end and c0 == c1) :
                        ws.merge_range(r0, c0, r_end, c1, curr, formats["level"])
                
                r0 = rr
                curr = val



def write_values(
        
        ws,
        formats : Dict[str, Any],
        rows_plan : List[Tuple[List[str], str, str]],
        col_map : Dict[Tuple[Any, Any, Any], int],
        row_by_key : Dict[Tuple[Any, Any, Any], Dict[str, Any]],
        *,
        start_row: int,
    
    ) -> None:
    
    r = start_row
    
    for _levels, _field, full_col in rows_plan :

        write_as_text = is_identifier_col(full_col)
        
        for key, cc in col_map.items() :
            
            rec = row_by_key.get(key)
            
            if rec is None :
                ws.write(r, cc, "—", formats["missing"])
            
            else :

                val = rec.get(full_col, None)
                
                if val is None :
                    ws.write(r, cc, "—", formats["missing"])
                
                else :
                    
                    if write_as_text :
                        ws.write_string(r, cc, str(val), formats["text"])
                    
                    else :
                        
                        if isinstance(val, (int, float)) :
                            ws.write_number(r, cc, float(val), formats["num"])
                        
                        else :
                            ws.write(r, cc, str(val), formats["text"] if isinstance(val, str) else formats["num"])
        r += 1


def save_wide_report_split_by_originating_action(
        
        df: pl.DataFrame,
        out_path: str,
        *,
        action_col: str = "originatingAction",
        report_actions: tuple[str, ...] = ("New", "Early Termination"),
        report_sheet: str = "Trading Report",
        others_sheet: str = "LifeCycle Report"

    ) -> str:
    """
    Write one workbook with two sheets based on originatingAction:
      - report_sheet: rows where action_col ∈ report_actions
      - others_sheet: all remaining rows (including null/missing)
    Leaves `df` unmodified.
    """
    if df is None or df.is_empty() :
        raise ValueError("[-] DataFrame is empty.")

    if action_col in df.columns :

        mask = pl.col(action_col).is_in(list(report_actions))
        
        df_report = df.filter(mask)
        df_others = df.filter(~mask | pl.col(action_col).is_null())
    
    else :

        # action_col missing → everything goes to "LifeCycle Report"; empty "Trading Report"
        df_report = pl.DataFrame(schema=df.schema)
        df_others = df

    # Keep identical column order on both sheets
    cols = df.columns
    
    df_report = df_report.select(cols)
    df_others = df_others.select(cols)

    wb = xlsxwriter.Workbook(out_path)

    try :

        header_fmt    = wb.add_format(
                            {
                                "bold": True, "bg_color": "#F3F4F6", "border": 1, "align": "center"
                            }
                        )
        
        cell_fmt_text = wb.add_format({"border": 1})
        cell_fmt_num  = wb.add_format({"border": 1, "align": "right"})
        cell_fmt_date = wb.add_format({"border": 1})

        ws_report = wb.add_worksheet(report_sheet)
        write_polars_df_sheet(ws_report, df_report,
                               header_fmt=header_fmt,
                               cell_fmt_text=cell_fmt_text,
                               cell_fmt_num=cell_fmt_num,
                               cell_fmt_date=cell_fmt_date)

        ws_others = wb.add_worksheet(others_sheet)
        write_polars_df_sheet(ws_others, df_others,
                               header_fmt=header_fmt,
                               cell_fmt_text=cell_fmt_text,
                               cell_fmt_num=cell_fmt_num,
                               cell_fmt_date=cell_fmt_date)
    finally:
        wb.close()

    return out_path


def save_vertical_trade_report_by_counterparty_dynamic_levels(
        
        df : pl.DataFrame,
        out_path: str,
        *,
        sep: str = ".",
        counterparty_col: str = "counterparty",
        trade_id_col: str = "tradeId",
        leg_id_col: str = "tradeLegId",
        general_section_name: str = "General Information",
        header_height_rows: int = 3,
    
    ) -> str:
    """
    Create a vertical trade report grouped by counterparty → trade ID → trade Leg ID.
    The left block contains hierarchical labels (L1..Lk + Field), the right block
    contains values organized by counterparty, tradeId, and leg.

    Args:
        df: Polars DataFrame with trade data.
        out_path: Absolute path to save the generated Excel file.
        sep: Separator for hierarchical column names (default: ".").
        counterparty_col: Column name containing the counterparty identifier.
        trade_id_col: Column name containing the trade ID.
        leg_id_col: Column name containing the trade leg ID.
        general_section_name: Label used for top-level fields without a namespace.
        header_height_rows: Number of header rows reserved for Bank/TradeId/Leg.

    Returns:
        Path to the saved Excel file.
    """
    import xlsxwriter
    if df is None or df.is_empty():
        raise ValueError("DataFrame is empty.")

    # 1) Build hierarchy maps: banks → tradeIds → legs
    banks, trade_ids_by_bank, legs_by_bank_tid = build_hierarchy_maps(df, counterparty_col, trade_id_col, leg_id_col)

    # 2) Build row plan (L1..Lk + Field) and column indices
    rows_plan, max_levels, field_col_idx, first_value_col, _ = build_rows_plan(df, sep=sep, counterparty_col=counterparty_col,
                                                                               trade_id_col=trade_id_col, leg_id_col=leg_id_col,
                                                                               general_section_name=general_section_name
                                                                            )

    # 3) Index records by (counterparty, tradeId, legId)
    row_by_key = index_records(df, counterparty_col=counterparty_col, trade_id_col=trade_id_col, leg_id_col=leg_id_col)

    # 4) Initialize workbook and formats
    wb = xlsxwriter.Workbook(out_path)
    ws = wb.add_worksheet("Report")
    formats = create_formats(wb)

    write_top_left_header_rows(ws, max_levels=max_levels, field_col_idx=field_col_idx, header_height_rows=header_height_rows,
                               fmt_level=formats["level"], fmt_field=formats["field"]
                               )
    
    # 5) Write column headers (banks / tradeIds / legs)
    col_map = write_headers(ws, formats, banks, trade_ids_by_bank, legs_by_bank_tid, field_col_idx=field_col_idx,
                            header_height_rows=header_height_rows, first_value_col=first_value_col
                            )

    # 6) Write left label block (L1..Lk and Field names)
    start_row, last_row, general_end = write_label_block(ws, formats, rows_plan, max_levels=max_levels, field_col_idx=field_col_idx, 
                                            header_height_rows=header_height_rows
                                            )
    
    merge_start = max(general_end + 1, start_row)

    # 7) Merge hierarchical labels vertically and horizontally
    merge_level_labels(ws, formats, rows_plan, max_levels=max_levels, field_col_idx=field_col_idx, start_row=start_row, last_row=last_row, merge_start=merge_start)

    # 8) Write values into the grid
    write_values(ws, formats, rows_plan, col_map, row_by_key, start_row=start_row)

    wb.close()
    return out_path


def write_vertical_sheet(
    df: pl.DataFrame,
    ws,
    formats: Dict[str, Any],
    *,
    sep: str = ".",
    counterparty_col: str = "counterparty",
    trade_id_col: str = "tradeId",
    leg_id_col: str = "tradeLegId",
    general_section_name: str = "General Information",
    header_height_rows: int = 3,
) -> None:
    """
    Render ONE vertical report into worksheet `ws` using the SAME layout/colors/grouping
    as save_vertical_trade_report_by_counterparty_dynamic_levels.
    """
    if df is None or df.is_empty():
        ws.write(0, 0, "No data for this selection.")
        return

    # 1) hierarchy
    banks, trade_ids_by_bank, legs_by_bank_tid = build_hierarchy_maps(
        df, counterparty_col, trade_id_col, leg_id_col
    )

    # 2) row plan / indices
    rows_plan, max_levels, field_col_idx, first_value_col, _ = build_rows_plan(
        df,
        sep=sep,
        counterparty_col=counterparty_col,
        trade_id_col=trade_id_col,
        leg_id_col=leg_id_col,
        general_section_name=general_section_name,
    )

    # 3) record index
    row_by_key = index_records(
        df,
        counterparty_col=counterparty_col,
        trade_id_col=trade_id_col,
        leg_id_col=leg_id_col,
    )

    # 4) top-left header rows (blank L cols + labels in Field col)
    write_top_left_header_rows(
        ws,
        max_levels=max_levels,
        field_col_idx=field_col_idx,
        header_height_rows=header_height_rows,
        fmt_level=formats["level"],
        fmt_field=formats["field"],
    )

    # 5) right headers
    col_map = write_headers(
        ws,
        formats,
        banks,
        trade_ids_by_bank,
        legs_by_bank_tid,
        field_col_idx=field_col_idx,
        header_height_rows=header_height_rows,
        first_value_col=first_value_col,
    )

    # 6) left label block + GI rectangle
    start_row, last_row, general_end = write_label_block(
        ws,
        formats,
        rows_plan,
        max_levels=max_levels,
        field_col_idx=field_col_idx,
        header_height_rows=header_height_rows,
        general_section_name=general_section_name,
    )

    # 7) merge remaining level labels AFTER GI rectangle
    merge_start = max(general_end + 1, start_row)
    merge_level_labels(
        ws,
        formats,
        rows_plan,
        max_levels=max_levels,
        field_col_idx=field_col_idx,
        start_row=start_row,
        last_row=last_row,
        merge_start=merge_start,
    )

    # 8) values
    write_values(
        ws,
        formats,
        rows_plan,
        col_map,
        row_by_key,
        start_row=start_row,
    )



def save_vertical_split_by_originating_action(
    df: pl.DataFrame,
    out_path: str,
    *,
    action_col: str = "originatingAction",
    report_actions: tuple[str, ...] = ("New", "Early Termination"),
    trading_sheet_name: str = "Trading Report",
    lifecycle_sheet_name: str = "LifeCycle Report",
    sep: str = ".",
    counterparty_col: str = "counterparty",
    trade_id_col: str = "tradeId",
    leg_id_col: str = "tradeLegId",
    general_section_name: str = "General Information",
    header_height_rows: int = 3,
) -> str:
    """
    Create ONE workbook with TWO vertical sheets (same layout/colors/grouping):
      - trading_sheet_name: originatingAction ∈ report_actions
      - lifecycle_sheet_name: all the rest (including nulls)
    """
    import xlsxwriter

    if df is None or df.is_empty():
        raise ValueError("DataFrame is empty.")

    if action_col in df.columns:
        mask = pl.col(action_col).is_in(list(report_actions))
        df_trading = df.filter(mask)
        df_lifecycle = df.filter(~mask | pl.col(action_col).is_null())
    else:
        df_trading = pl.DataFrame(schema=df.schema)  # empty but same schema
        df_lifecycle = df

    wb = xlsxwriter.Workbook(out_path)
    try:
        formats = create_formats(wb)

        # Sheet 1: Trading Report (VERTICAL)
        ws_trading = wb.add_worksheet(trading_sheet_name)
        write_vertical_sheet(
            df_trading,
            ws_trading,
            formats,
            sep=sep,
            counterparty_col=counterparty_col,
            trade_id_col=trade_id_col,
            leg_id_col=leg_id_col,
            general_section_name=general_section_name,
            header_height_rows=header_height_rows,
        )

        # Sheet 2: LifeCycle Report (VERTICAL)
        ws_lifecycle = wb.add_worksheet(lifecycle_sheet_name)
        write_vertical_sheet(
            df_lifecycle,
            ws_lifecycle,
            formats,
            sep=sep,
            counterparty_col=counterparty_col,
            trade_id_col=trade_id_col,
            leg_id_col=leg_id_col,
            general_section_name=general_section_name,
            header_height_rows=header_height_rows,
        )
    finally:
        wb.close()

    return out_path