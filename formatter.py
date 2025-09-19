import os, sys
import polars as pl
import datetime as dt
from typing import Dict, List, Optional, Any, Iterable

SEP = "."  # column namespace separator


def save_vertical_report_grouped_by_bank_trade (
        
        df: pl.DataFrame,
        base_dir: Optional[str] = None,
        *,
        base_name: str = "trade-recap-vertical-wide",
        counterparty_col: Optional[str] = None,
        trade_id_col: str = "tradeId",
        leg_id_col: str = "tradeLegId",
        group_sep: str = SEP,
        include_prefixes: Optional[List[str]] = None,   # e.g. ["instrument","fields"]; None = all
        exclude_fields: Optional[Iterable[str]] = None, # full column names to omit
        stamp_fmt: str = "%Y_%m_%dT%H_%M",
        tz: str = "Europe/Luxembourg",
        general_section_name: str = "General Information",

    ) -> str:
    """
    One worksheet:
      Left: variable label columns (Level1 .. LevelK-1, Field) with vertical & horizontal merges.
      Right: columns grouped horizontally by Bank -> TradeId -> TradeLegId (3 header rows).

    Merges:
      - 'General Information' (non-dotted): merged HORIZONTALLY across Level1..(Field-1)
        and VERTICALLY across its rows.
      - Level1 (e.g., 'instrument'): merged VERTICALLY over its rows; if the group has no Level2,
        also merged HORIZONTALLY across Level1..(Field-1).
      - Level2 (joined middle tokens): merged HORIZONTALLY across columns 1..(Field-1)
        and VERTICALLY over its rows.
    """
    if df.is_empty():
        raise ValueError("DataFrame is empty.")

    # --- counterparty column autodetect ---
    if counterparty_col is None:
        lc = {c.lower(): c for c in df.columns}
        guess = next((lc[c] for c in lc if "counterparty" in c), None)
        if not guess:
            raise ValueError("counterparty_col not provided and no *counterparty* column found.")
        counterparty_col = guess

    exclude_fields = set(exclude_fields or [])

    # --- helpers to classify columns ---
    def is_namespaced(c: str) -> bool:
        return group_sep in c

    scalar_mask = {
        c: (not isinstance(dt, pl.Struct)) and (not isinstance(dt, pl.List))
        for c, dt in df.schema.items()
    }

    # Non-dotted = General Information candidates (exclude ids/cp)
    general_all = [
        c for c in df.columns
        if (not is_namespaced(c))
        and c not in {trade_id_col, leg_id_col, counterparty_col}
        and c not in exclude_fields
        and scalar_mask.get(c, True)
    ]

    # Dotted candidates (scalar only)
    namespaced_all = [
        c for c in df.columns
        if is_namespaced(c) and c not in exclude_fields and scalar_mask.get(c, True)
    ]

    def lvl1_of(c: str) -> str:
        return c.split(group_sep, 1)[0]

    if include_prefixes is not None:
        keep = set(include_prefixes)
        namespaced_all = [c for c in namespaced_all if lvl1_of(c) in keep]

    # --- dynamic label width (Level columns + Field) ---
    def group_depth(col: str) -> int:
        if not is_namespaced(col):
            return 1  # General is a 1-level group
        return max(1, len(col.split(group_sep)) - 1)

    max_group_depth = 1
    if namespaced_all:
        max_group_depth = max(max_group_depth, max(group_depth(c) for c in namespaced_all))
    label_cols = max_group_depth + 1
    field_col_idx = label_cols - 1

    # --- timestamped path ---
    try:
        from zoneinfo import ZoneInfo
        now = dt.datetime.now(ZoneInfo(tz))
    except Exception:
        now = dt.datetime.now()
    stamp = now.strftime(stamp_fmt)
    out_dir = base_dir or (globals().get("DIRECTORY_DATA_ABS_PATH") or "./data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{base_name}_{stamp}.xlsx")

    # --- workbook / formats ---
    import xlsxwriter
    wb = xlsxwriter.Workbook(out_path, {"constant_memory": True})

    fmt_title     = wb.add_format({"bold": True, "align": "center", "valign": "vcenter",
                                   "bg_color": "#0F766E", "font_color": "#FFFFFF", "border": 1})
    fmt_header    = wb.add_format({"bold": True, "align": "center", "valign": "vcenter",
                                   "bg_color": "#1F2937", "font_color": "#FFFFFF", "border": 1})
    fmt_group     = wb.add_format({"bold": True, "align": "center", "valign": "vcenter",
                                   "bg_color": "#374151", "font_color": "#FFFFFF", "border": 1})
    fmt_field     = wb.add_format({"bold": True, "align": "left",   "valign": "vcenter",
                                   "bg_color": "#E5E7EB", "border": 1})
    fmt_even      = wb.add_format({"bg_color": "#FFFFFF", "border": 1})
    fmt_odd       = wb.add_format({"bg_color": "#F9FAFB", "border": 1})
    fmt_num_even  = wb.add_format({"bg_color": "#FFFFFF", "border": 1, "num_format": "0.00"})
    fmt_num_odd   = wb.add_format({"bg_color": "#F9FAFB", "border": 1, "num_format": "0.00"})
    fmt_int_even  = wb.add_format({"bg_color": "#FFFFFF", "border": 1, "num_format": "0"})
    fmt_int_odd   = wb.add_format({"bg_color": "#F9FAFB", "border": 1, "num_format": "0"})
    fmt_date_even = wb.add_format({"bg_color": "#FFFFFF", "border": 1, "num_format": "yyyy-mm-dd"})
    fmt_date_odd  = wb.add_format({"bg_color": "#F9FAFB", "border": 1, "num_format": "yyyy-mm-dd"})

    def _dtype_category(dtype: pl.DataType) -> str:
        s = str(dtype)
        if s.startswith(("Int", "UInt")):      return "int"
        if s.startswith(("Float", "Decimal")): return "float"
        if s.startswith("Datetime"):           return "datetime"
        if s.startswith("Date"):               return "date"
        return "other"

    def fmt_by_dtype(dtype: pl.DataType, row_idx: int):
        odd = (row_idx % 2 == 1)
        cat = _dtype_category(dtype)
        if cat == "int":               return fmt_int_odd  if odd else fmt_int_even
        if cat == "float":             return fmt_num_odd  if odd else fmt_num_even
        if cat in ("date","datetime"): return fmt_date_odd if odd else fmt_date_even
        return fmt_odd if odd else fmt_even

    # --- sheet ---
    ws = wb.add_worksheet("Trades")

    # label widths
    for i in range(label_cols):
        ws.set_column(i, i, 22 if i == 0 else (30 if i == field_col_idx else 24))

    # --- build column layout on the right (Bank → Trade → Legs) ---
    banks = df.select(pl.col(counterparty_col)).unique().to_series().to_list()
    banks = [b for b in banks if b is not None]

    # Lists for header ranges and per-column (bank, trade, leg) address
    bank_ranges: Dict[Any, tuple[int, int]] = {}
    trade_ranges: Dict[tuple[Any, Any], tuple[int, int]] = {}
    col_positions: list[tuple[Any, Any, Any, int]] = []  # (bank, trade, leg, col_index)

    col_ptr = label_cols

    # Cache blocks to avoid re-filtering a lot
    df_by_bank: Dict[Any, pl.DataFrame] = {}
    legs_by_trade: Dict[tuple[Any, Any], List[Any]] = {}

    for bank in banks:
        bdf = df.filter(pl.col(counterparty_col) == bank)
        df_by_bank[bank] = bdf

        trades = bdf.select(pl.col(trade_id_col)).unique().to_series().to_list()
        bank_start = col_ptr

        for tid in trades:
            block = bdf.filter(pl.col(trade_id_col) == tid).sort(leg_id_col)
            legs = block.select(pl.col(leg_id_col)).to_series().to_list()
            legs_by_trade[(bank, tid)] = legs

            t_start = col_ptr
            for leg in legs:
                col_positions.append((bank, tid, leg, col_ptr))
                col_ptr += 1
            t_end = max(t_start, col_ptr - 1)
            trade_ranges[(bank, tid)] = (t_start, t_end)

        bank_end = max(bank_start, col_ptr - 1)
        bank_ranges[bank] = (bank_start, bank_end)

    # --- three header rows (0..2) across all right-side columns ---
    ws.set_row(0, 22)
    ws.set_row(1, 20)
    ws.set_row(2, 18)

    # Row 0: Bank merges
    for c in range(label_cols):
        ws.write(0, c, "", fmt_title)
    for bank, (c0, c1) in bank_ranges.items():
        if c1 > c0:
            ws.merge_range(0, c0, 0, c1, str(bank), fmt_title)
        else:
            ws.write(0, c0, str(bank), fmt_title)

    # Row 1: TradeId merges per bank
    for c in range(label_cols):
        ws.write(1, c, "", fmt_header)
    for (bank, tid), (c0, c1) in trade_ranges.items():
        if c1 > c0:
            ws.merge_range(1, c0, 1, c1, str(tid), fmt_header)
        else:
            ws.write(1, c0, str(tid), fmt_header)

    # Row 2: TradeLegId
    for c in range(label_cols - 1):
        ws.write(2, c, "", fmt_group)
    ws.write(2, field_col_idx, "TradeLegId", fmt_field)
    for _, _, leg, c in col_positions:
        ws.write(2, c, str(leg), fmt_header)

    # --- value cache: map (bank, tid, col) -> {leg: value} on demand ---
    value_cache: Dict[tuple[Any, Any, str], Dict[Any, Any]] = {}

    def legmap(bank, tid, col) -> Dict[Any, Any]:
        key = (bank, tid, col)
        if key not in value_cache:
            bdf = df_by_bank[bank]
            block = bdf.filter(pl.col(trade_id_col) == tid)
            if col not in block.columns:
                value_cache[key] = {}
            else:
                small = block.select(pl.col(leg_id_col), pl.col(col)).to_dict(as_series=False)
                lids = small[leg_id_col]
                vals = small[col]
                value_cache[key] = {lids[k]: vals[k] for k in range(len(lids))}
        return value_cache[key]

    # --- build left-side row plan (order of rows) ---
    sch = dict(df.schema)

    def is_scalar_col(c: str) -> bool:
        dt = sch.get(c)
        return (dt is not None) and (not isinstance(dt, pl.Struct)) and (not isinstance(dt, pl.List))

    # tree: L1 -> L2 -> [full cols]
    from collections import defaultdict
    tree: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for c in namespaced_all:
        if not is_scalar_col(c):
            continue
        tokens = c.split(group_sep)
        if len(tokens) == 2:
            L1 = tokens[0]; L2 = ""
        else:
            L1 = tokens[0]; L2 = group_sep.join(tokens[1:-1]) if len(tokens) > 2 else ""
        tree[L1][L2].append(c)

    # Ordered list of rows to write
    rows_plan: list[tuple[str, str, str]] = []  # (L1, L2, full_col_name)
    # General first
    for c in sorted(general_all):
        rows_plan.append((general_section_name, "", c))
    # Then namespaced
    for L1 in sorted(tree.keys()):
        # empty L2 first, then alphabetical
        L2_keys = ([""] if "" in tree[L1] else []) + sorted(k for k in tree[L1] if k != "")
        for L2 in L2_keys:
            fields = sorted(tree[L1][L2], key=lambda x: x.split(group_sep)[-1])
            for col in fields:
                rows_plan.append((L1, L2, col))

    # --- write rows + do merges for label columns ---
    row = 3
    # Track merge ranges
    general_start = None
    L1_active = None; L1_start = None; L1_has_nonempty_L2 = False
    L2_active = None; L2_start = None

    def flush_L2():
        nonlocal L2_active, L2_start, row
        if L2_active is not None and L2_active != "":
            # merge Level2 horizontally 1..(Field-1) and vertically across its rows
            if row - 1 >= L2_start:
                ws.merge_range(L2_start, 1, row - 1, field_col_idx - 1, L2_active, fmt_group)
        L2_active = None; L2_start = None

    def flush_L1():
        nonlocal L1_active, L1_start, L1_has_nonempty_L2, row
        if L1_active is not None:
            if row - 1 >= L1_start:
                # vertical merge for Level1 in column 0
                ws.merge_range(L1_start, 0, row - 1, 0, L1_active, fmt_group)
                # if no Level2, also merge horizontally across 0..(Field-1)
                if not L1_has_nonempty_L2:
                    ws.merge_range(L1_start, 0, row - 1, field_col_idx - 1, L1_active, fmt_group)
        L1_active = None; L1_start = None; L1_has_nonempty_L2 = False

    # write each data row
    for (L1, L2, full_col) in rows_plan:

        # start/advance General block range
        if L1 == general_section_name:
            if general_start is None:
                general_start = row
        else:
            # if we are leaving General, flush its horizontal+vertical merge
            if general_start is not None:
                ws.merge_range(general_start, 0, row - 1, field_col_idx - 1, general_section_name, fmt_group)
                general_start = None

        # manage L1/L2 merge windows
        if L1 != general_section_name:
            if L1 != L1_active:
                # closing previous L2/L1
                flush_L2()
                flush_L1()
                # start new L1
                L1_active = L1
                L1_start = row
                L2_active = None; L2_start = None
                L1_has_nonempty_L2 = False

            if L2 != L2_active:
                # close old L2
                flush_L2()
                # open new L2 (could be empty)
                L2_active = L2
                L2_start = row
                if L2 != "":
                    L1_has_nonempty_L2 = True

        # left labels:
        # when we finally merge, the big L1/L2 labels will fill; here we only put the Field
        field_label = full_col.split(group_sep)[-1] if is_namespaced(full_col) else full_col
        ws.write(row, field_col_idx, field_label, fmt_field)

        # values across all (bank, trade, leg) columns
        dtype = df.schema.get(full_col, pl.Utf8)
        for (bank, tid, leg, col_index) in col_positions:
            m = legmap(bank, tid, full_col)
            v = m.get(leg, None)
            ws.write(row, col_index, v, fmt_by_dtype(dtype, row - 3))

        row += 1

    # flush pending merges after loop
    if general_start is not None:
        ws.merge_range(general_start, 0, row - 1, field_col_idx - 1, general_section_name, fmt_group)
    flush_L2()
    flush_L1()

    ws.freeze_panes(3, label_cols)
    wb.close()
    print(f"[+] Wrote Excel to: {out_path}")
    return out_path
