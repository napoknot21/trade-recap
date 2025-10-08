from __future__ import annotations

import os
import sys
import json
import argparse
import polars as pl
import datetime as dt

from typing import List, Dict, Any, Tuple, Optional

from src.config import RECAP_DATA_ABS_DIR, SEP, RECAP_RAW_DATA_ABS_DIR, DEFAULT_EXCLUDED_BOOKS_LIST
from src.config import *
from src.api import load_api_data
from src.fields import manage_list_type_column_from_df
from src.flatten import flatten_struct_like_columns_routed
from src.utils import drop_struct_and_liststruct_columns
from src.excel import save_df_timestamped_excel, save_vertical_split_by_originating_action
from src.outlook import create_email_item, save_email_item, generate_timestamped_name

def parse_args (argv: List[str] | None = None) -> argparse.Namespace:
    """
    CLI for the trade recap pipeline.

    Examples:
        python -m trade_recap.main --start-date 2025-09-22
        python -m trade_recap.main --exclude HV_BONDS_EXO HV_EXO_EQUITY --no-vertical
        python -m trade_recap.main --list-routes routes.json --base-dir "D:/exports"
    """
    p = argparse.ArgumentParser(description="Generate wide and vertical trade recap reports from ICE data.")

    p.add_argument(
        "--start-date", default=None, help="YYYY-MM-DD (defaults to ICE helper)."
    )
    
    p.add_argument(
        "--exclude", nargs="*", default=DEFAULT_EXCLUDED_BOOKS_LIST, help="Books to exclude (space-separated)."
    )

    p.add_argument(
        "--list-routes", default=None, help="Path to a JSON file defining LIST flatten routes (pattern/strategy/list_max/join_delim)."
    )

    p.add_argument(
        "--base-dir", default=None, help="Output directory. Defaults to DIRECTORY_DATA_ABS_PATH env or ./data."
    )

    p.add_argument(
        "--no-vertical", action="store_true", help="Skip the vertical (styled) report generation."
    )

    p.add_argument(
        "--sheet-name", default="Report",
        help="Sheet name for the vertical report (default: Report)."
    )
    p.add_argument(
        "--no-draft", action="store_true",
        help="Do not open an Outlook draft after saving reports."
    )
    # Optional: override email subject
    p.add_argument(
        "--subject", default=None,
        help="Override email subject. Default includes the run date."
    )

    return p.parse_args(argv)


def with_optional_casts(
        
        df: pl.DataFrame,
        *,
        date_cols: List[Tuple[str, str]] = None,  # [(column, "%Y-%m-%d"), ...]
        float_cols: List[str] = None,

    ) -> pl.DataFrame:
    """
    Cast only existing columns.
    - Dates: cast to Utf8 first (safe), then parse with the given format.
    - Floats: cast to Float64 (strict=False).
    """
    date_cols = date_cols or []
    float_cols = float_cols or []

    exprs: List[pl.Expr] = []

    # Dates
    for col, fmt in date_cols:
        if col in df.columns:
            exprs.append(
                pl.col(col)
                .cast(pl.Utf8, strict=False)                # safe for any type
                .str.strptime(pl.Date, strict=False, format=fmt)
                .alias(col)
            )

    # Floats
    for col in float_cols:
        if col in df.columns:
            exprs.append(
                pl.col(col).cast(pl.Float64, strict=False).alias(col)
            )

    return df.with_columns(exprs) if exprs else df



def load_routes_from_file (path: str | None) -> List[Dict[str, Any]] :
    """
    Load LIST flattening routes from a JSON file.
    If not provided or file missing/invalid, return sensible defaults.
    """
    default_routes = [
        {"pattern": "*.underlyingAssets", "strategy": "explode"},
        {"pattern": "*.tags", "strategy": "join", "join_delim": ", "},
        {"pattern": "*.barriers", "strategy": "index", "list_max": 10},
    ]
    if not path:
        return default_routes
    
    try :

        with open(path, "r", encoding="utf-8") as f:
            routes = json.load(f)
        
        if not isinstance(routes, list) :

            print(f"[!] list-routes file '{path}' did not contain a list; using defaults.")
            return default_routes
        
        return routes
    
    except Exception as e :

        print(f"[!] Failed to read routes file '{path}': {e}; using defaults.")
        return default_routes
    

def run (argv : Optional[List[str]] = None) -> None :
    """
    
    """
    args = parse_args(argv)

    base_dir = args.base_dir or RECAP_DATA_ABS_DIR or "./data"
    raw_dir = RECAP_RAW_DATA_ABS_DIR or "./data/raw"
    os.makedirs(base_dir, exist_ok=True)
    
    print(f"[*] Output directory: {base_dir}")

    # Pull from API
    df = load_api_data(start_date=args.start_date, excluded_books=args.exclude)
    
    if df.is_empty() :
        print("\n[!] No data returned from API. Exiting...\n")
        return

    # Wide columns for fields/customFields
    df = manage_list_type_column_from_df(df, "fields")
    df = manage_list_type_column_from_df(df, "customFields")

    # Deep flatten with LIST routes
    list_routes = load_routes_from_file(args.list_routes)
    
    df = flatten_struct_like_columns_routed(df, columns=["instrument", "premium", "settlement"], sep=SEP, parse_strings=True,
                                            infer_json_rows=None, drop_source=False, max_depth=100, default_list_strategy="index",
                                            default_list_max=30, routes=list_routes
                                            )

    # 4) Optional type casts (safe / non-strict)
    df = with_optional_casts(
        df,
        date_cols=[
            ("instrument.deliveryDate", "%Y-%m-%d"),
            ("instrument.expiryDate", "%Y-%m-%d"),
            ("instrument.tradeDate", "%Y-%m-%d"),
            ("instrument.premiumDate", "%Y-%m-%d"),
        ],
        float_cols=[
            "instrument.notional",
            "instrument.strike",
        ],
    )

    print(f"{df}\n")

    # 6) Export wide report
    wide_path = save_df_timestamped_excel(df, base_dir=raw_dir, base_name="trade-recap")
    #print(f"[+] Raw report saved: {wide_path}")

    # 5) Drop remaining Struct/List[Struct] to keep Excel wide sheet clean
    df = drop_struct_and_liststruct_columns(df, verbose=True)

    # 7) Optional vertical report
    if not args.no_vertical :
        date_name = generate_timestamped_name()
        full_name = "trade_recap_" + date_name + ".xlsx"

        vertical_path = os.path.join(base_dir, full_name)
        # The report function uses its own palette and layout; no sheet-name param needed here,
        # but you can add it to the function if you want per-CLI control.
        """
        vertical_path = save_vertical_trade_report_by_counterparty_dynamic_levels(df, out_path=vertical_path, sep=".",
                                                                                  counterparty_col="counterparty", trade_id_col="tradeId",
                                                                                  leg_id_col="tradeLegId", general_section_name="General Information"
                                                                                  )
        """
        vertical_path = save_vertical_split_by_originating_action(
            df,
            out_path=vertical_path,
            action_col="originatingAction",
            report_actions=("New", "Early Termination"),
            trading_sheet_name="Trading Report",
            lifecycle_sheet_name="LifeCycle Report",

        )
        

        print(f"[+] Vertical report saved: {vertical_path}\n")

    else :
        print("[*] Skipped vertical report (per --no-vertical).")

    # Build attachments list (only existing files)
    attachments: List[str] = []
    
    """
    if wide_path and os.path.isfile(wide_path) :
        attachments.append(wide_path)
    """

    if vertical_path and os.path.isfile(vertical_path) :
        attachments.append(vertical_path)

    # 9) Open Outlook draft with recap (never auto-send)
    if args.no_draft :

        print("[*] Skipped Outlook draft (per --no-draft).")
        return

    # Subject line: CLI override or default including run date (or start date if provided)
    run_date = (args.start_date or dt.date.today().isoformat())
    subject = args.subject or f"Trade Recap — {run_date}"

    try :

        mail_1 = create_email_item(
            
            to_email=EMAIL_DEFAULT_TO,                 # falls back to EMAIL_DEFAULT_TO
            cc_email=EMAIL_DEFAULT_CC,                 # falls back to EMAIL_DEFAULT_CC
            from_email=None,               # falls back to EMAIL_DEFAULT_FROM if configured
            subject=subject,
            dataframe=df,                  # let outlook.py build the recap HTML from DF
            attachments=attachments,
            display=True,                  # open compose window
            #place_html_above_signature=False,
        )

        # Optionally save a .msg copy on disk (Drafts folder)
        save_status_1 = save_email_item(mail_1)  # uses RECAP_EMAIL_ABS_DIR

        mail_2 = create_email_item(
            
            to_email=EMAIL_ALTERNATIVE_TO,                 # falls back to EMAIL_ALTERNATIVE_TO
            cc_email=EMAIL_ALTERNATIVE_CC,                 # falls back to EMAIL_ALTERNATIVE_CC
            from_email=EMAIL_ALTERNATIVE_FROM,               # falls back to EMAIL_DEFAULT_FROM if configured
            subject=subject,
            intro=EMAIL_ALTERNATIVE_BODY_INTRO,
            dataframe=df,                  # let outlook.py build the recap HTML from DF
            attachments=attachments,
            display=True,                  # open compose window
            #place_html_above_signature=False,
        )

        save_status_2 = save_email_item(mail_2)

        if save_status_1.get("success") and save_status_2.get("success") :
            print(f"[+] Drafts saved: {save_status_1.get('path')}")

        else :
            print(f"[!] Draft not saved: {save_status_1.get('message')}")
            
    except Exception as e:

        print(f"[!] Could not open Outlook draft: {e}")
        # Do not exit with error; reports are already saved.


if __name__ == "__main__":
    run()