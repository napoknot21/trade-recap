from __future__ import annotations

import os, sys
import time
import ast
import json
import threading
import polars as pl
import datetime as dt
#import win32com.client as win32

from dotenv import load_dotenv
from typing import List, Dict, Optional, Any

load_dotenv()

# Global Variables
LIBAPI_ABS_PATH=os.getenv("LIBAPI_ABS_PATH")
DIRECTORY_DATA_ABS_PATH=os.getenv("DIRECTORY_DATA_ABS_PATH")
FILE_BASENAME_EXCEL_SRC=os.getenv("FILE_BASENAME_EXCEL_SRC")
FILE_BASENAME_EXCEL_TARGET=os.getenv("FILE_BASENAME_EXCEL_TARGET")

# LibApi library added
sys.path.append(LIBAPI_ABS_PATH)
from libapi.ice.trade_manager import TradeManager


# Main functions

def load_api_data (
    
        start_date : Optional[str] = None,
        excluded_books : Optional[List[str]] = None,
        format : str = "%Y-%m-%d"
    
    ) -> pl.DataFrame:
    """
    This function fetch trade data from a specific date (to today)

    Args:
        start_date (str | None) : Starting date for the dates generation.
        excluded_books (List[str] | None) : Books / Portfolio to don't consider during the fetch
        format (str) : Default format for the date formating 
    """
    start = time.time()

    tm = TradeManager()
    
    print("\n[*] Getting Trades...")
    all_books_hv = tm.get_all_existing_hv_portfolios()
    all_books_wr = tm.get_all_existing_wr_portfolios()

    # Concate both lists
    all_books = all_books_hv + all_books_wr

    # Filter books to exclude
    all_books_filtered = [book for book in all_books if book not in excluded_books]

    print("\n[+] Books availables found: \n")
    print(all_books_filtered)

    # Generation of dates to consider
    print("\n[*] Searching trades for following dates: \n")
    dates = tm.generate_dates(start_date)
    print(dates)

    # Api Invocation for  trades information
    print("\n[*] Searching Trades from selected books...\n")
    data_raw : Dict = tm.get_trades_from_books_by_date(all_books_filtered, dates)

    # Extract list of trade (with its ID included)
    ids : List[Dict] = data_raw.get("tradeLegs")
    trade_ids : List = [trade['tradeLegId'] for trade in ids]

    # Fetching information by ID
    print("\n[*] Getting information threads...\n")
    info = tm.get_info_trades_from_ids(trade_ids)
    
    # Verifications
    if info is None :

        print("[-] Error during fetching information. Reload the script.")
        return pl.DataFrame()

    if "tradeLegs" not in info :

        print(f"[!] No trade information returned by API")
        return pl.DataFrame()

    # Data convertion to DataFrame
    df = pl.from_dicts(info["tradeLegs"], strict=False)

    print(f"[+] Process done in {time.time() - start:.2f} seconds\n")

    return df


def data_trade_hierarchy_tree (trade_dict : List[Dict]) :
    """
    This function check the trade dictory and create a new dictionary with a tradeId and tradeLegId hierarchy
    """
    tree = {}

    for trade in trade_dict :

        trade_id = trade.get("tradeId")
        trade_leg_id = trade.get("tradeLegId")

        if trade_id in tree.keys() :
            tree[trade_id].add(trade_leg_id)

        else :
            tree[trade_id] =  set()
            tree[trade_id].add(trade_leg_id)

    return tree


def align_columns (df1: pl.DataFrame, df2: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame] :
    """

    """
    # Union of both columns
    all_cols = set(df1.columns) | set(df2.columns)

    for col in all_cols:

        if col not in df1.columns:
            df1 = df1.with_columns(pl.lit(None).alias(col))
            
        if col not in df2.columns:
            df2 = df2.with_columns(pl.lit(None).alias(col))

    # Réordonner les colonnes dans le même ordre pour concat
    df1 = df1.select(sorted(all_cols))
    df2 = df2.select(sorted(all_cols))

    return df1, df2


def rerun_api_call (start_date : Optional[str] = None, books_excluded : Optional[List[str]] = None) :

    df = load_api_data(start_date, books_excluded)

    if df.is_empty() :
        
        print("\n[!] Empty dataframe. Retrying...\n")
        rerun_api_call(books_excluded, start_date)

    return (df)


def _parse_list_of_dicts (s : str) -> list[dict]:
    """
    Robustly parse strings like "[]", "[{'code':'A','value':1}]" (single quotes),
    or real JSON. Returns a Python list[{'code': str, 'value': str}].
    """
    if s is None :
        return []
    
    s = s.strip()
    if s == "" or s == "[]" :
        return []
    
    # Try Python-literal first (handles single quotes, None/True/False)
    try :
        v = ast.literal_eval(s)
    
    except Exception :
        
        # Fallback: try JSON after normalizing quotes
        try :
            v = json.loads(s.replace("'", '"'))
        
        except Exception :
            return []

    out = []
    if isinstance(v, list) :

        for item in v :

            if isinstance(item, dict) :

                code = "" if item.get("code") is None else str(item.get("code"))
                val = "" if item.get("value") is None else str(item.get("value"))
                
                out.append({"code": code, "value": val})
    
    return out


def manage_list_type_column_from_df(df: pl.DataFrame, column: str = "fields") -> pl.DataFrame:
    """

    """
    if df is None or df.is_empty() or column not in df.columns:
        return df

    target_dtype = pl.List(pl.Struct([pl.Field("code", pl.Utf8), pl.Field("value", pl.Utf8)]))

    # Decode string column -> List[Struct{code:str, value:str}] using a Python UDF
    # (works across Polars versions; small perf cost, but robust)
    df2 = df.with_columns(
        pl
        .when(pl.col(column).is_null())
        .then(pl.lit([], dtype=target_dtype))
        .otherwise(
            pl.col(column).map_elements(_parse_list_of_dicts, return_dtype=target_dtype)
        )
        .alias(column)
    )

    # Nothing to widen
    if df2.select(pl.col(column).list.len().sum()).item() == 0 :
        return df2

    # Explode -> pivot wide
    exploded = (

        df2.with_row_index(name="_rowid")
           .explode(column)
           .drop_nulls(column)
           .with_columns(
               pl.col(column).struct.field("code").alias("code"),
               pl.col(column).struct.field("value").alias("value"),
           )
           .select("_rowid", "code", "value")
    
    )

    if exploded.is_empty() :
        return df2

    wide = exploded.pivot(values="value", index="_rowid", on="code", aggregate_function="first")

    out = (
        
        df2
        .with_row_count("_rowid")
        .join(wide, on="_rowid", how="left")
        .drop("_rowid")
    
    )

    # Prefix only newly created columns
    new_cols = [c for c in out.columns if c not in df.columns]
    
    return out.rename({c: f"{column}.{c}" for c in new_cols})


def load_df_from_excel (file_basename : Optional[str]) -> Optional[pl.DataFrame] :
    """
    This function loads trade Recap or Trade information from a excel file
    """

    FILE_ABS_PATH = os.path.join(DIRECTORY_DATA_ABS_PATH, file_basename)
    file_abs_path = "./data/" + file_basename#FILE_ABS_PATH

    df = pl.read_excel(file_abs_path)
    return df


# ---------- Tests ----------

books_excluded: List[str] = ['HV_BONDS_EXO', 'HV_EXO_EQUITY' 'HV_SMART_BETA']

# Information from API
#df = rerun_api_call(books_excluded)

# Loading information from file
df = load_df_from_excel(FILE_BASENAME_EXCEL_SRC)
print(df)

print(df.select(["customFields", "fields", "instrument"]))

new_df = manage_list_type_column_from_df(df, "fields")
print(new_df)

filter_df = manage_list_type_column_from_df(new_df, "customFields")
print(filter_df)

destination_abs_path = os.path.join("./data/", FILE_BASENAME_EXCEL_TARGET)
filter_df.write_excel(destination_abs_path)
