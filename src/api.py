import os, sys
import time
import polars as pl

from typing import List, Dict, Optional
from src.config import LIBAPI_ABS_PATH

sys.path.append(LIBAPI_ABS_PATH)
from libapi.ice.trade_manager import TradeManager # type: ignore


def create_trade_manager () :
    """
    This function creates an instance of TradeManger from the LibApi
    """
    tm = TradeManager()

    return tm


def load_api_data (
        
        start_date: Optional[str] = None,
        end_date : Optional[str] = None,
        excluded_books: Optional[List[str]] = None,
        date_format: str = "%Y-%m-%d",

    ) -> pl.DataFrame:
    """
    Fetch trades (HV/WR) from `start_date` up to today, then enrich each leg
    with full details. Returns a Polars DataFrame.
    """
    start = time.time()
    tm = create_trade_manager()

    print("\n[*] Getting Trades...")
    all_books_hv = tm.get_all_existing_hv_portfolios() or []
    all_books_wr = tm.get_all_existing_wr_portfolios() or []
    all_books = all_books_hv + all_books_wr

    excluded_set = set(excluded_books or [])
    all_books_filtered = [b for b in all_books if b not in excluded_set]
    print("\n[+] Books available:\n", all_books_filtered)

    print("\n[*] Generating dates to fetch:\n")
    dates = tm.generate_dates(start_date, end_date, format=date_format) or []
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



def rerun_api_call (
        
        start_date: Optional[str] = None,
        books_excluded: Optional[List[str]] = None,
        max_retries: int = 3,
    
    ) -> pl.DataFrame:
    
    attempts = 0
    
    while attempts <= max_retries :

        df = load_api_data(start_date, books_excluded)
        
        if not df.is_empty():
            return df
        
        attempts += 1
        print(f"[!] Empty DataFrame; retry {attempts}/{max_retries}...")
    
    return pl.DataFrame()