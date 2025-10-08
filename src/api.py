from __future__ import annotations

import sys
import time
import polars as pl
import datetime as dt

from functools import lru_cache
from typing import List, Dict, Optional, Sequence, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config import LIBAPI_ABS_PATH

# LibApi import
sys.path.append(LIBAPI_ABS_PATH)
from libapi.ice.trade_manager import TradeManager # type: ignore


@lru_cache(maxsize=1)
def create_trade_manager() -> TradeManager:
    """
    Create (and cache) a single `TradeManager` instance.

    Returns:
        TradeManager: A (cached) instance of the LibApi `TradeManager`.

    Notes:
        - Cached with `lru_cache(maxsize=1)` to avoid re-initialization overhead.
        - If `TradeManager` is not thread-safe in your environment, avoid sharing
          this instance across threads, or disable parallel options below.
    """
    return TradeManager()


def load_api_data (
        
        start_date: Optional[str] = None,
        end_date : Optional[str] = None,
        excluded_books: Optional[List[str]] = None,
        date_format: str = "%Y-%m-%d",
        *,
        parallel_info_fetch : bool = False,
        info_chunk_size : int = 500,
        max_workers : Optional[int] = None,

    ) -> pl.DataFrame:
    """
    Fetch trades (HV/WR) between `start_date` and `end_date` (inclusive), then
    enrich each leg with full details. Returns a Polars DataFrame.

    Args:
        start_date (str | None) : Start date as string formatted by `date_format`.
        end_date (str | None) : End date as string formatted by `date_format`. If None, the underlying API default to "today".
        excluded_books (List[str] | None): Portfolio/book names to exclude (case-sensitive).
        date_format (str) : Format string passed to `TradeManager.generate_dates`.
        parallel_info_fetch (bool | None) : If True, fetch trade-leg details concurrently in chunks (threaded).
        info_chunk_size (int) : Number of trade IDs per detail request when parallelizing.
        max_workers (int | None) : Max threads for parallel detail calls. If None, Python chooses.

    Returns:
        pl.DataFrame: Enriched trade-leg details; empty DataFrame if nothing found.

    Raises:
        RuntimeError: If the API returns inconsistent structures that cannot be parsed.
    """
    t0 = time.time()
    tm = create_trade_manager()

    print("\n[*] Getting Trades...\n")
    
    # Collect candidate books (HV + WR) and filter
    all_books_hv = tm.get_all_existing_hv_portfolios() or []
    all_books_wr = tm.get_all_existing_wr_portfolios() or []

    all_books = list(dict.fromkeys(all_books_hv + all_books_wr))  # dedupe, preserve order

    excluded = set(excluded_books or ())

    if excluded:
        all_books = [b for b in all_books if b not in excluded]
    
    if not all_books :

        # Early return to avoid unnecessary API calls
        return pl.DataFrame()
    
    print(f"\n[*] Books available : \n\n{all_books}\n")

    dates : List[str] = tm.generate_dates(start_date, end_date, format=date_format) or []
    print(f"[*] Generating dates to fetch: {dates}\n")
    
    if not dates :

        print("[-] No dates available. Returning empty DataFrame.\n")
        return pl.DataFrame()
    
    print("[*] Fetching trade legs...\n")

    # Fetch raw trade legs
    data_raw : Dict = tm.get_trades_from_books_by_date(all_books, dates) or {}
    
    legs: List[Dict] = data_raw.get("tradeLegs") or []
    
    if not legs :

        print("[!] No trade legs returned by API.")
        return pl.DataFrame()

    # Gather trade_leg_ids
    trade_ids : List = [t.get("tradeLegId") for t in legs if t.get("tradeLegId") is not None]
    
    if not trade_ids :

        print("[-] No valid tradeLegId found.")
        return pl.DataFrame()

    # Fetch detailed info for each trade-leg
    info: Dict
    if parallel_info_fetch :

        # Threaded fan-out in chunks for I/O-bound APIs
        info = _fetch_trade_info_parallel(tm, trade_ids, chunk_size=info_chunk_size, max_workers=max_workers)
    
    else :
        info = tm.get_info_trades_from_ids(trade_ids) or {}

    trade_legs = info.get("tradeLegs")
    
    if not trade_legs :
        return pl.DataFrame()

    # Materialize Polars DataFrame
    # strict=False allows missing keys across dicts
    df = pl.from_dicts(trade_legs, strict=False)

    print(f"\n[+] [trade-recap] load_api_data done in {time.time() - t0:.2f}s "
          f"(books={len(all_books)}, dates={len(dates)}, legs={len(trade_ids)})\n")

    return df


def rerun_api_call (
        
        start_date : Optional[str | dt.datetime] = None,
        end_date : Optional[str | dt.datetime] = None,
        books_excluded : Optional[List[str]] = None,
        date_format : str = "%Y-%m-%d",
        *,
        max_retries : int = 3,
        retry_sleep_sec : float = 1.5,
        backoff : float = 1.6,
        parallel_info_fetch : bool = False,
        info_chunk_size : int = 500,
        max_workers : Optional[int] = None,

    ) -> pl.DataFrame:
    """
    Retry wrapper around `load_api_data` to mitigate transient API emptiness.

    Args:
        start_date: Start date for the fetch.
        end_date: End date for the fetch.
        excluded_books: Portfolio/book names to exclude.
        date_format: Date format string passed to `load_api_data`.
        max_retries: Maximum number of retries (>= 0). 0 means try once.
        retry_sleep_sec: Seconds to sleep between retries (exponential backoff applied).
        parallel_info_fetch: Forwarded to `load_api_data`.
        info_chunk_size: Forwarded to `load_api_data`.
        max_workers: Forwarded to `load_api_data`.

    Returns:
        pl.DataFrame: The first non-empty DataFrame returned by `load_api_data`,
            or an empty DataFrame if all attempts return empty.
    """
    attempts = 0
    sleep = retry_sleep_sec

    while attempts <= max_retries :

        df = load_api_data(start_date, end_date, books_excluded, date_format, parallel_info_fetch, info_chunk_size, max_workers)
        
        if not df.is_empty():
            return df
        
        attempts += 1
        if attempts > max_retries:
            break
        
        print(f"[!] Empty DataFrame; retry {attempts}/{max_retries} after {sleep:.1f}s...")
        
        time.sleep(sleep)
        sleep *= backoff  # gentle backoff
    
    return pl.DataFrame()


def _chunked (iterable : Sequence, n : int) -> Iterable[Sequence] :
    """
    Yield successive chunks of size n from `iterable`.
    """
    if n <= 0 :

        yield iterable
        return
    
    for i in range(0, len(iterable), n) :
        yield iterable[i : i + n]


def _fetch_trade_info_parallel (tm : TradeManager, trade_ids : Sequence, chunk_size : int = 500, max_workers : Optional[int] = None) -> Dict :
    """
    Concurrently fetch trade info for IDs in chunks using threads.

    Args:
        tm: A `TradeManager` instance (assumed thread-safe for read calls).
        trade_ids: Sequence of trade-leg IDs.
        chunk_size: Batch size per API call.
        max_workers: Max threads; None lets `ThreadPoolExecutor` decide.

    Returns:
        Dict: Aggregated response with key "tradeLegs": List[Dict].

    Notes:
        - This assumes `tm.get_info_trades_from_ids` accepts a list of IDs and
          that multiple concurrent calls are OK server-side.
        - If the API enforces strict rate limits, reduce `max_workers` or
          increase `chunk_size`.
    """
    results : List[Dict] =  []

    with ThreadPoolExecutor(max_workers=max_workers) as ex :

        futures = [
            
            ex.submit(tm.get_info_trades_from_ids, list(chunk))
            for chunk in _chunked(list(trade_ids), chunk_size)

        ]

        for fut in as_completed(futures) :

            data = fut.results() or {}
            legs = data.get("tradeLegs") or []

            if legs :
                results.extend(legs)

    return {"tradeLegs" : results}
