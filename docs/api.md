# API module

This document refers to [`api.py`](../src/api.py)

it connects directly with the `libapi` library for accesing to trade informations.

This doc explains what the module does, how the main functions work, why certain design choices were made, and how to use/extend it safely. It also points out a few small issues to fix.

---

## TL;DR

- **Goal:** fetch ICE trades (HV/WR) for a date range, then **enrich each trade leg** with full details, and return a **Polars DataFrame**.

- **Core functions:**
  
  - `create_trade_manager()` — builds and caches a single `TradeManager` instance.
  
  - `load_api_data()` — fetches books & dates, pulls raw trade legs, then loads detailed info; supports **parallel fan-out**.
  
  - `rerun_api_call()` — retries `load_api_data()` with **exponential backoff** until a non-empty DataFrame is obtained.
  
  - Private helpers: `_chunked()` and `_fetch_trade_info_parallel()`.

- **Performance knobs:** `parallel_info_fetch`, `info_chunk_size`, `max_workers`.

---

## Module Overview

```python
from functools import lru_cache
from libapi.ice.trade_manager import TradeManager
```

- `TradeManager` is imported from **LibApi**, whose absolute path is added to 
`sys.path` using `LIBAPI_ABS_PATH` from your project config.

- `polars` is used for efficient tabular data handling.

- The module intentionally prints a few progress messages (useful for CLI runs and logs).

---

## Design Choices & Rationale

### 1) **Singleton-like `TradeManager` via `lru_cache`**

```python
@lru_cache(maxsize=1)
def create_trade_manager() -> TradeManager:
    return TradeManager()
```

- Avoids repeated expensive initializations and auth handshakes.

- Keeps the module stateless from the caller’s perspective while reusing the underlying client.

- **Caveat:** If `TradeManager` is **not thread-safe**, do **not** share it across threads; disable `parallel_info_fetch` (or instantiate one per thread).

### 2) **Two-step data retrieval**

1. **Discovery layer:** enumerate candidate **books** (HV + WR) and **dates**.

2. **Detail layer:** fetch raw trade legs, then **enrich** via `get_info_trades_from_ids`.

This keeps network payloads sane and gives a place to insert batching/parallelism.

### 3) **Parallel fan-out for details**
- API calls that fetch leg details are **I/O-bound** → threading improves throughput.
- Chunking (`info_chunk_size`) + `ThreadPoolExecutor` balances rate limits vs. speed.

### 4) **Defensive emptiness checks**
- Early returns with **empty DataFrames** avoid downstream exceptions when:
  - No books (after exclusion),
  - No dates generated,
  - API returns no legs,
  - Legs without `tradeLegId`.

### 5) **`rerun_api_call()` retry wrapper**
- Real-world APIs can be flaky or briefly empty → automatic **retries with backoff** improve robustness in unattended jobs.

---

## Function-by-Function Walkthrough

### `create_trade_manager()`
Creates and caches a `TradeManager`. Subsequent calls reuse the same instance.

**When to use:** you rarely call this directly; `load_api_data()` does it for you. It’s exposed primarily for testing or advanced use.

---

### `load_api_data(...) -> pl.DataFrame`
**Purpose:** Core routine to fetch and enrich trades into a Polars DataFrame.

**Key steps:**

1. **Instantiate/Reuse** `TradeManager` via `create_trade_manager()`.

2. **Enumerate books :**

   - `get_all_existing_hv_portfolios()` + `get_all_existing_wr_portfolios()`

   - Merge, **dedupe (order-preserving)**, and apply `excluded_books`.

3. **Generate dates :**

   - `tm.generate_dates(start_date, end_date, format=date_format)`

   - If empty → return `pl.DataFrame()`.

4. **Fetch raw legs :**
   
   - `tm.get_trades_from_books_by_date(all_books, dates)` → `{ "tradeLegs": [...] }`.
   
   - Extract `tradeLegId` list; return empty if none.

5. **Enrich legs :**

   - Non-parallel path: one call to `tm.get_info_trades_from_ids(trade_ids)`.

   - Parallel path: `_fetch_trade_info_parallel(...)` which **chunks** and **threads** the calls.

6. **Materialize DataFrame :**

   - `pl.from_dicts(trade_legs, strict=False)` tolerates missing keys among legs.

7. **Log timing** and return the DataFrame.

**Important parameters :**

- `excluded_books: list[str] | None` — exact names to filter out.

- `date_format: str` — must match `TradeManager.generate_dates` expectations.

- `parallel_info_fetch: bool` — set to `True` to enable threaded detail calls.

- `info_chunk_size: int` — batch size for each parallel call.

- `max_workers: int | None` — threads; when `None`, Python picks a default.

**Returns:** a (possibly empty) **Polars DataFrame** with one row per **enriched trade leg**.

---

### `rerun_api_call(...) -> pl.DataFrame`

**Purpose:** Convenience wrapper that retries `load_api_data()` up to `max_retries` times with **exponential backoff** until a **non-empty** DataFrame is produced.

**Algorithm:**
```python
attempts = 0
sleep = retry_sleep_sec

while attempts <= max_retries :

    df = load_api_data(...)

    if not df.is_empty():
        return df
    
    attempts += 1
    
    if attempts > max_retries:
        break
    
    time.sleep(sleep)
    sleep *= backoff

return pl.DataFrame()
```

**Typical use:** headless pipelines where transient API emptiness should not abort the run.

---

### `_chunked(iterable, n)`

Generator that yields slices of size `n`. If `n <= 0`, yields the whole iterable once. Used to split `trade_ids` into batches.

---

### `_fetch_trade_info_parallel(tm, trade_ids, chunk_size, max_workers)`

**Purpose:** Fan-out detail calls concurrently.

**How it works:**

1. Build `futures` by submitting `tm.get_info_trades_from_ids(chunk)` for each chunk.

2. For each completed future, extract `tradeLegs` and **extend** the `results` list.

3. Return a combined dict `{ "tradeLegs": results }` so the caller path mirrors the non-parallel case.

**Notes:**

- Threading assumes `TradeManager.get_info_trades_from_ids` is **thread-safe** and the API can handle parallel requests.

- Tune `chunk_size` **up** to reduce request count; **down** to reduce payload size / error impact.

- Tune `max_workers` based on server rate limits and your runtime environment.

---

## Usage Examples

### Minimal fetch (non-parallel)
```python
from api import load_api_data

df = load_api_data(

    start_date="2025-09-01",
    end_date="2025-09-30",

)

print(df.shape)
```

### Excluding books and enabling parallel detail fetch

```python
from api import load_api_data

df = load_api_data(

    start_date="2025-09-01",
    end_date="2025-09-30",
    
    excluded_books=["Test Portfolio", "LegacyBook"],
    
    parallel_info_fetch=True,
    info_chunk_size=400,
    max_workers=8,

)
```

### Robust pipeline with retries

```python
from api import rerun_api_call

df = rerun_api_call(

    start_date="2025-09-01",
    end_date="2025-09-30",
    
    books_excluded=["Sandbox"],  # see naming note below
    
    max_retries=3,
    
    retry_sleep_sec=1.5,
    backoff=1.6,
    parallel_info_fetch=True,

)
```

---

## Expected DataFrame Shape & Types

- Each **row** corresponds to an enriched **trade leg**.

- Columns mirror the keys returned by LibApi’s `get_info_trades_from_ids` (union across legs).

- `strict=False` in `pl.from_dicts` means **missing fields** on some legs become **null** columns.

To inspect columns quickly:

```python
print(df.columns)
print(df.schema)
```

---

## Logging & Timing

`load_api_data` prints:

- Available books (post-exclusion),

- Generated date list,

- Start/end messages and a summary with elapsed seconds and counts.

These are intentionally lightweight for CLI observability; replace `print` with a project logger if needed.

---

## Error Handling & Edge Cases

- **Empty inputs:** no books or no dates → empty DataFrame.

- **No `tradeLegId`:** filtered out early to avoid invalid detail calls.

- **Inconsistent API responses:** `strict=False` avoids immediate failure; if the structure is truly incompatible, caller may receive an empty DataFrame.

- **Parallel errors:** a failed future currently just skips legs from that batch; consider logging exceptions per future for full visibility.

---

## Performance Considerations

- **Network bound:** Most time is spent in API calls, not DataFrame creation.

- **Parallel path:**

  - Effective when the API supports concurrency and bandwidth is adequate.

  - Use `max_workers` conservatively to respect rate limits.
  
- **Chunk size:** Larger chunks = fewer requests; smaller chunks = better isolation & smoother rate-limit behavior.

---

## Known Nits & Suggested Fixes

1. **Typo in futures handling**

   - Code uses `fut.results()`; the correct method is **`fut.result()`**.

   - **Fix:**
     ```python
     data = fut.result() or {}
     ```

2. **Parameter naming drift**

   - `load_api_data` uses `excluded_books`; `rerun_api_call` takes `books_excluded`.

   - **Suggestion:** standardize on `excluded_books` in both function signatures and calls to avoid confusion.

3. **Thread-safety note**
   - If `TradeManager` is not thread-safe, either (a) disable `parallel_info_fetch`, or (b) create a new `TradeManager` per worker (e.g., pass a callable that builds a client inside each task).

4. **Result aggregation on parallel errors**
   - Consider capturing exceptions from futures and logging which chunk failed, along with a retry at chunk granularity.

---

## Extensibility Tips

- **Filtering:** Add optional filters (e.g., by counterparty, product type) *after* enrichment using Polars expressions.

- **Schema normalization:** Build a small schema mapper to rename API keys to your analytics names.

- **Observability:** Replace prints with structured logging (logger name, levels, timings, request IDs).

- **Testing:**
  
  - Mock `TradeManager` to return deterministic legs and details.
  
  - Unit-test `_chunked` and retry logic with small inputs.

---

## Simple Sequence Diagram (ASCII)

```
Caller
  |
  |  load_api_data(start, end, ...)
  v
TradeManager (cached)
  |-- get_all_existing_hv_portfolios()
  |-- get_all_existing_wr_portfolios()
  |-- generate_dates(start, end)
  |-- get_trades_from_books_by_date(books, dates)
  |     -> tradeLegs (ids)
  |-- [parallel?]
  |     for each chunk(ids): get_info_trades_from_ids(chunk)
  |     -> tradeLegs (enriched)
  v
Polars.from_dicts(enriched_legs)
  |
  v
DataFrame (return)
```

---

## FAQ

- **Q:** What happens if `end_date` is `None`?

  **A:** Your LibApi implementation typically defaults to “today” in `generate_dates`.


- **Q:** Can I switch to `ProcessPoolExecutor`?

  **A:** Not recommended for I/O-bound tasks; threads are lighter and share the cached client more easily.


- **Q:** Does `strict=False` hide schema issues?

  **A:** It tolerates missing keys. If you require a fixed schema, follow with `select`/`with_columns` to coerce types.

---

## Ready-to-Use Snippets

**List unique books actually used in the returned DF**

```python
books = df.get_column("book").unique().to_list() if not df.is_empty() else []
```

**Keep only a subset of columns**

```python
keep = ["tradeLegId", "book", "tradeDate", "product", "qty", "price"]
clean = df.select([c for c in keep if c in df.columns])
```

**Filter by date range within the DF** (if the detail contains dates)

```python
clean = df.filter(pl.col("tradeDate").is_between(pl.date("2025-09-01"), pl.date("2025-09-30")))
```

---

### Final Notes

- The current structure is suitable for **batch jobs** and **ad-hoc analytics**.

- For services, consider adding **timeouts**, **per-chunk retries**, and **structured logs**.

- Standardize naming and fix the small typo in `_fetch_trade_info_parallel` to avoid runtime errors.
