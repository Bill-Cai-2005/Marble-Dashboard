import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import json
import os
from pathlib import Path
import requests
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Page configuration
st.set_page_config(
    page_title="Marble Watchlist Tool",
    page_icon="📊",
    layout="wide"
)

# API Configuration
POLYGON_BASE_URL = "https://api.polygon.io"
MAX_WORKERS = 24
SNAPSHOT_BATCH_SIZE = 250


def _load_polygon_api_key():
    key = os.getenv("POLYGON_API_KEY")
    if key:
        return key.strip()
    try:
        secret_key = st.secrets.get("POLYGON_API_KEY")
        if secret_key:
            return str(secret_key).strip()
    except Exception:
        pass
    return ""


POLYGON_API_KEY = _load_polygon_api_key()
if not POLYGON_API_KEY:
    st.error(
        "Missing Polygon API key. Set POLYGON_API_KEY as an environment variable "
        "or in Streamlit secrets."
    )
    st.stop()

# Data storage
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
WATCHLISTS_FILE = DATA_DIR / "watchlists.json"
TICKERS_FILE = DATA_DIR / "nyse_nasdaq_tickers.json"
UNIVERSAL_CACHE_FILE = DATA_DIR / "universal_cache.json"
CUSTOM_CACHE_FILE = DATA_DIR / "custom_watchlist_cache.json"
TICKER_REF_CACHE_FILE = DATA_DIR / "ticker_ref_cache.json"
START_PRICE_CACHE_FILE = DATA_DIR / "start_price_cache.json"
MARKET_CAP_CACHE_FILE = DATA_DIR / "market_cap_cache.json"
WATCHLIST_COLUMNS = [
    "Ticker",
    "Starting Price",
    "Current Price",
    "Market Cap",
    "Daily Stock Change %",
    "Change Since Start %",
    "Volume",
    "Industry",
    "Error",
]
NUMERIC_COLUMNS = [
    "Starting Price",
    "Current Price",
    "Market Cap",
    "Daily Stock Change %",
    "Change Since Start %",
    "Volume",
]
CACHE_LOCK = threading.Lock()

def load_watchlists():
    """Load custom watchlists from JSON file"""
    payload = load_cache(WATCHLISTS_FILE)
    return payload if isinstance(payload, dict) else {}

def save_watchlists(watchlists):
    """Save custom watchlists to JSON file"""
    save_cache(WATCHLISTS_FILE, watchlists)

def load_tickers():
    """Load NYSE and NASDAQ tickers"""
    payload = load_cache(TICKERS_FILE)
    if isinstance(payload, dict):
        return payload
    return {"nyse": [], "nasdaq": []}

def save_tickers(tickers):
    """Save NYSE and NASDAQ tickers"""
    save_cache(TICKERS_FILE, tickers)

def _polygon_get(path, params=None, timeout=20, retries=4):
    q = {} if params is None else dict(params)
    q["apiKey"] = POLYGON_API_KEY
    last_err = None
    for attempt in range(retries):
        try:
            response = requests.get(f"{POLYGON_BASE_URL}{path}", params=q, timeout=timeout)
            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.5 * (2 ** attempt))
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** attempt))
    raise last_err


def _to_iso(d):
    if hasattr(d, "isoformat"):
        return d.isoformat()
    return str(d)


def load_cache(cache_file):
    if cache_file.exists():
        with CACHE_LOCK:
            raw = cache_file.read_text(encoding="utf-8")
        if not raw.strip():
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Recover from concatenated JSON blobs by reading the first object.
            try:
                decoder = json.JSONDecoder()
                obj, _ = decoder.raw_decode(raw)
                return obj if isinstance(obj, (dict, list)) else {}
            except Exception:
                st.warning(f"Cache file is corrupted: {cache_file.name}. Using empty cache.")
                return {}
    return {}


def save_cache(cache_file, payload):
    tmp_file = cache_file.with_suffix(cache_file.suffix + ".tmp")
    serialized = json.dumps(payload)
    with CACHE_LOCK:
        tmp_file.write_text(serialized, encoding="utf-8")
        tmp_file.replace(cache_file)


def _chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def normalize_watchlist_df(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=WATCHLIST_COLUMNS)
    for col in WATCHLIST_COLUMNS:
        if col not in df.columns:
            df[col] = None
    normalized = df[WATCHLIST_COLUMNS].copy()
    for col in NUMERIC_COLUMNS:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    return normalized


def get_market_status():
    now_et = pd.Timestamp.now(tz="America/New_York")
    if now_et.weekday() >= 5:
        return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close


def _safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _is_individual_company_result(row):
    instrument_type = str(row.get("type", "")).upper().strip()
    if instrument_type != "CS":
        return False
    name = str(row.get("name", "")).upper()
    blocked_tokens = (
        " ETF",
        " FUND",
        " TRUST",
        " ETN",
        " INDEX",
        " SPDR",
        " ISHARES",
        " PROSHARES",
        " ULTRASHORT",
        " INVERSE",
    )
    return not any(token in name for token in blocked_tokens)


def _is_individual_company_ticker(ticker):
    try:
        data = _polygon_get(f"/v3/reference/tickers/{ticker}")
        row = data.get("results", {}) if isinstance(data, dict) else {}
        if not row:
            return False, "Ticker not found in Polygon."
        if not _is_individual_company_result(row):
            return False, "Ticker is not an individual operating company (common stock only)."
        ref_cache = _get_ref_cache()
        ref_cache[ticker] = {
            "market_cap": _safe_float(row.get("market_cap")),
            "industry": row.get("sic_description") or row.get("industry"),
        }
        _save_ref_cache(ref_cache)
        return True, ""
    except Exception as e:
        return False, f"Validation failed: {str(e)}"


def _parse_polygon_ticker_result(res):
    market_cap = _safe_float(res.get("market_cap"))
    industry = res.get("sic_description") or res.get("industry")
    return market_cap, industry


def _get_ref_cache():
    return load_cache(TICKER_REF_CACHE_FILE)


def _save_ref_cache(cache):
    save_cache(TICKER_REF_CACHE_FILE, cache)


def _get_start_price_cache():
    return load_cache(START_PRICE_CACHE_FILE)


def _save_start_price_cache(cache):
    save_cache(START_PRICE_CACHE_FILE, cache)


def _get_market_cap_cache():
    payload = load_cache(MARKET_CAP_CACHE_FILE)
    return payload if isinstance(payload, dict) else {}


def _save_market_cap_cache(payload):
    save_cache(MARKET_CAP_CACHE_FILE, payload)


def _latest_market_cap_snapshot(cache):
    if not isinstance(cache, dict) or not cache:
        return None, {}
    latest_ts = None
    latest_caps = {}
    for ts, entry in cache.items():
        if not isinstance(entry, dict):
            continue
        caps = entry.get("market_caps", {})
        if not isinstance(caps, dict):
            continue
        if latest_ts is None or str(ts) > str(latest_ts):
            latest_ts = ts
            latest_caps = caps
    return latest_ts, latest_caps


def _apply_market_cap_cache_to_df(df, caps_by_ticker):
    if df is None or df.empty or not isinstance(caps_by_ticker, dict):
        return df
    updated_df = df.copy()
    fetched_caps = updated_df["Ticker"].map(
        lambda t: _safe_float(caps_by_ticker.get(str(t).upper())) if pd.notna(t) else None
    )
    updated_df["Market Cap"] = fetched_caps.combine_first(updated_df["Market Cap"])
    return normalize_watchlist_df(updated_df)


def _get_start_price_entry(cache, date_key):
    entry = cache.get(date_key, {}) if isinstance(cache, dict) else {}
    # Backward-compatible: old format is {date_key: {ticker: price}}
    if isinstance(entry, dict) and "prices" in entry:
        prices = entry.get("prices", {})
        cached_at = entry.get("cached_at")
    else:
        prices = entry if isinstance(entry, dict) else {}
        cached_at = None
    return prices, cached_at


def _set_start_price_entry(cache, date_key, prices):
    cache[date_key] = {
        "cached_at": datetime.now().isoformat(),
        "prices": prices,
    }


def _cached_start_dates():
    cache = _get_start_price_cache()
    dates = list(cache.keys()) if isinstance(cache, dict) else []
    return sorted(dates)


def _render_cache_overview():
    st.subheader("Historical Prices")
    with st.expander("Show historical price caches"):
        start_cache = _get_start_price_cache()
        if isinstance(start_cache, dict) and start_cache:
            for date_key in sorted(start_cache.keys()):
                prices, cached_at = _get_start_price_entry(start_cache, date_key)
                st.markdown(
                    f"- start date `{date_key}` — cached at: `{cached_at or 'unknown'}` | "
                    f"tickers: `{len(prices)}`"
                )
        else:
            st.markdown("No historical price caches yet.")


def _fetch_first_close_on_or_after(ticker, start_date, lookahead_days=14):
    to_date = start_date + timedelta(days=lookahead_days)
    aggs = _polygon_get(
        f"/v2/aggs/ticker/{ticker}/range/1/day/{_to_iso(start_date)}/{_to_iso(to_date)}",
        {"adjusted": "true", "sort": "asc", "limit": 50},
    )
    rows = aggs.get("results", [])
    if not rows:
        return None
    return _safe_float(rows[0].get("c"))


def _fetch_first_open_on_or_after(ticker, start_date, lookahead_days=14):
    to_date = start_date + timedelta(days=lookahead_days)
    aggs = _polygon_get(
        f"/v2/aggs/ticker/{ticker}/range/1/day/{_to_iso(start_date)}/{_to_iso(to_date)}",
        {"adjusted": "true", "sort": "asc", "limit": 50},
    )
    rows = aggs.get("results", [])
    if not rows:
        return None
    return _safe_float(rows[0].get("o"))


def _fetch_last_close_on_or_before(ticker, end_date, lookback_days=14):
    from_date = end_date - timedelta(days=lookback_days)
    aggs = _polygon_get(
        f"/v2/aggs/ticker/{ticker}/range/1/day/{_to_iso(from_date)}/{_to_iso(end_date)}",
        {"adjusted": "true", "sort": "asc", "limit": 50},
    )
    rows = aggs.get("results", [])
    if not rows:
        return None, None
    last = rows[-1]
    return _safe_float(last.get("c")), _safe_float(last.get("v"))


def fetch_historical_open_prices_for_date(tickers, selected_date):
    """Fetch and cache opening prices for all tickers for a selected start date."""
    date_key = _to_iso(selected_date)
    cache = _get_start_price_cache()
    prices, _ = _get_start_price_entry(cache, date_key)
    prices = dict(prices)

    unique_tickers = list(dict.fromkeys(tickers))
    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(unique_tickers)
    if total == 0:
        st.warning("No tickers loaded.")
        return

    with ThreadPoolExecutor(max_workers=max(4, MAX_WORKERS // 2)) as executor:
        futures = {
            executor.submit(_fetch_first_open_on_or_after, t, selected_date): t
            for t in unique_tickers
        }
        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            status_text.text(f"Historical open fetch {completed + 1}/{total} ({ticker})")
            try:
                open_price = _safe_float(future.result())
                if open_price is not None:
                    prices[ticker] = open_price
            except Exception:
                pass
            completed += 1
            progress_bar.progress(completed / total)

    _set_start_price_entry(cache, date_key, prices)
    _save_start_price_cache(cache)
    status_text.text(f"✅ Historical open cache saved for {date_key} ({len(prices)} tickers).")
    progress_bar.empty()


def calculate_market_caps_for_dataframe(df):
    """Fetch market caps via reference endpoint for tickers currently in the table."""
    if df is None or df.empty or "Ticker" not in df.columns:
        st.warning("No table data available to calculate market cap.")
        return df

    updated_df = df.copy()
    tickers = [str(t).upper() for t in updated_df["Ticker"].dropna().tolist()]
    tickers = list(dict.fromkeys(tickers))
    total = len(tickers)
    if total == 0:
        st.warning("No tickers found in table.")
        return updated_df

    progress_bar = st.progress(0)
    status_text = st.empty()
    ref_cache = _get_ref_cache()
    caps_by_ticker = {}

    with ThreadPoolExecutor(max_workers=max(4, MAX_WORKERS // 2)) as executor:
        futures = {
            executor.submit(_polygon_get, f"/v3/reference/tickers/{ticker}"): ticker
            for ticker in tickers
        }
        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            status_text.text(f"Calculating market cap {completed + 1}/{total} ({ticker})")
            try:
                data = future.result()
                row = data.get("results", {}) if isinstance(data, dict) else {}
                market_cap = _safe_float(row.get("market_cap"))
                if market_cap is not None:
                    caps_by_ticker[ticker] = market_cap
                if row and _is_individual_company_result(row):
                    ref_cache[ticker] = _build_ref_cache_entry_from_row(row)
            except Exception:
                pass
            completed += 1
            progress_bar.progress(completed / total)

    if caps_by_ticker:
        updated_df = _apply_market_cap_cache_to_df(updated_df, caps_by_ticker)

    _save_ref_cache(ref_cache)
    cap_cache = _get_market_cap_cache()
    cap_cache[datetime.now().isoformat()] = {"market_caps": caps_by_ticker}
    _save_market_cap_cache(cap_cache)
    status_text.text(f"✅ Market cap calculation complete ({len(caps_by_ticker)}/{total} updated).")
    progress_bar.empty()
    return normalize_watchlist_df(updated_df)


def refresh_reference_cache_for_tickers(tickers):
    ref_cache = _get_ref_cache()
    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(tickers)
    if total == 0:
        return

    for i, ticker in enumerate(tickers, start=1):
        status_text.text(f"Reference refresh {i}/{total} ({ticker})")
        try:
            data = _polygon_get(f"/v3/reference/tickers/{ticker}")
            row = data.get("results", {}) if isinstance(data, dict) else {}
            if row and _is_individual_company_result(row):
                ref_cache[ticker] = {
                    "market_cap": _safe_float(row.get("market_cap")),
                    "industry": row.get("sic_description") or row.get("industry"),
                }
        except Exception:
            pass
        progress_bar.progress(i / total)

    _save_ref_cache(ref_cache)
    status_text.text("✅ Reference refresh complete!")
    progress_bar.empty()


def _build_snapshot_row(snapshot, ref_cache, start_date_for_change=None, start_price_cache=None):
    ticker = str(snapshot.get("ticker", "")).upper().strip()
    day = snapshot.get("day", {}) or {}
    prev_day = snapshot.get("prevDay", {}) or {}
    last_trade = snapshot.get("lastTrade", {}) or {}
    prev_close = _safe_float(prev_day.get("c"))
    current_price = _safe_float(last_trade.get("p"))
    day_close = _safe_float(day.get("c"))
    if current_price is None:
        current_price = day_close
    volume = _safe_float(day.get("v"))
    change_pct = None
    if prev_close and prev_close != 0:
        # Standard stock app behavior:
        # - market open: prev close -> current/last trade
        # - market closed: second-last close -> last close
        if get_market_status() and current_price is not None:
            change_pct = ((current_price - prev_close) / prev_close) * 100
        elif day_close is not None:
            change_pct = ((day_close - prev_close) / prev_close) * 100

    change_since_start = None
    starting_price = prev_close
    if start_date_for_change is not None and isinstance(start_price_cache, dict):
        date_key = _to_iso(start_date_for_change)
        prices, _ = _get_start_price_entry(start_price_cache, date_key)
        cached_start = _safe_float(prices.get(ticker))
        if cached_start is not None:
            starting_price = cached_start
            if current_price is not None and cached_start != 0:
                change_since_start = ((current_price - cached_start) / cached_start) * 100

    ref = ref_cache.get(ticker, {})
    return {
        "Ticker": ticker,
        "Starting Price": starting_price,
        "Current Price": current_price,
        "Market Cap": _safe_float(ref.get("market_cap")),
        "Daily Stock Change %": change_pct,
        "Change Since Start %": change_since_start,
        "Volume": volume,
        "Industry": ref.get("industry"),
        "Error": None,
    }


def _daily_change_from_aggs(aggs, custom_start_date=None, custom_end_date=None):
    if not aggs:
        return None, None, None, None
    rows = sorted(aggs, key=lambda x: x.get("t", 0))
    if custom_start_date and custom_end_date:
        start_close = _safe_float(rows[0].get("c"))
        end_close = _safe_float(rows[-1].get("c"))
        volume = _safe_float(rows[-1].get("v"))
        if start_close and start_close != 0 and end_close is not None:
            return start_close, end_close, ((end_close - start_close) / start_close) * 100, volume
        return start_close, end_close, None, volume
    latest = rows[-1]
    volume = _safe_float(latest.get("v"))
    if len(rows) >= 2:
        prev_close = _safe_float(rows[-2].get("c"))
        last_close = _safe_float(rows[-1].get("c"))
        if prev_close and prev_close != 0 and last_close is not None:
            # Standard closed-market behavior: second-last close -> last close
            return prev_close, last_close, ((last_close - prev_close) / prev_close) * 100, volume
    return None, _safe_float(latest.get("c")), None, volume


def get_polygon_stock_data(ticker_symbol, custom_start_date=None, custom_end_date=None, start_price_cache=None):
    try:
        # Use reference cache first; fetch once if missing.
        ref_cache = _get_ref_cache()
        ref_row = ref_cache.get(ticker_symbol)
        if ref_row:
            market_cap = _safe_float(ref_row.get("market_cap"))
            industry = ref_row.get("industry")
        else:
            ref = _polygon_get(f"/v3/reference/tickers/{ticker_symbol}")
            ref_res = ref.get("results", {}) if isinstance(ref, dict) else {}
            market_cap, industry = _parse_polygon_ticker_result(ref_res)
            ref_cache[ticker_symbol] = {"market_cap": market_cap, "industry": industry}
            _save_ref_cache(ref_cache)

        change_pct = None
        starting_price = None
        current_price = None
        volume = None
        use_custom_range = custom_start_date is not None and custom_end_date is not None

        # For non-custom mode while market is open, use snapshot for prev close -> current trade.
        if not use_custom_range and get_market_status():
            try:
                snap = _polygon_get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker_symbol}")
                ticker_snap = snap.get("ticker", {})
                prev_close = _safe_float((ticker_snap.get("prevDay", {}) or {}).get("c"))
                current_price = _safe_float((ticker_snap.get("lastTrade", {}) or {}).get("p"))
                if current_price is None:
                    current_price = _safe_float((ticker_snap.get("day", {}) or {}).get("c"))
                volume = _safe_float((ticker_snap.get("day", {}) or {}).get("v"))
                if prev_close and prev_close != 0 and current_price is not None:
                    change_pct = ((current_price - prev_close) / prev_close) * 100
            except Exception:
                pass

        # Custom range path: cached start price + end/current price.
        if use_custom_range:
            date_key = _to_iso(custom_start_date)
            cached_price = None
            if isinstance(start_price_cache, dict):
                prices, _ = _get_start_price_entry(start_price_cache, date_key)
                cached_price = _safe_float(prices.get(ticker_symbol))
            starting_price = cached_price if cached_price is not None else _fetch_first_open_on_or_after(
                ticker_symbol, custom_start_date
            )
            current_price, volume = _fetch_last_close_on_or_before(ticker_symbol, custom_end_date)
            if starting_price and starting_price != 0 and current_price is not None:
                change_pct = ((current_price - starting_price) / starting_price) * 100
        # Non-custom fallback path.
        elif change_pct is None:
            from_date = _to_iso(date.today() - timedelta(days=7))
            to_date = _to_iso(date.today())
            aggs = _polygon_get(
                f"/v2/aggs/ticker/{ticker_symbol}/range/1/day/{from_date}/{to_date}",
                {"adjusted": "true", "sort": "asc", "limit": 5000},
            )
            starting_price, current_price, change_pct, volume = _daily_change_from_aggs(
                aggs.get("results", []),
                custom_start_date=None,
                custom_end_date=None,
            )
        else:
            # Snapshot open-session path already has current price; start is prev close.
            starting_price = prev_close if 'prev_close' in locals() else None

        return {
            "Ticker": ticker_symbol,
            "Starting Price": starting_price,
            "Current Price": current_price,
            "Market Cap": market_cap,
            "Daily Stock Change %": change_pct,
            "Change Since Start %": change_pct if use_custom_range else None,
            "Volume": volume,
            "Industry": industry,
        }
    except Exception as e:
        return {
            "Ticker": ticker_symbol,
            "Starting Price": None,
            "Current Price": None,
            "Market Cap": None,
            "Daily Stock Change %": None,
            "Change Since Start %": None,
            "Volume": None,
            "Industry": None,
            "Error": str(e),
        }

def get_stock_data(ticker_symbol, custom_start_date=None, custom_end_date=None, start_price_cache=None):
    return get_polygon_stock_data(
        ticker_symbol,
        custom_start_date,
        custom_end_date,
        start_price_cache=start_price_cache,
    )

def fetch_nyse_nasdaq_tickers():
    """Fetch all active NYSE/NASDAQ tickers from Polygon with pagination."""
    nasdaq_tickers = []
    nyse_tickers = []
    ref_cache = _get_ref_cache()
    nasdaq_codes = {"NASDAQ", "XNAS", "XNCM", "XNGS", "XNMS", "BATS", "BATS"}
    nyse_codes = {"NYSE", "XNYS", "ARCX", "XASE", "AMEX"}
    url = f"{POLYGON_BASE_URL}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "apiKey": POLYGON_API_KEY,
    }

    try:
        while True:
            data = requests.get(url, params=params, timeout=30).json()
            for row in data.get("results", []):
                symbol = str(row.get("ticker", "")).strip().upper()
                exch = str(row.get("primary_exchange", "")).upper().strip()
                if not symbol or len(symbol) > 6:
                    continue
                if not _is_individual_company_result(row):
                    continue
                ref_cache[symbol] = {
                    "market_cap": _safe_float(row.get("market_cap")),
                    "industry": row.get("sic_description") or row.get("industry"),
                }
                if exch in nasdaq_codes or "NASDAQ" in exch:
                    nasdaq_tickers.append(symbol)
                elif exch in nyse_codes or "NYSE" in exch:
                    nyse_tickers.append(symbol)
            next_url = data.get("next_url")
            if not next_url:
                break
            url = next_url
            params = {"apiKey": POLYGON_API_KEY}
    except Exception as e:
        st.error(f"Polygon ticker fetch failed: {str(e)}")

    nasdaq_tickers = sorted(list(set(nasdaq_tickers)))
    nyse_tickers = sorted(list(set(nyse_tickers)))
    _save_ref_cache(ref_cache)
    st.success(
        f"Loaded {len(nasdaq_tickers)} NASDAQ and {len(nyse_tickers)} NYSE tickers "
        f"(Total: {len(nasdaq_tickers) + len(nyse_tickers)})"
    )
    return {"nyse": nyse_tickers, "nasdaq": nasdaq_tickers}

def format_value(value):
    """Format numeric values for display"""
    if value is None:
        return "N/A"
    if isinstance(value, (int, float)):
        if abs(value) >= 1e9:
            return f"${value/1e9:.2f}B"
        elif abs(value) >= 1e6:
            return f"${value/1e6:.2f}M"
        elif abs(value) >= 1e3:
            return f"${value/1e3:.2f}K"
        elif isinstance(value, float):
            return f"{value:.2f}"
        else:
            return str(value)
    return str(value)


def _run_parallel_refresh(tickers, custom_start=None, custom_end=None):
    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(tickers)
    results = []
    done = 0
    if total == 0:
        return pd.DataFrame([])
    start_price_cache = _get_start_price_cache() if custom_start and custom_end else None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_stock_data, t, custom_start, custom_end, start_price_cache): t for t in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                row = future.result()
            except Exception as e:
                row = {
                    "Ticker": ticker,
                    "Starting Price": None,
                    "Current Price": None,
                    "Market Cap": None,
                    "Daily Stock Change %": None,
                    "Volume": None,
                    "Industry": None,
                    "Error": str(e),
                }
            results.append(row)
            done += 1
            status_text.text(f"Processed {done}/{total} ({ticker})")
            progress_bar.progress(done / total)

    # Retry failed tickers once to improve completeness on transient errors.
    failed_tickers = [r["Ticker"] for r in results if r.get("Error")]
    if failed_tickers:
        status_text.text(f"Retrying failed tickers ({len(failed_tickers)})...")
        retry_rows = {}
        with ThreadPoolExecutor(max_workers=max(4, MAX_WORKERS // 2)) as retry_executor:
            retry_futures = {
                retry_executor.submit(get_stock_data, t, custom_start, custom_end, start_price_cache): t
                for t in failed_tickers
            }
            for retry_future in as_completed(retry_futures):
                t = retry_futures[retry_future]
                try:
                    retry_rows[t] = retry_future.result()
                except Exception as e:
                    retry_rows[t] = {
                        "Ticker": t,
                        "Starting Price": None,
                        "Current Price": None,
                        "Market Cap": None,
                        "Daily Stock Change %": None,
                        "Volume": None,
                        "Industry": None,
                        "Error": str(e),
                    }
        results = [retry_rows.get(r["Ticker"], r) if r.get("Error") else r for r in results]

    status_text.text("✅ Refresh complete!")
    progress_bar.empty()
    return normalize_watchlist_df(pd.DataFrame(results))


def _refresh_via_snapshot(tickers, start_date_for_change=None):
    """Fast refresh path for non-custom-date mode using Polygon snapshots."""
    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(tickers)
    if total == 0:
        return normalize_watchlist_df(pd.DataFrame([]))

    ref_cache = _get_ref_cache()
    start_price_cache = _get_start_price_cache() if start_date_for_change is not None else None
    rows = []
    processed = 0
    failed = []

    for chunk in _chunked(tickers, SNAPSHOT_BATCH_SIZE):
        status_text.text(f"Snapshot batch refresh {processed}/{total}...")
        try:
            data = _polygon_get(
                "/v2/snapshot/locale/us/markets/stocks/tickers",
                {"tickers": ",".join(chunk)},
                timeout=30,
            )
            by_ticker = {str(it.get("ticker", "")).upper(): it for it in data.get("tickers", [])}
            for t in chunk:
                snap = by_ticker.get(t)
                if snap is None:
                    failed.append(t)
                    rows.append(
                        {
                            "Ticker": t,
                            "Starting Price": None,
                            "Current Price": None,
                            "Market Cap": _safe_float(ref_cache.get(t, {}).get("market_cap")),
                            "Daily Stock Change %": None,
                            "Change Since Start %": None,
                            "Volume": None,
                            "Industry": ref_cache.get(t, {}).get("industry"),
                            "Error": "Missing snapshot row",
                        }
                    )
                else:
                    rows.append(
                        _build_snapshot_row(
                            snap,
                            ref_cache,
                            start_date_for_change=start_date_for_change,
                            start_price_cache=start_price_cache,
                        )
                    )
        except Exception:
            failed.extend(chunk)
            for t in chunk:
                rows.append(
                    {
                        "Ticker": t,
                        "Starting Price": None,
                        "Current Price": None,
                        "Market Cap": _safe_float(ref_cache.get(t, {}).get("market_cap")),
                        "Daily Stock Change %": None,
                        "Change Since Start %": None,
                        "Volume": None,
                        "Industry": ref_cache.get(t, {}).get("industry"),
                        "Error": "Snapshot request failed",
                    }
                )
        processed += len(chunk)
        progress_bar.progress(min(1.0, processed / total))

    # Snapshot-only mode: do not run per-ticker fallback on failed/missing rows.
    if failed:
        status_text.text(f"Snapshot complete with {len(failed)} failed/missing tickers (fallback disabled).")

    status_text.text("✅ Refresh complete!")
    progress_bar.empty()
    return normalize_watchlist_df(pd.DataFrame(rows))

def main():
    st.title("📊 Marble Watchlist Tool")
    
    # Sidebar navigation
    page = st.sidebar.selectbox("Navigate", ["Universal Watchlist", "Custom Watchlists"])
    
    if page == "Universal Watchlist":
        universal_watchlist_page()
    else:
        custom_watchlists_page()

def universal_watchlist_page():
    st.header("Universal Watchlist")
    _render_cache_overview()
    
    # Load tickers
    tickers_data = load_tickers()
    all_tickers = tickers_data.get("nyse", []) + tickers_data.get("nasdaq", [])
    
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    
    with col1:
        if st.button("🔄 Fetch NYSE/NASDAQ Tickers"):
            with st.spinner("Fetching tickers from NYSE and NASDAQ..."):
                tickers_data = fetch_nyse_nasdaq_tickers()
                tickers_data["fetched_at"] = datetime.now().isoformat()
                save_tickers(tickers_data)
                all_tickers = tickers_data.get("nyse", []) + tickers_data.get("nasdaq", [])
                st.success(f"Loaded {len(all_tickers)} tickers")
    
    with col2:
        use_custom_range = st.checkbox("Use Custom Time Range")
        historical_cache_date = st.date_input("Historical Price Date (Open)", value=date.today(), key="historical_open_date")
        if st.button("📚 Historical Price Fetch"):
            if not all_tickers:
                st.warning("Please fetch tickers first")
            else:
                fetch_historical_open_prices_for_date(all_tickers, historical_cache_date)
    
    custom_start = None
    custom_end = None
    
    if use_custom_range:
        col_start, col_end = st.columns(2)
        with col_start:
            custom_start = st.date_input("Start Date", value=date.today())
        with col_end:
            custom_end = st.date_input("End Date", value=date.today())
        cached_dates = _cached_start_dates()
        if cached_dates:
            start_cache = _get_start_price_cache()
            labels = []
            for d in cached_dates[-12:]:
                _, ts = _get_start_price_entry(start_cache, d)
                labels.append(f"{d} (cached {ts or 'unknown'})")
            st.caption(f"Cached start dates available ({len(cached_dates)}): " + ", ".join(labels))
        else:
            st.caption("No cached start dates yet. Use Historical Price Fetch to build cache.")
    
    with col4:
        if st.button("⚡ Snapshot Refresh"):
            if not all_tickers:
                st.warning("Please fetch tickers first")
            else:
                df = _refresh_via_snapshot(
                    all_tickers,
                    start_date_for_change=custom_start if use_custom_range else None,
                )
                st.session_state.universal_data = df
                save_cache(
                    UNIVERSAL_CACHE_FILE,
                    {
                        "last_refreshed": datetime.now().isoformat(),
                        "records": df.to_dict(orient="records"),
                    },
                )
    
    # Display watchlist
    if 'universal_data' in st.session_state:
        df = st.session_state.universal_data
        if st.button("🗂️ Load Market Cap From Previous Date"):
            cap_cache = _get_market_cap_cache()
            latest_ts, latest_caps = _latest_market_cap_snapshot(cap_cache)
            if latest_caps:
                loaded_df = _apply_market_cap_cache_to_df(df, latest_caps)
                st.session_state.universal_data = loaded_df
                df = loaded_df
                st.info(f"Applied market cap cache from {latest_ts}.")
                save_cache(
                    UNIVERSAL_CACHE_FILE,
                    {
                        "last_refreshed": datetime.now().isoformat(),
                        "records": loaded_df.to_dict(orient="records"),
                    },
                )
            else:
                st.warning("No previous market cap cache found.")
        if st.button("🧮 Calculate Market Cap"):
            recalculated_df = calculate_market_caps_for_dataframe(df)
            st.session_state.universal_data = recalculated_df
            df = recalculated_df
            save_cache(
                UNIVERSAL_CACHE_FILE,
                {
                    "last_refreshed": datetime.now().isoformat(),
                    "records": recalculated_df.to_dict(orient="records"),
                },
            )
        # Universal view excludes Industry/Error columns per product requirements.
        universal_df = df.drop(columns=["Industry", "Error"], errors="ignore")
        table_min_cap_m = st.number_input(
            "Table Filter: Min Market Cap (Millions USD)",
            min_value=0,
            value=0,
            step=100,
            key="universal_table_mcap_filter_m",
        )
        if table_min_cap_m > 0:
            table_min_cap = int(table_min_cap_m) * 1_000_000
            universal_df = universal_df[universal_df["Market Cap"].fillna(0) >= table_min_cap]
        
        # Sorting
        st.subheader("Sort Options")
        sort_col, sort_order = st.columns(2)
        with sort_col:
            sort_column = st.selectbox("Sort by", universal_df.columns.tolist(), key="universal_sort_col")
        with sort_order:
            ascending = st.checkbox("Ascending", value=True, key="universal_sort_order")
        
        if sort_column:
            universal_df = universal_df.sort_values(by=sort_column, ascending=ascending, na_position='last')

        st.dataframe(universal_df, use_container_width=True, height=600)
        
        # Download button
        csv = universal_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"universal_watchlist_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

def refresh_universal_watchlist(tickers, custom_start=None, custom_end=None):
    """Refresh data for all tickers in universal watchlist"""
    if custom_start or custom_end:
        df = _run_parallel_refresh(
            tickers,
            custom_start=custom_start,
            custom_end=custom_end,
        )
    else:
        df = _refresh_via_snapshot(tickers, start_date_for_change=None)
    st.session_state.universal_data = df

def custom_watchlists_page():
    st.header("Custom Watchlists")
    _render_cache_overview()
    
    watchlists = load_watchlists()
    
    # Create new watchlist
    st.subheader("Create New Watchlist")
    new_watchlist_name = st.text_input("Watchlist Name", key="new_watchlist")
    if st.button("Create Watchlist"):
        if new_watchlist_name:
            if new_watchlist_name not in watchlists:
                watchlists[new_watchlist_name] = []
                save_watchlists(watchlists)
                st.success(f"Created watchlist: {new_watchlist_name}")
            else:
                st.error("Watchlist already exists")
        else:
            st.error("Please enter a watchlist name")
    
    # Select watchlist to manage
    if watchlists:
        selected_watchlist = st.selectbox("Select Watchlist", list(watchlists.keys()))
        
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
        
        with col1:
            new_ticker = st.text_input("Add Ticker", key=f"add_ticker_{selected_watchlist}").upper()
            if st.button("Add Ticker"):
                if new_ticker:
                    if new_ticker not in watchlists[selected_watchlist]:
                        is_company, reason = _is_individual_company_ticker(new_ticker)
                        if is_company:
                            watchlists[selected_watchlist].append(new_ticker)
                            save_watchlists(watchlists)
                            st.success(f"Added {new_ticker}")
                        else:
                            st.error(reason)
                    else:
                        st.warning(f"{new_ticker} already in watchlist")
        
        with col2:
            use_custom_range = st.checkbox("Use Custom Time Range", key=f"custom_range_{selected_watchlist}")
        
        custom_start = None
        custom_end = None
        
        if use_custom_range:
            col_start, col_end = st.columns(2)
            with col_start:
                custom_start = st.date_input("Start Date", value=date.today(), key=f"start_{selected_watchlist}")
            with col_end:
                custom_end = st.date_input("End Date", value=date.today(), key=f"end_{selected_watchlist}")
        
        with col4:
            if st.button("🔄 Refresh Watchlist"):
                tickers = watchlists[selected_watchlist]
                if tickers:
                    refresh_custom_watchlist(
                        selected_watchlist,
                        tickers,
                        custom_start,
                        custom_end,
                    )
                else:
                    st.warning("Watchlist is empty")
        
        # Display tickers in watchlist
        st.subheader(f"Tickers in {selected_watchlist}")
        tickers = watchlists[selected_watchlist]
        
        if tickers:
            # Create a dataframe for ticker management
            ticker_df = pd.DataFrame({'Ticker': tickers})
            st.dataframe(ticker_df, use_container_width=True)
            
            # Remove ticker
            remove_ticker = st.selectbox("Remove Ticker", [""] + tickers, key=f"remove_{selected_watchlist}")
            if st.button("Remove"):
                if remove_ticker:
                    watchlists[selected_watchlist].remove(remove_ticker)
                    save_watchlists(watchlists)
                    st.success(f"Removed {remove_ticker}")
                    st.rerun()
        
        # Display watchlist data
        watchlist_key = f"watchlist_data_{selected_watchlist}"
        if watchlist_key in st.session_state:
            df = st.session_state[watchlist_key]
            
            # Sorting
            st.subheader("Sort Options")
            sort_col, sort_order = st.columns(2)
            with sort_col:
                sort_column = st.selectbox("Sort by", df.columns.tolist(), key=f"sort_col_{selected_watchlist}")
            with sort_order:
                ascending = st.checkbox("Ascending", value=True, key=f"sort_order_{selected_watchlist}")
            
            if sort_column:
                df = df.sort_values(by=sort_column, ascending=ascending, na_position='last')

            st.dataframe(df, use_container_width=True, height=600)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{selected_watchlist}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        
        # Delete watchlist
        st.subheader("Delete Watchlist")
        if st.button("🗑️ Delete Watchlist", key=f"delete_{selected_watchlist}"):
            del watchlists[selected_watchlist]
            save_watchlists(watchlists)
            st.success(f"Deleted watchlist: {selected_watchlist}")
            st.rerun()
    else:
        st.info("No watchlists created yet. Create one above!")

def refresh_custom_watchlist(
    watchlist_name,
    tickers,
    custom_start=None,
    custom_end=None,
):
    """Refresh data for a specific custom watchlist"""
    if custom_start or custom_end:
        df = _run_parallel_refresh(
            tickers,
            custom_start=custom_start,
            custom_end=custom_end,
        )
    else:
        df = _refresh_via_snapshot(tickers, start_date_for_change=None)
    st.session_state[f"watchlist_data_{watchlist_name}"] = df

if __name__ == "__main__":
    main()
