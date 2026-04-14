"""
COT Data Fetcher & COT Index Calculator
Source: CFTC Commitments of Traders (weekly, released every Friday)
"""

import os
import time
import zipfile
from io import BytesIO

import pandas as pd
import requests

COT_INDEX_LOOKBACK = 52  # weeks
CACHE_MAX_AGE = 7 * 86400  # re-download current year if cache older than 7 days


def _fetch_zip_csv(url, cache_file, force_refresh=False):
    """Download a CFTC zip file, extract CSV, cache locally."""
    if os.path.exists(cache_file) and not force_refresh:
        return pd.read_csv(cache_file, low_memory=False)

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(BytesIO(resp.content)) as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pd.read_csv(f, low_memory=False)

    df.to_csv(cache_file, index=False)
    return df


def fetch_cot_history(cache_dir, year_start=2016, year_end=2026):
    """Fetch Disaggregated + Financial Futures COT data from CFTC."""
    os.makedirs(cache_dir, exist_ok=True)
    all_disagg = []
    all_fin = []

    import datetime

    current_year = datetime.datetime.now().year

    for year in range(year_start, year_end + 1):
        # Current year: re-download if cache is older than 7 days
        refresh = False
        if year == current_year:
            cache_disagg_path = os.path.join(cache_dir, f"cot_disagg_{year}.csv")
            if os.path.exists(cache_disagg_path):
                age = time.time() - os.path.getmtime(cache_disagg_path)
                if age > CACHE_MAX_AGE:
                    refresh = True

        url_disagg = f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
        cache_disagg = os.path.join(cache_dir, f"cot_disagg_{year}.csv")
        try:
            df = _fetch_zip_csv(url_disagg, cache_disagg, force_refresh=refresh)
            all_disagg.append(df)
        except Exception as e:
            print(f"  Disagg {year}: FAILED - {e}")

        url_fin = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
        cache_fin = os.path.join(cache_dir, f"cot_fin_{year}.csv")
        try:
            df = _fetch_zip_csv(url_fin, cache_fin, force_refresh=refresh)
            all_fin.append(df)
        except Exception as e:
            print(f"  FinFut {year}: FAILED - {e}")

    disagg = pd.concat(all_disagg, ignore_index=True) if all_disagg else pd.DataFrame()
    fin = pd.concat(all_fin, ignore_index=True) if all_fin else pd.DataFrame()
    return disagg, fin


def extract_market(df_cot, market_code):
    """Extract a single market from COT Disaggregated data by CFTC code."""
    code_col = None
    for col in df_cot.columns:
        cl = col.strip()
        if "CFTC_Commodity_Code" in cl or "CFTC Commodity Code" in cl:
            code_col = col
            break
    if code_col is None:
        for col in df_cot.columns:
            if "cftc" in col.lower() and "code" in col.lower() and "market" not in col.lower():
                code_col = col
                break

    if code_col is None:
        return pd.DataFrame()

    df_cot[code_col] = df_cot[code_col].astype(str).str.strip()
    df = df_cot[df_cot[code_col] == market_code.strip()].copy()

    if len(df) == 0:
        return pd.DataFrame()

    date_col = None
    for col in df.columns:
        cl = col.lower().strip()
        if "report_date" in cl.replace(" ", "_") or ("date" in cl and "report" in cl):
            date_col = col
            break
    if date_col is None:
        for col in df.columns:
            if "date" in col.lower():
                date_col = col
                break

    df["date"] = pd.to_datetime(df[date_col])
    df = df.sort_values("date").reset_index(drop=True)

    result = pd.DataFrame()
    result["date"] = df["date"]

    col_map = {}
    for col in df.columns:
        cll = col.strip().lower()
        if "m_money" in cll and "long" in cll and "spread" not in cll and "change" not in cll and "pct" not in cll:
            if "all" in cll and "mm_long" not in col_map:
                col_map["mm_long"] = col
        if "m_money" in cll and "short" in cll and "spread" not in cll and "change" not in cll and "pct" not in cll:
            if "all" in cll and "mm_short" not in col_map:
                col_map["mm_short"] = col
        if "prod_merc" in cll and "long" in cll and "change" not in cll and "pct" not in cll:
            if "all" in cll and "prod_long" not in col_map:
                col_map["prod_long"] = col
        if "prod_merc" in cll and "short" in cll and "change" not in cll and "pct" not in cll:
            if "all" in cll and "prod_short" not in col_map:
                col_map["prod_short"] = col
        if "nonrept" in cll and "long" in cll and "change" not in cll and "pct" not in cll:
            if "all" in cll and "retail_long" not in col_map:
                col_map["retail_long"] = col
        if "nonrept" in cll and "short" in cll and "change" not in cll and "pct" not in cll:
            if "all" in cll and "retail_short" not in col_map:
                col_map["retail_short"] = col
        if "open_interest_all" in cll and "oi" not in col_map:
            col_map["oi"] = col

    if "mm_long" in col_map:
        result["noncomm_long"] = pd.to_numeric(df[col_map["mm_long"]], errors="coerce")
    if "mm_short" in col_map:
        result["noncomm_short"] = pd.to_numeric(df[col_map["mm_short"]], errors="coerce")
    if "prod_long" in col_map:
        result["comm_long"] = pd.to_numeric(df[col_map["prod_long"]], errors="coerce")
    if "prod_short" in col_map:
        result["comm_short"] = pd.to_numeric(df[col_map["prod_short"]], errors="coerce")
    if "retail_long" in col_map:
        result["retail_long"] = pd.to_numeric(df[col_map["retail_long"]], errors="coerce")
    if "retail_short" in col_map:
        result["retail_short"] = pd.to_numeric(df[col_map["retail_short"]], errors="coerce")
    if "oi" in col_map:
        result["open_interest"] = pd.to_numeric(df[col_map["oi"]], errors="coerce")

    if "noncomm_long" not in result.columns or "noncomm_short" not in result.columns:
        return pd.DataFrame()

    result = result.dropna(subset=["noncomm_long", "noncomm_short"]).reset_index(drop=True)

    if "open_interest" in result.columns and result["date"].duplicated().any():
        result = (
            result.sort_values("open_interest", ascending=False)
            .drop_duplicates("date", keep="first")
            .sort_values("date")
            .reset_index(drop=True)
        )

    return result


def extract_market_fin(df_fin, market_name_contains):
    """Extract a single market from Financial Futures COT by name substring."""
    mask = df_fin["Market_and_Exchange_Names"].str.contains(
        market_name_contains, case=False, na=False
    )
    df = df_fin[mask].copy()

    if len(df) == 0:
        return pd.DataFrame()

    date_col = "Report_Date_as_YYYY-MM-DD" if "Report_Date_as_YYYY-MM-DD" in df.columns else None
    if date_col is None:
        for col in df.columns:
            if "date" in col.lower() and "yyyy" in col.lower():
                date_col = col
                break
    df["date"] = pd.to_datetime(df[date_col])
    df = df.sort_values("date").reset_index(drop=True)

    result = pd.DataFrame()
    result["date"] = df["date"]

    col_map = {}
    for col in df.columns:
        cl = col.strip()
        cll = cl.lower().replace(" ", "_")

        if "asset_mgr" in cll or "Asset_Mgr" in cl:
            if "long" in cll and "spread" not in cll and "change" not in cll and "pct" not in cll:
                col_map["am_long"] = col
            elif "short" in cll and "spread" not in cll and "change" not in cll and "pct" not in cll:
                col_map["am_short"] = col

        if "lev_money" in cll or "Lev_Money" in cl:
            if "long" in cll and "spread" not in cll and "change" not in cll and "pct" not in cll:
                col_map["lm_long"] = col
            elif "short" in cll and "spread" not in cll and "change" not in cll and "pct" not in cll:
                col_map["lm_short"] = col

        if "nonrept" in cll or "NonRept" in cl:
            if "long" in cll and "change" not in cll and "pct" not in cll:
                col_map["retail_long"] = col
            elif "short" in cll and "change" not in cll and "pct" not in cll:
                col_map["retail_short"] = col

        if "open_interest" in cll and "change" not in cll:
            if "oi" not in col_map:
                col_map["oi"] = col

    if "am_long" in col_map and "lm_long" in col_map:
        result["noncomm_long"] = (
            pd.to_numeric(df[col_map["am_long"]], errors="coerce")
            + pd.to_numeric(df[col_map["lm_long"]], errors="coerce")
        )
        result["noncomm_short"] = (
            pd.to_numeric(df[col_map["am_short"]], errors="coerce")
            + pd.to_numeric(df[col_map["lm_short"]], errors="coerce")
        )
    elif "am_long" in col_map:
        result["noncomm_long"] = pd.to_numeric(df[col_map["am_long"]], errors="coerce")
        result["noncomm_short"] = pd.to_numeric(df[col_map["am_short"]], errors="coerce")

    if "retail_long" in col_map:
        result["retail_long"] = pd.to_numeric(df[col_map["retail_long"]], errors="coerce")
        result["retail_short"] = pd.to_numeric(df[col_map["retail_short"]], errors="coerce")

    if "oi" in col_map:
        result["open_interest"] = pd.to_numeric(df[col_map["oi"]], errors="coerce")

    if "noncomm_long" not in result.columns:
        return pd.DataFrame()

    result = result.dropna(subset=["noncomm_long", "noncomm_short"]).reset_index(drop=True)
    return result


def compute_cot_index(df_market, lookback=COT_INDEX_LOOKBACK):
    """COT Index: normalize net positioning over lookback window (0-100)."""
    df = df_market.copy()

    df["sm_net"] = df["noncomm_long"] - df["noncomm_short"]
    sm_min = df["sm_net"].rolling(lookback, min_periods=10).min()
    sm_max = df["sm_net"].rolling(lookback, min_periods=10).max()
    sm_range = (sm_max - sm_min).replace(0, 1)
    df["cot_index_sm"] = ((df["sm_net"] - sm_min) / sm_range) * 100

    if "retail_long" in df.columns and "retail_short" in df.columns:
        df["retail_net"] = df["retail_long"] - df["retail_short"]
        r_min = df["retail_net"].rolling(lookback, min_periods=10).min()
        r_max = df["retail_net"].rolling(lookback, min_periods=10).max()
        r_range = (r_max - r_min).replace(0, 1)
        df["cot_index_retail"] = ((df["retail_net"] - r_min) / r_range) * 100
        df["cot_index_retail_inv"] = 100 - df["cot_index_retail"]

    keep = ["date", "noncomm_long", "noncomm_short", "sm_net", "cot_index_sm", "open_interest"]
    if "retail_long" in df.columns:
        keep += ["retail_long", "retail_short", "cot_index_retail_inv"]
    return df[[c for c in keep if c in df.columns]]
