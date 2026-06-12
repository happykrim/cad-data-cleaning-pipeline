#!/usr/bin/env python
"""
Pipeline step: scrape CAD Property Land acreage for rows still missing legal_acreage.

Recommended location in your pipeline:
    1_fill_missing_legal_acreage_no_arg_updated.py
    1b_fill_missing_legal_acreage_from_cad_no_arg.py   <-- this script
    2_remap_essential_fields_add_lists_no_arg.py

No command-line arguments. Edit the CONFIG section below, then run:
    python 1b_fill_missing_legal_acreage_from_cad_no_arg.py

What it does
------------
- Reads county folders from INPUT_ROOT, usually the output from step 1.
- Finds rows where legal_acreage is still blank.
- Gets the county CAD website link from the same Google Sheet/settings pattern used by
  property_type_enrichment_uc_chrome_no_args.py, or from COUNTY_APPRAISAL_WEBSITES.
- Builds BIS/eSearch URLs like /Property/View/{id} and /Property/View/R{id}.
- Tries a fast browser-like HTTP request first, then falls back to undetected Chrome/Selenium when needed.
- Detects temporary BIS/CAD generic error pages and retries them instead of marking them as no land table.
- Parses the Property Land table and extracts/sums the Acreage column.
- Writes the acreage back to the same legal_acreage column in a live/resumable output file.
- Adds trace columns showing URL, status, raw acreage, method, attempts, and timestamp.
- Runs multiple counties at the same time and uses per-county browser workers.
- Performs a per-county capacity probe and then adapts delays/cooldowns only for real site/transport failures.
  Valid no-land/no-acreage pages no longer trigger one-minute cooldowns.
- Generates CSV and HTML reports at the end.

Install requirements
--------------------
    pip install pandas beautifulsoup4 lxml openpyxl undetected-chromedriver selenium gspread oauth2client

Notes
-----
This script is intentionally respectful: it does not try to bypass access controls.
If a county site shows a block/captcha/rate-limit page, the script cools down, slows
that county, and records the row status for review.
"""

from __future__ import annotations

import concurrent.futures as futures
import html
import json
import logging
import math
import os
import random
import re
import shutil
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, UTC
from pathlib import Path
from statistics import mean
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse, urlunparse

import pandas as pd
from bs4 import BeautifulSoup, FeatureNotFound

try:
    import undetected_chromedriver as uc
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: undetected-chromedriver. Install it with:\n"
        "    pip install undetected-chromedriver selenium"
    ) from exc

from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError:
    gspread = None
    ServiceAccountCredentials = None

try:
    import property_type_enrichment_uc_settings as base_settings
except Exception:
    base_settings = None


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

SCRIPT_VERSION = "cad_legal_acreage_fast_stable_v2"

# This should be the OUTPUT_ROOT from 1_fill_missing_legal_acreage_no_arg_updated.py.
INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_acres_data_counties\_by_priority\priority_1\step_2_legal_acreage_filled"

# New pipeline output folder. Point 2_remap_essential_fields_add_lists_no_arg.py to this folder.
# Set OUTPUT_ROOT = None and IN_PLACE_UPDATE = True only if you intentionally want to update INPUT_ROOT files directly.
OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_acres_data_counties\_by_priority\priority_1\step_2b_legal_acreage_cad_filled"
IN_PLACE_UPDATE = False

# Leave empty to process every eligible county folder found in INPUT_ROOT.
# Example: COUNTIES_TO_RUN = ["Comal", "Hays"]
COUNTIES_TO_RUN: List[str] = []

SUPPORTED_INPUT_EXTENSIONS = {".csv", ".xlsx", ".xls"}
OVERWRITE_OUTPUT = False
RESUME_INCOMPLETE_OUTPUT = True

# Retry rows that were marked failed/blocked/site_error in a previous run.
# This is enabled by default for this CAD step because county websites can return
# temporary error pages even when the same URL works a moment later.
RESCRAPE_FAILED_ROWS = True

# Old versions of this script could classify temporary CAD error pages as
# no_land_table. Rows with terminal no-fill statuses from an older parser version
# are retried once; rows produced by this SCRIPT_VERSION are skipped on resume.
RETRY_STALE_TERMINAL_NO_FILL_ROWS_ON_RESUME = True

# After this parser has confirmed property_not_found/no_land_table/no_acreage_value,
# do not re-scrape those rows on later resumes unless you intentionally set this True.
RESCRAPE_CURRENT_TERMINAL_NO_FILL_ROWS = False

# If a browser worker crashes, retry any rows that never reached the progress
# callback before writing a final summary. This prevents counties from being
# marked complete when only part of a worker chunk actually ran.
RETRY_UNPROCESSED_ROWS_AFTER_WORKER_CRASH = True

# Save the output file after every scraped row. This is safest/resumable, especially for CSV.
LIVE_SAVE_ENABLED = True
LIVE_SAVE_EVERY_N_ROWS = 1
WRITE_OUTPUT_ATOMICALLY = True
TERMINAL_PROGRESS_EVERY_N_ROWS = 1

# Legal acreage behavior.
LEGAL_ACREAGE_COLUMN_CANDIDATES = [
    "legal_acreage",
    "legal acreage",
    "legal_acres",
    "legal acres",
    "legal_acre",
    "acreage",
]
PROPERTY_ID_COLUMN_CANDIDATES = [
    "prop_id",
    "prop_id_text",
    "quick_ref_id",
    "quick ref id",
    "Quick Ref ID",
    "property_id",
    "Property ID",
    "property id",
    "PROP_ID",
    "id",
    "ID",
]
TREAT_ZERO_AS_MISSING = False
ROUND_ACRES_DECIMALS = 6
ACCEPT_ZERO_ACREAGE_FROM_CAD = False

# Property Land table behavior. If multiple land rows exist, sum their acreage by default.
LAND_ACREAGE_AGGREGATION = "sum"  # valid: "sum", "first", "max"
USE_SQFT_FALLBACK_IN_PROPERTY_LAND_TABLE = True
SQFT_PER_ACRE = 43560.0

# URL behavior. Most BIS/eSearch sites use /Property/View/{id}; some use an R prefix.
PROPERTY_ID_URL_FORMATS = getattr(base_settings, "PROPERTY_ID_URL_FORMATS", ["{id}", "R{id}"])
TAX_YEAR = getattr(base_settings, "TAX_YEAR", None)

# Google Sheet / county website lookup.
# The script can read the same tracker sheet used by property_type_enrichment_uc_chrome_no_args.py.
READ_WEBSITE_LINKS_FROM_GOOGLE_SHEET = True
NO_SHEET = bool(getattr(base_settings, "NO_SHEET", False)) if base_settings else False
DRY_RUN_SHEET_UPDATES = bool(getattr(base_settings, "DRY_RUN_SHEET_UPDATES", False)) if base_settings else True
GSHEET_URL = getattr(base_settings, "GSHEET_URL", "") if base_settings else ""
CREDENTIALS_JSON_FILE = getattr(base_settings, "CREDENTIALS_JSON_FILE", "") if base_settings else ""
WORKSHEET_NAME = getattr(base_settings, "WORKSHEET_NAME", None) if base_settings else None
SHEET_HEADER_COUNTY = getattr(base_settings, "SHEET_HEADER_COUNTY", "County") if base_settings else "County"
SHEET_HEADER_APPRAISAL_WEBSITE = getattr(base_settings, "SHEET_HEADER_APPRAISAL_WEBSITE", "Appraisal Website") if base_settings else "Appraisal Website"
SHEET_HEADER_HAS_LINK = getattr(base_settings, "SHEET_HEADER_HAS_LINK", "Has Link") if base_settings else "Has Link"
SHEET_HEADER_CATEGORY = getattr(base_settings, "SHEET_HEADER_CATEGORY", "Category") if base_settings else "Category"
ALLOWED_CATEGORY_VALUES = getattr(base_settings, "ALLOWED_CATEGORY_VALUES", {"-1", "1", "category 1", "cat 1", "c1"}) if base_settings else {"-1", "1", "category 1", "cat 1", "c1"}
PROCESS_ONLY_COUNTIES_WITH_LINK_AND_ALLOWED_CATEGORY = True

# Optional sheet progress update. Disabled by default so this new step does not overwrite
# the existing "Property Type Scraped" tracker column. If you add a dedicated sheet column,
# set UPDATE_GOOGLE_SHEET_STATUS=True and SHEET_HEADER_CAD_LEGAL_ACREAGE_SCRAPED to that header.
UPDATE_GOOGLE_SHEET_STATUS = False
SHEET_HEADER_CAD_LEGAL_ACREAGE_SCRAPED = "CAD Legal Acreage Scraped"
SHEET_UPDATE_EVERY_N_ROWS = 25

# If the Google Sheet is disabled/unavailable, put county website links here.
# Key can be "Comal" or "Comal County". Value can be "comalad.org" or an eSearch URL.
COUNTY_APPRAISAL_WEBSITES: Dict[str, str] = {
    # "Comal": "comalad.org",
}

# Optional direct base URL overrides, used after the appraisal website is derived.
# Example: COUNTY_ESEARCH_BASE_URL_OVERRIDES = {"Comal": "https://esearch.comalad.org"}
COUNTY_ESEARCH_BASE_URL_OVERRIDES: Dict[str, str] = {}

# Chrome / browser behavior.
HEADLESS = bool(getattr(base_settings, "HEADLESS", False)) if base_settings else False
WINDOW_SIZE = getattr(base_settings, "WINDOW_SIZE", "1366,900") if base_settings else "1366,900"
PAGE_LOAD_TIMEOUT_SECONDS = int(getattr(base_settings, "PAGE_LOAD_TIMEOUT_SECONDS", 45)) if base_settings else 45
ELEMENT_WAIT_SECONDS = int(getattr(base_settings, "ELEMENT_WAIT_SECONDS", 15)) if base_settings else 15
CHROME_USER_DATA_DIR = getattr(base_settings, "CHROME_USER_DATA_DIR", None) if base_settings else None
CHROME_BINARY_LOCATION = getattr(base_settings, "CHROME_BINARY_LOCATION", None) if base_settings else None
CHROME_START_ATTEMPTS = int(getattr(base_settings, "CHROME_START_ATTEMPTS", 3)) if base_settings else 3
CHROME_START_STAGGER_SECONDS = float(getattr(base_settings, "CHROME_START_STAGGER_SECONDS", 2.5)) if base_settings else 2.5
CLEAR_UNDETECTED_CHROMEDRIVER_CACHE_ON_START = bool(getattr(base_settings, "CLEAR_UNDETECTED_CHROMEDRIVER_CACHE_ON_START", False)) if base_settings else False
RESTART_BROWSER_EVERY_N_PAGES = int(getattr(base_settings, "RESTART_BROWSER_EVERY_N_PAGES", 250)) if base_settings else 250

# Overall concurrency. Keep this conservative by default, but do not inherit the
# very slow property-type scraper pacing unless CAD-specific settings are added.
# Optional overrides can be placed in property_type_enrichment_uc_settings.py as:
# CAD_ACREAGE_MAX_COUNTY_WORKERS, CAD_ACREAGE_MAX_BROWSER_WORKERS_PER_COUNTY, etc.
MAX_COUNTY_WORKERS = int(getattr(base_settings, "CAD_ACREAGE_MAX_COUNTY_WORKERS", getattr(base_settings, "MAX_COUNTY_WORKERS", 4))) if base_settings else 4
MAX_BROWSER_WORKERS_PER_COUNTY = int(getattr(base_settings, "CAD_ACREAGE_MAX_BROWSER_WORKERS_PER_COUNTY", max(2, min(3, getattr(base_settings, "MAX_BROWSER_WORKERS_PER_COUNTY", 2))))) if base_settings else 3

# Fast path: BIS/eSearch property pages are usually server-rendered HTML. Trying a
# normal HTTP GET first is much faster than opening every parcel in Chrome. If a
# site needs the browser, the script falls back to Selenium automatically.
USE_FAST_HTTP_FIRST = bool(getattr(base_settings, "CAD_ACREAGE_USE_FAST_HTTP_FIRST", True)) if base_settings else True
FALLBACK_TO_BROWSER_AFTER_HTTP_FAILURE = bool(getattr(base_settings, "CAD_ACREAGE_FALLBACK_TO_BROWSER", True)) if base_settings else True
FAST_HTTP_TIMEOUT_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_FAST_HTTP_TIMEOUT_SECONDS", 18.0)) if base_settings else 18.0
FAST_HTTP_RETRIES_PER_URL = int(getattr(base_settings, "CAD_ACREAGE_FAST_HTTP_RETRIES_PER_URL", 1)) if base_settings else 1
FAST_HTTP_USER_AGENT = getattr(
    base_settings,
    "CAD_ACREAGE_FAST_HTTP_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
) if base_settings else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# Respectful pacing / adaptive throttling. These defaults are faster than the
# original property-type scraper, because the script only fetches missing acreage
# and now treats valid no-land/no-acreage pages as data outcomes, not failures.
MIN_DELAY_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_MIN_DELAY_SECONDS", 0.35)) if base_settings else 0.35
MAX_DELAY_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_MAX_DELAY_SECONDS", 1.50)) if base_settings else 1.50
LONG_PAUSE_EVERY_N_PAGES = int(getattr(base_settings, "CAD_ACREAGE_LONG_PAUSE_EVERY_N_PAGES", 150)) if base_settings else 150
LONG_PAUSE_MIN_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_LONG_PAUSE_MIN_SECONDS", 8.0)) if base_settings else 8.0
LONG_PAUSE_MAX_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_LONG_PAUSE_MAX_SECONDS", 20.0)) if base_settings else 20.0
COOLDOWN_AFTER_BLOCK_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_COOLDOWN_AFTER_BLOCK_SECONDS", 90.0)) if base_settings else 90.0
MAX_CONSECUTIVE_FAILURES_PER_BROWSER = int(getattr(base_settings, "CAD_ACREAGE_MAX_CONSECUTIVE_FAILURES_PER_BROWSER", 8)) if base_settings else 8
MAX_ATTEMPTS_PER_PROPERTY = int(getattr(base_settings, "CAD_ACREAGE_MAX_ATTEMPTS_PER_PROPERTY", 3)) if base_settings else 3
RETRY_DELAY_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_RETRY_DELAY_SECONDS", 2.0)) if base_settings else 2.0

# Dynamic capacity probe. The probe opens a small number of real property pages per county.
ENABLE_CAPACITY_PROBE = True
CAPACITY_PROBE_MIN_WORKERS = 1
CAPACITY_PROBE_MAX_WORKERS = MAX_BROWSER_WORKERS_PER_COUNTY
CAPACITY_PROBE_PAGES_PER_WORKER = 1
CAPACITY_PROBE_MAX_FAILURE_RATE = 0.35
CAPACITY_PROBE_MAX_BLOCKED = 0
CAPACITY_PROBE_MAX_AVG_SECONDS = 35.0
CAPACITY_PROBE_COOLDOWN_BETWEEN_LEVELS_SECONDS = 8.0

# Adaptive throttling once scraping starts. Only real site/transport failures
# affect throttling. Data outcomes like no_land_table or no_acreage_value do not.
ADAPTIVE_WINDOW_SIZE = int(getattr(base_settings, "CAD_ACREAGE_ADAPTIVE_WINDOW_SIZE", 20)) if base_settings else 20
ADAPTIVE_FAILURE_RATE_THRESHOLD = float(getattr(base_settings, "CAD_ACREAGE_ADAPTIVE_FAILURE_RATE_THRESHOLD", 0.50)) if base_settings else 0.50
ADAPTIVE_MAX_DELAY_MULTIPLIER = float(getattr(base_settings, "CAD_ACREAGE_ADAPTIVE_MAX_DELAY_MULTIPLIER", 3.0)) if base_settings else 3.0
ADAPTIVE_SUCCESS_DECAY = float(getattr(base_settings, "CAD_ACREAGE_ADAPTIVE_SUCCESS_DECAY", 0.94)) if base_settings else 0.94
ADAPTIVE_FAILURE_INCREASE = float(getattr(base_settings, "CAD_ACREAGE_ADAPTIVE_FAILURE_INCREASE", 1.18)) if base_settings else 1.18
ADAPTIVE_BLOCK_INCREASE = float(getattr(base_settings, "CAD_ACREAGE_ADAPTIVE_BLOCK_INCREASE", 1.60)) if base_settings else 1.60
ADAPTIVE_SOFT_COOLDOWN_SECONDS = float(getattr(base_settings, "CAD_ACREAGE_ADAPTIVE_SOFT_COOLDOWN_SECONDS", 8.0)) if base_settings else 8.0

REQUEST_OK_STATUSES = {"success", "no_land_table", "no_acreage_value", "property_not_found"}
TERMINAL_NO_FILL_STATUSES = {"no_land_table", "no_acreage_value", "property_not_found"}
THROTTLE_BAD_STATUSES = {"blocked", "site_error", "failed"}
FAST_HTTP_ACCEPT_STATUSES = REQUEST_OK_STATUSES

# Logging / reports.
LOG_LEVEL = "INFO"
LOG_FILE = "cad_legal_acreage_scraper.log"
WRITE_DETAILED_SCRAPE_EVENTS_CSV = True
WRITE_HTML_REPORT = True


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=getattr(logging, str(LOG_LEVEL).upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(threadName)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)

_DRIVER_CREATION_LOCK = threading.Lock()


# =============================================================================
# Data models
# =============================================================================

@dataclass
class CountySheetRow:
    county: str
    row_number: int = 0
    appraisal_website: str = ""
    has_link: str = ""
    category: str = ""


@dataclass
class LandRow:
    row_number: int
    raw_acreage: str = ""
    acreage: Optional[float] = None
    raw_sqft: str = ""
    sqft: Optional[float] = None
    description: str = ""
    land_type: str = ""
    source: str = "acreage_column"


@dataclass
class CadAcreageResult:
    input_id: str
    status: str
    url: str = ""
    fetch_mode: str = ""
    acreage: Optional[float] = None
    raw_acreage: str = ""
    method: str = ""
    land_rows: List[LandRow] = field(default_factory=list)
    attempts: int = 0
    elapsed_seconds: float = 0.0
    error: str = ""
    scraped_at: str = ""
    property_id_page: str = ""
    quick_ref_id_page: str = ""


@dataclass
class FileSummary:
    county: str
    input_file: str
    output_file: str
    status: str
    total_rows: int = 0
    missing_before: int = 0
    targeted_rows: int = 0
    already_completed_rows: int = 0
    filled_from_cad: int = 0
    success_no_fill: int = 0
    failed: int = 0
    blocked: int = 0
    property_not_found: int = 0
    no_land_table: int = 0
    no_acreage_value: int = 0
    site_error: int = 0
    unprocessed: int = 0
    missing_after: int = 0
    worker_count: int = 1
    elapsed_seconds: float = 0.0
    error: str = ""


# =============================================================================
# Text / numeric helpers
# =============================================================================

def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_header(value: Any) -> str:
    return clean_text(value).strip().lower()


def normalize_column_name(name: Any) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def normalize_county_name(value: Any) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"\s+county\s*,?\s*texas$", "", text)
    text = re.sub(r"\s+county$", "", text)
    return text.strip()


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    text = str(value).strip().lower()
    if text in {"", "nan", "none", "null", "n/a", "na", "--", "unknown", "not available"}:
        return True
    return False


def parse_number(value: Any) -> Optional[float]:
    if is_missing(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = clean_text(value)
    # Keep decimal points and minus signs; remove currency/commas/labels.
    text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-.", ".-"}:
        return None
    # Protect against strings with multiple decimals.
    if text.count(".") > 1:
        parts = text.split(".")
        text = parts[0] + "." + "".join(parts[1:])
    try:
        return float(text)
    except Exception:
        return None


def round_or_none(value: Any, digits: int = ROUND_ACRES_DECIMALS) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return round(float(value), digits)
    except Exception:
        return None


def legal_acreage_is_missing(value: Any) -> bool:
    if is_missing(value):
        return True
    if TREAT_ZERO_AS_MISSING:
        parsed = parse_number(value)
        return parsed is not None and abs(parsed) <= 1e-12
    return False


def parse_acreage_value_to_float(value: Any) -> Optional[float]:
    """
    Convert any scraped or existing acreage-looking value into a plain float.

    This is intentionally tolerant because CAD pages and CSV/XLSX files may hold
    acreage as strings, ints, floats, values with commas, values with labels, or
    pandas extension scalar values. Missing/unparseable values return None.
    """
    parsed = parse_number(value)
    if parsed is None:
        return None
    return round_or_none(parsed, ROUND_ACRES_DECIMALS)


def make_dataframe_columns_mutable(df: pd.DataFrame, columns: Iterable[str]) -> None:
    """
    Convert selected columns to object dtype before live updates.

    Newer pandas versions can create strict string columns when files are read
    with dtype=str. Assigning a scraped float such as 0.59 or an int such as 3
    into those columns can raise:
        Invalid value '0.59' for dtype 'str'
    Object dtype keeps IDs/text as text while allowing numeric scrape results.
    """
    for col in columns:
        if col and col in df.columns:
            try:
                df[col] = df[col].astype("object")
            except Exception:
                # Best effort. If conversion fails, leave the column as-is and
                # the assignment helper below will still try to write safely.
                pass


def normalize_existing_legal_acreage_values(df: pd.DataFrame, legal_col: str) -> None:
    """
    Normalize existing non-empty legal_acreage values to floats when possible.

    Blank values stay blank so the missing-row logic still works. Values that do
    not parse as acreage are preserved for review instead of being deleted.
    """
    if legal_col not in df.columns:
        return

    make_dataframe_columns_mutable(df, [legal_col])
    for idx in df.index:
        current = df.at[idx, legal_col]
        if is_missing(current):
            df.at[idx, legal_col] = ""
            continue
        parsed = parse_acreage_value_to_float(current)
        if parsed is not None:
            df.at[idx, legal_col] = parsed


def prepare_dataframe_for_live_updates(df: pd.DataFrame, legal_col: str) -> None:
    """Prepare columns that receive scraped values so callback writes cannot fail."""
    normalize_existing_legal_acreage_values(df, legal_col)
    trace_cols = [
        "cad_legal_acreage_status",
        "cad_legal_acreage_source",
        "cad_legal_acreage_method",
        "cad_legal_acreage_raw",
        "cad_legal_acreage_url",
        "cad_legal_acreage_error",
        "cad_legal_acreage_attempts",
        "cad_legal_acreage_elapsed_seconds",
        "cad_legal_acreage_land_rows_json",
        "cad_legal_acreage_scraped_at",
        "cad_legal_acreage_worker",
        "cad_legal_acreage_fetch_mode",
        "cad_legal_acreage_parser_version",
    ]
    make_dataframe_columns_mutable(df, trace_cols)


def clean_property_id(value: Any) -> str:
    text = clean_text(value)
    if text.endswith(".0"):
        text = text[:-2]
    return text


def make_soup(html_text: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html_text or "", "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html_text or "", "html.parser")


def html_escape(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def format_duration(seconds: Optional[float]) -> str:
    try:
        if seconds is None:
            return ""
        seconds_float = float(seconds)
        if seconds_float < 0 or math.isnan(seconds_float):
            return ""
    except Exception:
        return ""
    seconds = int(round(seconds_float))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def status_counts_as_request_ok(status: str) -> bool:
    return clean_text(status).lower() in REQUEST_OK_STATUSES


def status_should_throttle(status: str) -> bool:
    return clean_text(status).lower() in THROTTLE_BAD_STATUSES


def status_is_terminal_no_fill(status: str) -> bool:
    return clean_text(status).lower() in TERMINAL_NO_FILL_STATUSES


# =============================================================================
# URL helpers
# =============================================================================

def derive_esearch_base_url(appraisal_website: str) -> str:
    raw = clean_text(appraisal_website)
    if not raw:
        raise ValueError("Empty appraisal website")
    if not re.match(r"^https?://", raw, flags=re.I):
        raw = "https://" + raw

    parsed = urlparse(raw)
    host = parsed.netloc.lower().strip()
    host = host.replace("www.", "")

    if host.startswith("esearch."):
        esearch_host = host
    else:
        esearch_host = "esearch." + host

    return urlunparse(("https", esearch_host, "", "", "", "")).rstrip("/")


def build_candidate_id_values(prop_id: str) -> List[str]:
    raw = clean_property_id(prop_id)
    if not raw:
        return []

    no_prefix = raw[1:] if re.match(r"^[A-Za-z]\d+$", raw) else raw
    values: List[str] = []

    for fmt in PROPERTY_ID_URL_FORMATS:
        base_for_format = no_prefix if str(fmt).upper().startswith("R{") else raw
        try:
            candidate = str(fmt).format(id=base_for_format)
        except Exception:
            candidate = raw
        candidate = clean_text(candidate)
        if candidate and candidate not in values:
            values.append(candidate)

    if raw not in values:
        values.insert(0, raw)

    return values


def build_property_url(base_url: str, candidate_id: str, tax_year: Optional[str] = None) -> str:
    url = f"{base_url.rstrip('/')}/Property/View/{candidate_id}"
    if tax_year:
        url += f"?year={tax_year}"
    return url


# =============================================================================
# Sheet / county website lookup
# =============================================================================

class WebsiteLinkLookup:
    def __init__(self) -> None:
        self.enabled = bool(READ_WEBSITE_LINKS_FROM_GOOGLE_SHEET and not NO_SHEET)
        self.worksheet = None
        self.header_map: Dict[str, int] = {}
        self.rows_by_county: Dict[str, CountySheetRow] = {}
        self.lock = threading.Lock()

        # Always load manual mappings first. Sheet rows override them if available.
        for county, url in COUNTY_APPRAISAL_WEBSITES.items():
            key = normalize_county_name(county)
            self.rows_by_county[key] = CountySheetRow(county=county, appraisal_website=url, has_link="Yes", category="manual")

        if not self.enabled:
            logging.warning("Google Sheet website lookup is disabled. Using COUNTY_APPRAISAL_WEBSITES only.")
            return
        if not GSHEET_URL:
            logging.warning("GSHEET_URL is blank. Using COUNTY_APPRAISAL_WEBSITES only.")
            self.enabled = False
            return
        if gspread is None or ServiceAccountCredentials is None:
            raise RuntimeError("gspread/oauth2client not installed. Install them or set NO_SHEET=True.")

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_JSON_FILE, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(GSHEET_URL)
        self.worksheet = spreadsheet.worksheet(WORKSHEET_NAME) if WORKSHEET_NAME else spreadsheet.sheet1
        self._load_sheet_rows()

    def _load_sheet_rows(self) -> None:
        values = self.worksheet.get_all_values()
        if not values:
            raise RuntimeError("Google Sheet appears empty.")

        headers = values[0]
        self.header_map = {clean_text(h): i + 1 for i, h in enumerate(headers) if clean_text(h)}

        def cell(row: Sequence[str], header: str) -> str:
            idx = self.header_map.get(header)
            if not idx or idx > len(row):
                return ""
            return clean_text(row[idx - 1])

        for row_idx, row in enumerate(values[1:], start=2):
            county = cell(row, SHEET_HEADER_COUNTY)
            if not county:
                continue
            item = CountySheetRow(
                county=county,
                row_number=row_idx,
                appraisal_website=cell(row, SHEET_HEADER_APPRAISAL_WEBSITE),
                has_link=cell(row, SHEET_HEADER_HAS_LINK),
                category=cell(row, SHEET_HEADER_CATEGORY),
            )
            self.rows_by_county[normalize_county_name(county)] = item

        logging.info("Loaded %s county website rows from Google Sheet.", len(self.rows_by_county))

    def get_county_row(self, county_folder_name: str) -> Optional[CountySheetRow]:
        key = normalize_county_name(county_folder_name)
        row = self.rows_by_county.get(key)
        if row is None:
            return None
        if not PROCESS_ONLY_COUNTIES_WITH_LINK_AND_ALLOWED_CATEGORY:
            return row
        if row.category == "manual":
            return row
        has_link_ok = row.has_link.strip().lower() == "yes"
        category_ok = row.category.strip().lower() in {str(x).lower() for x in ALLOWED_CATEGORY_VALUES}
        if has_link_ok and category_ok and row.appraisal_website:
            return row
        return None

    def update_status(self, row: CountySheetRow, status: str) -> None:
        if not UPDATE_GOOGLE_SHEET_STATUS:
            logging.info("[SHEET update disabled] %s | %s", row.county, status)
            return
        if not self.enabled or self.worksheet is None or row.row_number <= 0:
            logging.info("[SHEET unavailable] %s | %s", row.county, status)
            return
        if DRY_RUN_SHEET_UPDATES:
            logging.info("[SHEET dry-run] row=%s | %s", row.row_number, status)
            return

        col = self.header_map.get(SHEET_HEADER_CAD_LEGAL_ACREAGE_SCRAPED)
        if not col:
            logging.warning("Sheet header not found: %s. Skipping sheet update.", SHEET_HEADER_CAD_LEGAL_ACREAGE_SCRAPED)
            return
        with self.lock:
            self.worksheet.update_cell(row.row_number, col, status)


# =============================================================================
# File helpers
# =============================================================================

def read_input_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(path, dtype=str, keep_default_na=False)

    # Convert away from strict pandas string dtype so live scrape callbacks can
    # safely place floats/ints into legal_acreage and trace columns.
    return df.astype("object")


def write_output_file(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.stem}.__tmp__{path.suffix}") if WRITE_OUTPUT_ATOMICALLY else path
    if path.suffix.lower() == ".csv":
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    else:
        df.to_excel(tmp_path, index=False)
    if tmp_path != path:
        os.replace(tmp_path, path)


def find_column(df: pd.DataFrame, candidates: Sequence[str], required: bool = False) -> Optional[str]:
    if df.empty:
        if required:
            raise ValueError("Cannot detect columns from an empty dataframe.")
        return None

    exact_map = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in exact_map:
            return exact_map[key]

    normalized_map = {normalize_column_name(c): c for c in df.columns}
    for candidate in candidates:
        key = normalize_column_name(candidate)
        if key in normalized_map:
            return normalized_map[key]

    if required:
        raise KeyError(f"Could not find required column. Candidates={candidates}. Available={list(df.columns)}")
    return None


def find_property_id_column(df: pd.DataFrame) -> Optional[str]:
    col = find_column(df, PROPERTY_ID_COLUMN_CANDIDATES, required=False)
    if col:
        return col
    for c in df.columns:
        key = normalize_column_name(c)
        if "prop" in key and "id" in key:
            return c
    return None


def find_legal_acreage_column(df: pd.DataFrame) -> Optional[str]:
    return find_column(df, LEGAL_ACREAGE_COLUMN_CANDIDATES, required=False)


TRACE_COLUMN_DEFAULTS: Dict[str, Any] = {
    "cad_legal_acreage_status": "",
    "cad_legal_acreage_source": "",
    "cad_legal_acreage_method": "",
    "cad_legal_acreage_raw": "",
    "cad_legal_acreage_url": "",
    "cad_legal_acreage_error": "",
    "cad_legal_acreage_attempts": "",
    "cad_legal_acreage_elapsed_seconds": "",
    "cad_legal_acreage_land_rows_json": "",
    "cad_legal_acreage_scraped_at": "",
    "cad_legal_acreage_worker": "",
    "cad_legal_acreage_fetch_mode": "",
    "cad_legal_acreage_parser_version": "",
}


def ensure_trace_columns(df: pd.DataFrame) -> None:
    for col, default in TRACE_COLUMN_DEFAULTS.items():
        if col not in df.columns:
            df[col] = pd.Series([default] * len(df), index=df.index, dtype="object")
        else:
            make_dataframe_columns_mutable(df, [col])


def discover_county_files(county_folder: Path) -> List[Path]:
    return [
        p for p in sorted(county_folder.iterdir())
        if p.is_file() and p.suffix.lower() in SUPPORTED_INPUT_EXTENSIONS and not p.name.startswith("~$")
    ]


def output_path_for(input_file: Path, county_folder: Path, output_root: Optional[Path]) -> Path:
    if IN_PLACE_UPDATE:
        return input_file
    if output_root is None:
        raise ValueError("OUTPUT_ROOT cannot be None unless IN_PLACE_UPDATE=True")
    county_output_folder = output_root / county_folder.name
    suffix = input_file.suffix.lower()
    if suffix == ".xls":
        suffix = ".xlsx"
    return county_output_folder / f"{input_file.stem}{suffix}"


# =============================================================================
# Chrome helpers
# =============================================================================

def maybe_cleanup_uc_cache() -> None:
    if not CLEAR_UNDETECTED_CHROMEDRIVER_CACHE_ON_START:
        return
    cache_dir = Path(os.environ.get("APPDATA", "")) / "undetected_chromedriver"
    if not cache_dir.exists():
        return
    try:
        shutil.rmtree(cache_dir)
        logging.info("Cleared undetected_chromedriver cache: %s", cache_dir)
    except Exception as exc:
        logging.warning("Could not clear undetected_chromedriver cache %s: %s", cache_dir, exc)


def create_driver() -> uc.Chrome:
    options = uc.ChromeOptions()
    options.page_load_strategy = "eager"

    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument(f"--window-size={WINDOW_SIZE}")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--lang=en-US,en")

    if CHROME_USER_DATA_DIR:
        # Do not use the same Chrome user-data-dir with multiple workers. Chrome locks profiles.
        options.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
    if CHROME_BINARY_LOCATION:
        options.binary_location = CHROME_BINARY_LOCATION

    attempts = max(1, int(CHROME_START_ATTEMPTS))
    last_exc: Optional[Exception] = None

    with _DRIVER_CREATION_LOCK:
        if CHROME_START_STAGGER_SECONDS > 0:
            time.sleep(random.uniform(0.5, CHROME_START_STAGGER_SECONDS))
        for attempt in range(1, attempts + 1):
            try:
                logging.info("Creating Chrome driver attempt %s/%s...", attempt, attempts)
                driver = uc.Chrome(options=options, use_subprocess=True)
                driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT_SECONDS)
                return driver
            except Exception as exc:
                last_exc = exc
                logging.warning("Chrome driver startup failed attempt %s/%s: %s", attempt, attempts, str(exc)[:300])
                time.sleep(3 + attempt * 2)

    raise RuntimeError(f"Could not create Chrome driver after {attempts} attempt(s): {last_exc}") from last_exc


def page_looks_blocked(html_text: str) -> bool:
    text = clean_text(make_soup(html_text).get_text(" ", strip=True)).lower()
    blocked_terms = [
        "access denied",
        "too many requests",
        "rate limit",
        "unusual traffic",
        "temporarily blocked",
        "verify you are human",
        "checking your browser",
        "captcha",
        "forbidden",
        "service unavailable",
    ]
    return any(term in text for term in blocked_terms)


def page_looks_transient_site_error(html_text: str) -> bool:
    """Detect the BIS/CAD generic error page shown in the screenshots.

    These pages are often temporary. They should be retried and should not be
    misclassified as no_land_table, because that would make the resume logic
    skip a row that may work in another browser/session.
    """
    soup = make_soup(html_text)
    title = clean_text(soup.title.get_text(" ", strip=True)).lower() if soup.title else ""
    text = clean_text(soup.get_text(" ", strip=True)).lower()
    transient_terms = [
        "an error occurred",
        "oops! something went wrong",
        "oops something went wrong",
        "please try again later",
        "something went wrong",
        "internal server error",
        "runtime error",
    ]
    return title == "error" or any(term in text for term in transient_terms)


def page_has_property_details(soup: BeautifulSoup) -> bool:
    if find_panel_by_heading(soup, "Property Details") is not None:
        return True
    text = clean_text(soup.get_text(" ", strip=True))
    return "Property ID:" in text and ("Legal Description:" in text or "Property Values" in text)


# =============================================================================
# CAD Property Land parsing
# =============================================================================

def find_panel_by_heading(soup: BeautifulSoup, heading_contains: str) -> Optional[Any]:
    target = heading_contains.lower()
    for panel in soup.select("div.panel"):
        heading = panel.select_one(".panel-heading")
        heading_text = clean_text(heading.get_text(" ", strip=True)) if heading else ""
        if target in heading_text.lower():
            return panel
    return None


def extract_property_id_page(soup: BeautifulSoup) -> Tuple[str, str]:
    details: Dict[str, str] = {}
    panel = find_panel_by_heading(soup, "Property Details")
    table = panel.select_one("table") if panel else None
    if not table:
        return "", ""
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"], recursive=False)
        i = 0
        while i < len(cells):
            cell = cells[i]
            if cell.name == "th":
                label = clean_text(cell.get_text(" ", strip=True)).rstrip(":")
                value = ""
                if i + 1 < len(cells) and cells[i + 1].name == "td":
                    value = clean_text(cells[i + 1].get_text(" ", strip=True))
                    i += 2
                else:
                    i += 1
                if label:
                    details[label] = value
                    continue
            strong = cell.find("strong") if cell else None
            if strong:
                label = clean_text(strong.get_text(" ", strip=True)).rstrip(":")
                full = clean_text(cell.get_text(" ", strip=True))
                strong_text = clean_text(strong.get_text(" ", strip=True))
                value = clean_text(full[len(strong_text):]) if strong_text and full.startswith(strong_text) else full
                if label:
                    details[label] = value
            i += 1
    return details.get("Property ID", ""), details.get("Quick Ref ID", "")


def get_cell_data_acres(cell: Any) -> str:
    if cell is None:
        return ""
    for node in [cell] + list(cell.select("[data-acres]")):
        if node and node.has_attr("data-acres"):
            val = clean_text(node.get("data-acres"))
            if val:
                return val
    return ""


def parse_table_headers(table: Any) -> List[str]:
    header_row = None
    for tr in table.find_all("tr"):
        ths = tr.find_all("th", recursive=False)
        if ths:
            header_row = tr
            break
    if not header_row:
        return []
    return [clean_text(th.get_text(" ", strip=True)) for th in header_row.find_all("th", recursive=False)]


def header_index(headers: Sequence[str], names: Iterable[str]) -> Optional[int]:
    wanted = {normalize_column_name(n) for n in names}
    for i, h in enumerate(headers):
        if normalize_column_name(h) in wanted:
            return i
    return None


def parse_land_table(table: Any) -> List[LandRow]:
    headers = parse_table_headers(table)
    if not headers:
        return []

    acreage_idx = header_index(headers, ["Acreage", "Acres", "Acre", "Legal Acreage"])
    sqft_idx = header_index(headers, ["Sqft", "SQFT", "Square Feet", "Sq Ft"])
    desc_idx = header_index(headers, ["Description", "Land Description"])
    type_idx = header_index(headers, ["Type", "Land Type"])

    if acreage_idx is None and sqft_idx is None:
        return []

    rows: List[LandRow] = []
    data_row_number = 0
    for tr in table.find_all("tr"):
        if tr.find_all("th", recursive=False):
            continue
        cells = tr.find_all("td", recursive=False)
        if not cells:
            continue
        data_row_number += 1

        def cell_text(idx: Optional[int]) -> str:
            if idx is None or idx >= len(cells):
                return ""
            return clean_text(cells[idx].get_text(" ", strip=True))

        raw_acreage = ""
        acreage = None
        source = "acreage_column"
        if acreage_idx is not None and acreage_idx < len(cells):
            data_acres = get_cell_data_acres(cells[acreage_idx])
            raw_acreage = data_acres or cell_text(acreage_idx)
            acreage = parse_number(raw_acreage)

        raw_sqft = cell_text(sqft_idx)
        sqft = parse_number(raw_sqft)

        if acreage is None and USE_SQFT_FALLBACK_IN_PROPERTY_LAND_TABLE and sqft is not None and sqft > 0:
            acreage = sqft / SQFT_PER_ACRE
            raw_acreage = raw_sqft
            source = "sqft_fallback"

        rows.append(
            LandRow(
                row_number=data_row_number,
                raw_acreage=raw_acreage,
                acreage=round_or_none(acreage),
                raw_sqft=raw_sqft,
                sqft=sqft,
                description=cell_text(desc_idx),
                land_type=cell_text(type_idx),
                source=source,
            )
        )
    return rows


def find_property_land_rows(soup: BeautifulSoup) -> Tuple[List[LandRow], str]:
    panel = find_panel_by_heading(soup, "Property Land")
    if panel:
        table = panel.select_one("table")
        if table:
            rows = parse_land_table(table)
            if rows:
                return rows, "property_land_panel"
            return [], "property_land_table_no_acreage_columns"
        return [], "property_land_panel_no_table"

    # Fallback: search any table with an Acreage and Sqft header.
    for table in soup.find_all("table"):
        headers = parse_table_headers(table)
        normalized = {normalize_column_name(h) for h in headers}
        if "acreage" in normalized and ("sqft" in normalized or "description" in normalized):
            rows = parse_land_table(table)
            if rows:
                return rows, "fallback_table_with_acreage_header"
    return [], "property_land_table_not_found"


def aggregate_land_rows(rows: Sequence[LandRow]) -> Tuple[Optional[float], str, str]:
    values = [r.acreage for r in rows if r.acreage is not None]
    if not values:
        return None, "", "no_numeric_acreage_values"

    if not ACCEPT_ZERO_ACREAGE_FROM_CAD:
        values = [v for v in values if abs(float(v)) > 1e-12]
    if not values:
        return None, "", "zero_acreage_not_accepted"

    method = f"property_land_{LAND_ACREAGE_AGGREGATION}"
    if LAND_ACREAGE_AGGREGATION == "first":
        value = values[0]
    elif LAND_ACREAGE_AGGREGATION == "max":
        value = max(values)
    else:
        value = sum(values)
        method = "property_land_sum"

    raw = " + ".join(str(r.raw_acreage) for r in rows if r.acreage is not None and (ACCEPT_ZERO_ACREAGE_FROM_CAD or abs(float(r.acreage)) > 1e-12))
    return round_or_none(value), raw, method


def parse_cad_legal_acreage_from_html(html_text: str) -> Tuple[Optional[float], str, str, List[LandRow], str, str, str]:
    soup = make_soup(html_text)
    body_text = clean_text(soup.get_text(" ", strip=True))

    if page_looks_transient_site_error(html_text):
        return None, "", "", [], "site_error", "", "cad_generic_error_page"
    if "Property Not Found" in body_text or "No data found" in body_text:
        return None, "", "", [], "property_not_found", "", "property_not_found"
    if page_looks_blocked(html_text):
        return None, "", "", [], "blocked", "", "blocked_or_rate_limited"

    valid_property_page = page_has_property_details(soup)
    property_id_page, quick_ref_id_page = extract_property_id_page(soup)
    rows, source = find_property_land_rows(soup)
    if not rows:
        if valid_property_page:
            return None, "", "", [], "no_land_table", property_id_page, f"confirmed_valid_property_page_without_land_table:{source}"
        title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""
        body_preview = body_text[:240]
        return None, "", "", [], "failed", property_id_page, f"property_page_not_recognized_or_land_table_not_found | title={title} | body={body_preview}"

    acreage, raw, method = aggregate_land_rows(rows)
    if acreage is None:
        return None, raw, method, rows, "no_acreage_value", property_id_page, "no_numeric_accepted_acreage_value"
    return acreage, raw, method, rows, "success", property_id_page, quick_ref_id_page


def land_rows_to_json(rows: Sequence[LandRow]) -> str:
    data = [
        {
            "row_number": r.row_number,
            "raw_acreage": r.raw_acreage,
            "acreage": r.acreage,
            "raw_sqft": r.raw_sqft,
            "sqft": r.sqft,
            "description": r.description,
            "land_type": r.land_type,
            "source": r.source,
        }
        for r in rows
    ]
    return json.dumps(data, ensure_ascii=False)


# =============================================================================
# Scraping
# =============================================================================

def wait_for_property_page(driver: uc.Chrome) -> None:
    try:
        WebDriverWait(driver, ELEMENT_WAIT_SECONDS).until(
            lambda d: (
                "Property Details" in d.page_source
                or "Property Land" in d.page_source
                or "Property Not Found" in d.page_source
                or "No data found" in d.page_source
                or "An Error Occurred" in d.page_source
                or "something went wrong" in d.page_source.lower()
                or "captcha" in d.page_source.lower()
                or "access denied" in d.page_source.lower()
            )
        )
    except TimeoutException:
        pass



def build_result_from_html_parse(
    input_id: str,
    url: str,
    html_text: str,
    attempts: int,
    start_time: float,
    fetch_mode: str,
    transport_error: str = "",
) -> CadAcreageResult:
    acreage, raw, method, rows, status, property_id_page, quick_ref_or_error = parse_cad_legal_acreage_from_html(html_text)
    elapsed = time.perf_counter() - start_time

    quick_ref_id_page = quick_ref_or_error if status == "success" else ""
    error = "" if status == "success" else (quick_ref_or_error or transport_error or status)
    if transport_error and status in {"failed", "site_error"} and transport_error not in error:
        error = f"{transport_error} | {error}" if error else transport_error

    return CadAcreageResult(
        input_id=input_id,
        status=status,
        url=url,
        fetch_mode=fetch_mode,
        acreage=acreage,
        raw_acreage=raw,
        method=method,
        land_rows=list(rows),
        attempts=attempts,
        elapsed_seconds=round(elapsed, 3),
        error=error,
        scraped_at=datetime.now().isoformat(timespec="seconds"),
        property_id_page=property_id_page,
        quick_ref_id_page=quick_ref_id_page,
    )


def fetch_html_fast(url: str) -> Tuple[str, str]:
    """Fetch a CAD property page with a normal browser-like HTTP request.

    Returns (html_text, transport_error). HTTP error bodies are still returned
    when available so the parser can classify BIS generic error pages.
    """
    headers = {
        "User-Agent": FAST_HTTP_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
    }
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=FAST_HTTP_TIMEOUT_SECONDS) as response:
            data = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            return data.decode(charset, errors="replace"), ""
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read()
            charset = exc.headers.get_content_charset() if exc.headers else "utf-8"
            html_text = body.decode(charset or "utf-8", errors="replace") if body else ""
        except Exception:
            html_text = ""
        return html_text, f"http_error_{exc.code}"
    except urllib.error.URLError as exc:
        return "", f"url_error: {str(exc.reason)[:180]}"
    except Exception as exc:
        return "", f"http_fetch_error: {type(exc).__name__}: {str(exc)[:180]}"


def load_and_parse_cad_legal_acreage_fast_http(
    base_url: str,
    prop_id: str,
    max_attempts: int = FAST_HTTP_RETRIES_PER_URL,
) -> CadAcreageResult:
    input_id = clean_property_id(prop_id)
    if not input_id:
        return CadAcreageResult(
            input_id=input_id,
            status="failed",
            fetch_mode="http",
            error="blank_property_id",
            scraped_at=datetime.now().isoformat(timespec="seconds"),
        )

    start_time = time.perf_counter()
    last_result: Optional[CadAcreageResult] = None
    attempts = max(1, int(max_attempts))

    for attempt in range(1, attempts + 1):
        for candidate_id in build_candidate_id_values(input_id):
            url = build_property_url(base_url, candidate_id, TAX_YEAR)
            html_text, transport_error = fetch_html_fast(url)
            if html_text:
                result = build_result_from_html_parse(
                    input_id=input_id,
                    url=url,
                    html_text=html_text,
                    attempts=attempt,
                    start_time=start_time,
                    fetch_mode="http",
                    transport_error=transport_error,
                )
                last_result = result

                # Try alternate id formats if this candidate was not found.
                if result.status == "property_not_found":
                    continue

                # Success and verified data-absence statuses are accepted from HTTP.
                if result.status in FAST_HTTP_ACCEPT_STATUSES:
                    return result

                # Otherwise let retry/browser fallback handle it.
                continue

            elapsed = time.perf_counter() - start_time
            last_result = CadAcreageResult(
                input_id=input_id,
                status="failed",
                url=url,
                fetch_mode="http",
                attempts=attempt,
                elapsed_seconds=round(elapsed, 3),
                error=transport_error or "empty_http_response",
                scraped_at=datetime.now().isoformat(timespec="seconds"),
            )

        if attempt < attempts:
            time.sleep(max(0.2, RETRY_DELAY_SECONDS) * attempt)

    if last_result is not None:
        return last_result

    return CadAcreageResult(
        input_id=input_id,
        status="failed",
        fetch_mode="http",
        attempts=attempts,
        elapsed_seconds=round(time.perf_counter() - start_time, 3),
        error="no_candidate_urls_built",
        scraped_at=datetime.now().isoformat(timespec="seconds"),
    )

def load_and_parse_cad_legal_acreage(driver: uc.Chrome, base_url: str, prop_id: str, max_attempts: int = MAX_ATTEMPTS_PER_PROPERTY) -> CadAcreageResult:
    input_id = clean_property_id(prop_id)
    if not input_id:
        return CadAcreageResult(input_id=input_id, status="failed", fetch_mode="browser", error="blank_property_id", scraped_at=datetime.now().isoformat(timespec="seconds"))

    start_time = time.perf_counter()
    last_error = ""
    last_url = ""
    last_raw = ""
    last_method = ""
    last_rows: List[LandRow] = []
    last_property_id_page = ""
    last_quick_ref_id_page = ""
    attempts = 0

    for attempt in range(1, max(1, max_attempts) + 1):
        attempts = attempt
        for candidate_id in build_candidate_id_values(input_id):
            url = build_property_url(base_url, candidate_id, TAX_YEAR)
            last_url = url
            try:
                driver.get(url)
                wait_for_property_page(driver)
                html_text = driver.page_source or ""

                acreage, raw, method, rows, status, property_id_page, quick_ref_or_error = parse_cad_legal_acreage_from_html(html_text)
                elapsed = time.perf_counter() - start_time
                last_raw = raw or last_raw
                last_method = method or last_method
                last_rows = list(rows) if rows else last_rows
                last_property_id_page = property_id_page or last_property_id_page

                if status == "success":
                    last_quick_ref_id_page = quick_ref_or_error or last_quick_ref_id_page
                    return CadAcreageResult(
                        input_id=input_id,
                        status="success",
                        url=url,
                        fetch_mode="browser",
                        acreage=acreage,
                        raw_acreage=raw,
                        method=method,
                        land_rows=list(rows),
                        attempts=attempts,
                        elapsed_seconds=round(elapsed, 3),
                        scraped_at=datetime.now().isoformat(timespec="seconds"),
                        property_id_page=property_id_page,
                        quick_ref_id_page=quick_ref_or_error,
                    )

                if status == "property_not_found":
                    last_error = "property_not_found"
                    # Try another id format before retrying the whole property.
                    continue

                if status == "blocked":
                    return CadAcreageResult(
                        input_id=input_id,
                        status="blocked",
                        url=url,
                        fetch_mode="browser",
                        raw_acreage=last_raw,
                        method=last_method,
                        land_rows=last_rows,
                        attempts=attempts,
                        elapsed_seconds=round(elapsed, 3),
                        error=quick_ref_or_error or "blocked_or_rate_limited",
                        scraped_at=datetime.now().isoformat(timespec="seconds"),
                        property_id_page=last_property_id_page,
                        quick_ref_id_page=last_quick_ref_id_page,
                    )

                last_error = quick_ref_or_error or status
                # For parse/table issues, retry can help if the page was partially loaded.

            except TimeoutException as exc:
                last_error = f"timeout: {str(exc)[:180]}"
            except WebDriverException as exc:
                last_error = f"webdriver_error: {str(exc)[:220]}"
            except Exception as exc:
                last_error = f"unexpected_error: {str(exc)[:220]}"

        if attempt < max_attempts:
            time.sleep(RETRY_DELAY_SECONDS * attempt)

    elapsed = time.perf_counter() - start_time
    status = "property_not_found" if last_error == "property_not_found" else "failed"
    if "cad_generic_error_page" in last_error or "something went wrong" in last_error.lower() or "try again later" in last_error.lower():
        status = "site_error"
    if last_error.startswith("property_land_table") or "land_table" in last_error or "confirmed_valid_property_page_without_land_table" in last_error:
        status = "no_land_table"
    if "no_numeric" in last_error or "zero_acreage" in last_error:
        status = "no_acreage_value"

    return CadAcreageResult(
        input_id=input_id,
        status=status,
        url=last_url,
        fetch_mode="browser",
        raw_acreage=last_raw,
        method=last_method,
        land_rows=last_rows,
        attempts=attempts,
        elapsed_seconds=round(elapsed, 3),
        error=last_error,
        scraped_at=datetime.now().isoformat(timespec="seconds"),
        property_id_page=last_property_id_page,
        quick_ref_id_page=last_quick_ref_id_page,
    )


class CountyLoadController:
    def __init__(self, county_name: str) -> None:
        self.county_name = county_name
        self.lock = threading.Lock()
        self.delay_multiplier = 1.0
        self.cooldown_until = 0.0
        self.recent_statuses: Deque[str] = deque(maxlen=max(5, ADAPTIVE_WINDOW_SIZE))

    def before_request(self, worker_id: int) -> None:
        with self.lock:
            cooldown_remaining = self.cooldown_until - time.time()
            multiplier = self.delay_multiplier
        if cooldown_remaining > 0:
            logging.warning("[%s] worker=%s cooling down %.1fs", self.county_name, worker_id, cooldown_remaining)
            time.sleep(cooldown_remaining)
        delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS) * multiplier
        if delay > 0:
            time.sleep(delay)

    def after_result(self, result: CadAcreageResult, worker_id: int) -> None:
        status = clean_text(result.status).lower()
        with self.lock:
            self.recent_statuses.append(status)
            if status == "blocked":
                self.delay_multiplier = min(ADAPTIVE_MAX_DELAY_MULTIPLIER, self.delay_multiplier * ADAPTIVE_BLOCK_INCREASE)
                self.cooldown_until = max(self.cooldown_until, time.time() + COOLDOWN_AFTER_BLOCK_SECONDS)
                logging.warning(
                    "[%s] blocked/rate-limited on worker=%s; delay multiplier now %.2f; cooldown %.0fs",
                    self.county_name,
                    worker_id,
                    self.delay_multiplier,
                    COOLDOWN_AFTER_BLOCK_SECONDS,
                )
                return

            if status_counts_as_request_ok(status):
                # Success and verified no-fill pages mean the server responded normally.
                self.delay_multiplier = max(1.0, self.delay_multiplier * ADAPTIVE_SUCCESS_DECAY)
            elif status_should_throttle(status):
                self.delay_multiplier = min(ADAPTIVE_MAX_DELAY_MULTIPLIER, self.delay_multiplier * ADAPTIVE_FAILURE_INCREASE)

            if len(self.recent_statuses) >= min(ADAPTIVE_WINDOW_SIZE, 8):
                bad = sum(1 for s in self.recent_statuses if status_should_throttle(s))
                rate = bad / len(self.recent_statuses)
                if rate >= ADAPTIVE_FAILURE_RATE_THRESHOLD:
                    self.delay_multiplier = min(ADAPTIVE_MAX_DELAY_MULTIPLIER, self.delay_multiplier * ADAPTIVE_FAILURE_INCREASE)
                    self.cooldown_until = max(self.cooldown_until, time.time() + ADAPTIVE_SOFT_COOLDOWN_SECONDS)
                    logging.warning(
                        "[%s] recent real site/transport failure rate %.0f%%; slowing county to multiplier %.2f for %.0fs",
                        self.county_name,
                        rate * 100,
                        self.delay_multiplier,
                        ADAPTIVE_SOFT_COOLDOWN_SECONDS,
                    )

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "delay_multiplier": round(self.delay_multiplier, 3),
                "cooldown_remaining": max(0.0, round(self.cooldown_until - time.time(), 1)),
                "recent_statuses": list(self.recent_statuses),
            }

def browser_worker(
    county_name: str,
    worker_id: int,
    base_url: str,
    items: List[Tuple[int, str]],
    controller: CountyLoadController,
    progress_callback=None,
) -> Dict[int, CadAcreageResult]:
    results: Dict[int, CadAcreageResult] = {}
    driver: Optional[uc.Chrome] = None
    browser_page_loads = 0
    rows_processed_by_worker = 0
    consecutive_failures = 0

    def restart_driver() -> None:
        nonlocal driver
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
        logging.info("[%s] [browser-%s] starting Chrome", county_name, worker_id)
        driver = create_driver()

    def ensure_driver() -> uc.Chrome:
        if driver is None:
            restart_driver()
        return driver  # type: ignore[return-value]

    try:
        if not USE_FAST_HTTP_FIRST:
            ensure_driver()

        for idx, prop_id in items:
            if driver is not None and browser_page_loads > 0 and browser_page_loads % RESTART_BROWSER_EVERY_N_PAGES == 0:
                logging.info("[%s] [browser-%s] scheduled Chrome restart after %s browser page loads", county_name, worker_id, browser_page_loads)
                restart_driver()

            controller.before_request(worker_id)

            if USE_FAST_HTTP_FIRST:
                result = load_and_parse_cad_legal_acreage_fast_http(base_url, prop_id)
                if result.status not in FAST_HTTP_ACCEPT_STATUSES and FALLBACK_TO_BROWSER_AFTER_HTTP_FAILURE:
                    # The fast path returned a transient error, blocked page, or unrecognized page.
                    # Try the real browser before marking the row as failed.
                    driver_obj = ensure_driver()
                    result = load_and_parse_cad_legal_acreage(driver_obj, base_url, prop_id)
            else:
                driver_obj = ensure_driver()
                result = load_and_parse_cad_legal_acreage(driver_obj, base_url, prop_id)

            rows_processed_by_worker += 1
            if result.fetch_mode == "browser":
                browser_page_loads += 1
            results[idx] = result
            controller.after_result(result, worker_id)

            if status_counts_as_request_ok(result.status):
                consecutive_failures = 0
            elif status_should_throttle(result.status):
                consecutive_failures += 1

            if progress_callback is not None:
                try:
                    progress_callback(idx, result, worker_id)
                except Exception as callback_exc:
                    logging.warning("[%s] [browser-%s] progress callback failed: %s", county_name, worker_id, callback_exc)

            if result.status == "blocked":
                logging.warning("[%s] [browser-%s] blocked. Restarting after cooldown.", county_name, worker_id)
                time.sleep(COOLDOWN_AFTER_BLOCK_SECONDS)
                restart_driver()
                consecutive_failures = 0

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES_PER_BROWSER:
                logging.warning(
                    "[%s] [browser-%s] %s consecutive real failures. Short cooldown and restart.",
                    county_name,
                    worker_id,
                    consecutive_failures,
                )
                time.sleep(min(COOLDOWN_AFTER_BLOCK_SECONDS, 45.0))
                if driver is not None:
                    restart_driver()
                consecutive_failures = 0

            if rows_processed_by_worker % LONG_PAUSE_EVERY_N_PAGES == 0:
                pause = random.uniform(LONG_PAUSE_MIN_SECONDS, LONG_PAUSE_MAX_SECONDS)
                logging.info("[%s] [browser-%s] long pause %.1fs after %s rows", county_name, worker_id, pause, rows_processed_by_worker)
                time.sleep(pause)

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    return results


# =============================================================================
# Capacity probe
# =============================================================================

def split_items(items: List[Tuple[int, str]], n: int) -> List[List[Tuple[int, str]]]:
    n = max(1, min(n, len(items))) if items else 1
    chunks = [[] for _ in range(n)]
    for i, item in enumerate(items):
        chunks[i % n].append(item)
    return [chunk for chunk in chunks if chunk]


def run_probe_level(county_name: str, base_url: str, items: List[Tuple[int, str]], worker_count: int) -> Dict[str, Any]:
    if not items:
        return {"worker_count": worker_count, "tested": 0, "success": 0, "failed": 0, "blocked": 0, "avg_seconds": 0.0}

    probe_items = items[: max(1, worker_count * CAPACITY_PROBE_PAGES_PER_WORKER)]
    chunks = split_items(probe_items, worker_count)
    controller = CountyLoadController(f"{county_name}-probe-{worker_count}")
    all_results: List[CadAcreageResult] = []
    start = time.perf_counter()

    logging.info("[%s] Capacity probe: testing %s worker(s) on %s page(s).", county_name, len(chunks), len(probe_items))

    if len(chunks) == 1:
        result_map = browser_worker(county_name, 1, base_url, chunks[0], controller)
        all_results.extend(result_map.values())
    else:
        with futures.ThreadPoolExecutor(max_workers=len(chunks), thread_name_prefix=f"{county_name}-probe") as pool:
            future_map = {
                pool.submit(browser_worker, county_name, i + 1, base_url, chunk, controller): i + 1
                for i, chunk in enumerate(chunks)
            }
            for fut in futures.as_completed(future_map):
                try:
                    all_results.extend(fut.result().values())
                except Exception as exc:
                    logging.warning("[%s] Capacity probe worker failed: %s", county_name, exc)

    elapsed = time.perf_counter() - start
    tested = len(all_results)
    success = sum(1 for r in all_results if r.status == "success")
    request_ok = sum(1 for r in all_results if status_counts_as_request_ok(r.status))
    blocked = sum(1 for r in all_results if r.status == "blocked")
    site_error = sum(1 for r in all_results if r.status == "site_error")
    failed = tested - request_ok
    avg_seconds = mean([r.elapsed_seconds for r in all_results if r.elapsed_seconds]) if all_results else elapsed
    summary = {
        "worker_count": len(chunks),
        "tested": tested,
        "success": success,
        "request_ok": request_ok,
        "failed": failed,
        "blocked": blocked,
        "site_error": site_error,
        "failure_rate": (failed / tested) if tested else 1.0,
        "avg_seconds": round(avg_seconds, 3),
        "elapsed_seconds": round(elapsed, 3),
    }
    logging.info(
        "[%s] Capacity probe result: workers=%s tested=%s request_ok=%s success=%s failed=%s blocked=%s site_error=%s avg=%.2fs failure_rate=%.0f%%",
        county_name,
        summary["worker_count"],
        tested,
        request_ok,
        success,
        failed,
        blocked,
        site_error,
        avg_seconds,
        summary["failure_rate"] * 100,
    )
    return summary


def estimate_county_worker_count(county_name: str, base_url: str, items: List[Tuple[int, str]]) -> Tuple[int, List[Dict[str, Any]]]:
    max_workers = max(1, min(int(CAPACITY_PROBE_MAX_WORKERS), int(MAX_BROWSER_WORKERS_PER_COUNTY), len(items) if items else 1))
    min_workers = max(1, min(int(CAPACITY_PROBE_MIN_WORKERS), max_workers))
    if not ENABLE_CAPACITY_PROBE or not items or max_workers <= min_workers:
        chosen = max(1, min(max_workers, len(items) if items else 1))
        logging.info("[%s] Capacity probe skipped. Using %s browser worker(s).", county_name, chosen)
        return chosen, []

    probe_summaries: List[Dict[str, Any]] = []
    best_workers = min_workers
    for worker_count in range(min_workers, max_workers + 1):
        summary = run_probe_level(county_name, base_url, items, worker_count)
        probe_summaries.append(summary)

        blocked_bad = summary["blocked"] > CAPACITY_PROBE_MAX_BLOCKED
        failure_bad = summary["failure_rate"] > CAPACITY_PROBE_MAX_FAILURE_RATE
        speed_bad = summary["avg_seconds"] > CAPACITY_PROBE_MAX_AVG_SECONDS

        if blocked_bad or failure_bad or speed_bad:
            logging.warning(
                "[%s] Capacity probe stopped at workers=%s (blocked_bad=%s, failure_bad=%s, speed_bad=%s). Using %s worker(s).",
                county_name,
                worker_count,
                blocked_bad,
                failure_bad,
                speed_bad,
                best_workers,
            )
            break

        best_workers = worker_count
        if worker_count < max_workers and CAPACITY_PROBE_COOLDOWN_BETWEEN_LEVELS_SECONDS > 0:
            time.sleep(CAPACITY_PROBE_COOLDOWN_BETWEEN_LEVELS_SECONDS)

    chosen = max(1, min(best_workers, len(items)))
    logging.info("[%s] Capacity decision: using %s browser worker(s).", county_name, chosen)
    return chosen, probe_summaries


# =============================================================================
# Processing
# =============================================================================

def result_to_trace_values(result: CadAcreageResult, worker_id: int) -> Dict[str, Any]:
    attempts = parse_acreage_value_to_float(result.attempts)
    elapsed = parse_acreage_value_to_float(result.elapsed_seconds)
    worker = parse_acreage_value_to_float(worker_id)

    return {
        "cad_legal_acreage_status": clean_text(result.status),
        "cad_legal_acreage_source": f"cad_property_land_table_{clean_text(result.fetch_mode) or 'unknown'}" if result.status == "success" else "",
        "cad_legal_acreage_method": clean_text(result.method),
        "cad_legal_acreage_raw": clean_text(result.raw_acreage),
        "cad_legal_acreage_url": clean_text(result.url),
        "cad_legal_acreage_error": clean_text(result.error),
        "cad_legal_acreage_attempts": attempts if attempts is not None else "",
        "cad_legal_acreage_elapsed_seconds": elapsed if elapsed is not None else "",
        "cad_legal_acreage_land_rows_json": land_rows_to_json(result.land_rows) if result.land_rows else "",
        "cad_legal_acreage_scraped_at": clean_text(result.scraped_at),
        "cad_legal_acreage_worker": worker if worker is not None else "",
        "cad_legal_acreage_fetch_mode": clean_text(result.fetch_mode),
        "cad_legal_acreage_parser_version": SCRIPT_VERSION,
    }


def row_needs_scrape(df: pd.DataFrame, idx: Any, prop_col: str, legal_col: str) -> bool:
    prop_id = clean_property_id(df.at[idx, prop_col])
    if not prop_id:
        return False
    if not legal_acreage_is_missing(df.at[idx, legal_col]):
        return False

    status = clean_text(df.at[idx, "cad_legal_acreage_status"]).lower() if "cad_legal_acreage_status" in df.columns else ""
    parser_version = clean_text(df.at[idx, "cad_legal_acreage_parser_version"]) if "cad_legal_acreage_parser_version" in df.columns else ""

    if not status:
        return True

    # If legal_acreage is still blank even though the row says success, retry;
    # this protects against interrupted/failed live-save callbacks from older runs.
    if status == "success":
        return True

    # Terminal no-fill statuses from this parser are trusted on resume. Older
    # no_land_table statuses are retried because the previous version could
    # misclassify a temporary CAD error page as no_land_table.
    if status_is_terminal_no_fill(status):
        if parser_version == SCRIPT_VERSION:
            return RESCRAPE_CURRENT_TERMINAL_NO_FILL_ROWS
        return RETRY_STALE_TERMINAL_NO_FILL_ROWS_ON_RESUME

    # Failed/blocked/site_error rows are retryable by default.
    if status_should_throttle(status):
        return RESCRAPE_FAILED_ROWS

    return RESCRAPE_FAILED_ROWS


def apply_result_to_dataframe(df: pd.DataFrame, idx: Any, legal_col: str, result: CadAcreageResult, worker_id: int) -> bool:
    filled = False

    # Make sure columns are mutable even when resuming from files that pandas
    # loaded as strict string dtype. This keeps the live progress callback from
    # failing on float/int assignments.
    make_dataframe_columns_mutable(df, [legal_col, *TRACE_COLUMN_DEFAULTS.keys()])

    if result.status == "success" and result.acreage is not None:
        acreage_float = parse_acreage_value_to_float(result.acreage)
        if acreage_float is not None:
            df.at[idx, legal_col] = acreage_float
            filled = True
    for col, value in result_to_trace_values(result, worker_id).items():
        df.at[idx, col] = value
    return filled


def process_file(
    file_path: Path,
    county_folder: Path,
    base_url: str,
    website_lookup: WebsiteLinkLookup,
    sheet_row: CountySheetRow,
    output_root: Optional[Path],
    scrape_events: List[Dict[str, Any]],
    probe_reports: List[Dict[str, Any]],
) -> FileSummary:
    start_time = time.perf_counter()
    county_name = county_folder.name
    out_path = output_path_for(file_path, county_folder, output_root)

    if out_path.exists() and not OVERWRITE_OUTPUT:
        logging.info("[%s] Resume/output exists: %s", county_name, out_path)
        df = read_input_file(out_path)
    else:
        logging.info("[%s] Reading input file: %s", county_name, file_path)
        df = read_input_file(file_path)

    df.columns = [str(c).strip() for c in df.columns]
    prop_col = find_property_id_column(df)
    legal_col = find_legal_acreage_column(df)
    if not prop_col:
        raise KeyError(f"No property id column found in {file_path.name}")
    if not legal_col:
        raise KeyError(f"No legal_acreage column found in {file_path.name}")

    ensure_trace_columns(df)
    prepare_dataframe_for_live_updates(df, legal_col)

    total_rows = len(df)
    missing_before = int(sum(1 for idx in df.index if legal_acreage_is_missing(df.at[idx, legal_col])))
    already_completed_rows = int((df["cad_legal_acreage_status"].fillna("").astype(str).str.lower() == "success").sum()) if "cad_legal_acreage_status" in df.columns else 0

    if LIVE_SAVE_ENABLED:
        write_output_file(df, out_path)
        logging.info("[%s] Live output initialized: %s", county_name, out_path)

    items = [(idx, clean_property_id(df.at[idx, prop_col])) for idx in df.index if row_needs_scrape(df, idx, prop_col, legal_col)]
    pending_total = len(items)

    if pending_total == 0:
        missing_after = int(sum(1 for idx in df.index if legal_acreage_is_missing(df.at[idx, legal_col])))
        write_output_file(df, out_path)
        logging.info("[%s] No pending missing legal_acreage rows in %s. missing_after=%s", county_name, file_path.name, missing_after)
        return FileSummary(
            county=county_name,
            input_file=str(file_path),
            output_file=str(out_path),
            status="no_pending_rows",
            total_rows=total_rows,
            missing_before=missing_before,
            targeted_rows=0,
            already_completed_rows=already_completed_rows,
            missing_after=missing_after,
            elapsed_seconds=round(time.perf_counter() - start_time, 3),
        )

    worker_count, probe_summaries = estimate_county_worker_count(county_name, base_url, items)
    for p in probe_summaries:
        p = dict(p)
        p.update({"county": county_name, "file": file_path.name})
        probe_reports.append(p)

    chunks = split_items(items, worker_count)
    controller = CountyLoadController(county_name)
    progress_lock = threading.Lock()
    processed_counter = {
        "done": 0,
        "filled": 0,
        "success_no_fill": 0,
        "failed": 0,
        "blocked": 0,
        "property_not_found": 0,
        "no_land_table": 0,
        "no_acreage_value": 0,
        "site_error": 0,
        "unprocessed": 0,
        "last_save_done": 0,
        "last_sheet_done": 0,
    }
    live_save_every = max(1, int(LIVE_SAVE_EVERY_N_ROWS))
    terminal_every = max(1, int(TERMINAL_PROGRESS_EVERY_N_ROWS))
    sheet_every = max(1, int(SHEET_UPDATE_EVERY_N_ROWS))
    scrape_start_time = time.perf_counter()
    processed_indices: set = set()

    def progress_callback(idx: Any, result: CadAcreageResult, worker_id: int) -> None:
        with progress_lock:
            filled = apply_result_to_dataframe(df, idx, legal_col, result, worker_id)
            processed_indices.add(idx)
            processed_counter["done"] += 1
            if filled:
                processed_counter["filled"] += 1
            elif result.status == "success":
                processed_counter["success_no_fill"] += 1
            elif result.status == "blocked":
                processed_counter["blocked"] += 1
            elif result.status == "property_not_found":
                processed_counter["property_not_found"] += 1
            elif result.status == "no_land_table":
                processed_counter["no_land_table"] += 1
            elif result.status == "no_acreage_value":
                processed_counter["no_acreage_value"] += 1
            elif result.status == "site_error":
                processed_counter["site_error"] += 1
            else:
                processed_counter["failed"] += 1

            scrape_events.append(
                {
                    "county": county_name,
                    "file": file_path.name,
                    "row_index": idx,
                    "property_id_input": result.input_id,
                    "status": result.status,
                    "acreage": result.acreage,
                    "raw_acreage": result.raw_acreage,
                    "method": result.method,
                    "fetch_mode": result.fetch_mode,
                    "url": result.url,
                    "error": result.error,
                    "attempts": result.attempts,
                    "elapsed_seconds": result.elapsed_seconds,
                    "worker_id": worker_id,
                    "scraped_at": result.scraped_at,
                }
            )

            done = processed_counter["done"]
            should_save = LIVE_SAVE_ENABLED and (done - processed_counter["last_save_done"] >= live_save_every or done == pending_total)
            if should_save:
                write_output_file(df, out_path)
                processed_counter["last_save_done"] = done

            pct_done = (done / pending_total * 100.0) if pending_total else 100.0
            elapsed_scrape = max(0.001, time.perf_counter() - scrape_start_time)
            rows_per_minute = done / elapsed_scrape * 60.0
            eta_seconds = ((pending_total - done) / (rows_per_minute / 60.0)) if rows_per_minute > 0 else None
            controller_state = controller.snapshot()
            if done % terminal_every == 0 or done == pending_total:
                logging.info(
                    "[%s] %s | %s/%s rows (%.1f%%) | filled=%s no_land=%s no_acres=%s not_found=%s site_err=%s failed=%s blocked=%s | rate=%.1f rows/min ETA=%s delay=%.2fx | worker=%s fetch=%s id=%s status=%s acres=%s saved=%s",
                    county_name,
                    file_path.name,
                    done,
                    pending_total,
                    pct_done,
                    processed_counter["filled"],
                    processed_counter["no_land_table"],
                    processed_counter["no_acreage_value"],
                    processed_counter["property_not_found"],
                    processed_counter["site_error"],
                    processed_counter["failed"],
                    processed_counter["blocked"],
                    rows_per_minute,
                    format_duration(eta_seconds),
                    controller_state["delay_multiplier"],
                    worker_id,
                    result.fetch_mode,
                    result.input_id,
                    result.status,
                    result.acreage if result.acreage is not None else "",
                    "yes" if should_save else "no",
                )

            should_update_sheet = done - processed_counter["last_sheet_done"] >= sheet_every or done == pending_total
            if should_update_sheet:
                website_lookup.update_status(
                    sheet_row,
                    f"CAD acreage scraping {file_path.name}: {done}/{pending_total} | filled={processed_counter['filled']} | failed={processed_counter['failed']} | site_err={processed_counter['site_error']} | blocked={processed_counter['blocked']}",
                )
                processed_counter["last_sheet_done"] = done

    logging.info("[%s] Scraping %s missing rows from %s with %s browser worker(s).", county_name, pending_total, file_path.name, len(chunks))

    if len(chunks) == 1:
        browser_worker(county_name, 1, base_url, chunks[0], controller, progress_callback=progress_callback)
    else:
        with futures.ThreadPoolExecutor(max_workers=len(chunks), thread_name_prefix=f"{county_name}-cad") as pool:
            future_map = {
                pool.submit(browser_worker, county_name, i + 1, base_url, chunk, controller, progress_callback): i + 1
                for i, chunk in enumerate(chunks)
            }
            for fut in futures.as_completed(future_map):
                worker_id = future_map[fut]
                try:
                    fut.result()
                    logging.info("[%s] Browser worker %s finished.", county_name, worker_id)
                except Exception as exc:
                    logging.exception("[%s] Browser worker %s crashed: %s", county_name, worker_id, exc)

    remaining_items = [item for item in items if item[0] not in processed_indices]
    if remaining_items and RETRY_UNPROCESSED_ROWS_AFTER_WORKER_CRASH:
        logging.warning(
            "[%s] %s row(s) did not reach the progress callback. Retrying once with one recovery worker before finalizing.",
            county_name,
            len(remaining_items),
        )
        try:
            browser_worker(county_name, 999, base_url, remaining_items, controller, progress_callback=progress_callback)
        except Exception as exc:
            logging.exception("[%s] Recovery worker crashed: %s", county_name, exc)

    remaining_items = [item for item in items if item[0] not in processed_indices]
    processed_counter["unprocessed"] = len(remaining_items)
    if remaining_items:
        logging.warning(
            "[%s] File is incomplete: %s/%s targeted rows processed; %s rows still unprocessed.",
            county_name,
            processed_counter["done"],
            pending_total,
            len(remaining_items),
        )

    write_output_file(df, out_path)
    missing_after = int(sum(1 for idx in df.index if legal_acreage_is_missing(df.at[idx, legal_col])))
    elapsed = round(time.perf_counter() - start_time, 3)
    file_status = "partial_incomplete" if processed_counter["unprocessed"] else "ok"

    summary = FileSummary(
        county=county_name,
        input_file=str(file_path),
        output_file=str(out_path),
        status=file_status,
        total_rows=total_rows,
        missing_before=missing_before,
        targeted_rows=pending_total,
        already_completed_rows=already_completed_rows,
        filled_from_cad=processed_counter["filled"],
        success_no_fill=processed_counter["success_no_fill"],
        failed=processed_counter["failed"],
        blocked=processed_counter["blocked"],
        property_not_found=processed_counter["property_not_found"],
        no_land_table=processed_counter["no_land_table"],
        no_acreage_value=processed_counter["no_acreage_value"],
        site_error=processed_counter["site_error"],
        unprocessed=processed_counter["unprocessed"],
        missing_after=missing_after,
        worker_count=len(chunks),
        elapsed_seconds=elapsed,
    )
    logging.info("[%s] Final file saved: %s | status=%s | filled=%s | missing_after=%s | unprocessed=%s | elapsed=%.1fs", county_name, out_path, summary.status, summary.filled_from_cad, missing_after, summary.unprocessed, elapsed)
    website_lookup.update_status(sheet_row, f"CAD acreage {summary.status} {file_path.name}: filled={summary.filled_from_cad}/{pending_total} | site_err={summary.site_error} | unprocessed={summary.unprocessed} | missing_after={missing_after}")
    return summary


def process_county(
    county_folder: Path,
    website_lookup: WebsiteLinkLookup,
    output_root: Optional[Path],
    scrape_events: List[Dict[str, Any]],
    probe_reports: List[Dict[str, Any]],
) -> List[FileSummary]:
    county_name = county_folder.name
    row = website_lookup.get_county_row(county_name)
    if row is None:
        msg = "No eligible sheet/manual website link found for county."
        logging.warning("[%s] %s", county_name, msg)
        return [FileSummary(county=county_name, input_file="", output_file="", status="skipped_no_website_link", error=msg)]

    override_base = COUNTY_ESEARCH_BASE_URL_OVERRIDES.get(county_name) or COUNTY_ESEARCH_BASE_URL_OVERRIDES.get(normalize_county_name(county_name))
    try:
        base_url = clean_text(override_base) if override_base else derive_esearch_base_url(row.appraisal_website)
        logging.info("[%s] Base URL: %s", county_name, base_url)
    except Exception as exc:
        msg = f"Could not derive eSearch URL: {exc}"
        logging.error("[%s] %s", county_name, msg)
        website_lookup.update_status(row, "CAD acreage failed: could not derive eSearch URL")
        return [FileSummary(county=county_name, input_file="", output_file="", status="failed_base_url", error=msg)]

    files = discover_county_files(county_folder)
    if not files:
        msg = "No supported CSV/XLSX files found."
        logging.warning("[%s] %s", county_name, msg)
        return [FileSummary(county=county_name, input_file="", output_file="", status="skipped_no_files", error=msg)]

    summaries: List[FileSummary] = []
    for file_path in files:
        try:
            summary = process_file(file_path, county_folder, base_url, website_lookup, row, output_root, scrape_events, probe_reports)
            summaries.append(summary)
        except Exception as exc:
            logging.exception("[%s] File failed: %s", county_name, file_path.name)
            summaries.append(
                FileSummary(
                    county=county_name,
                    input_file=str(file_path),
                    output_file="",
                    status="failed_exception",
                    error=f"{type(exc).__name__}: {str(exc)[:250]}",
                )
            )
            website_lookup.update_status(row, f"CAD acreage file failed: {file_path.name} | {str(exc)[:120]}")
    return summaries


def discover_county_folders(input_root: Path, website_lookup: WebsiteLinkLookup) -> List[Path]:
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(f"INPUT_ROOT does not exist or is not a directory: {input_root}")

    allowed = {normalize_county_name(c) for c in COUNTIES_TO_RUN} if COUNTIES_TO_RUN else None
    folders: List[Path] = []
    for folder in sorted(input_root.iterdir()):
        if not folder.is_dir():
            continue
        norm = normalize_county_name(folder.name)
        if allowed and norm not in allowed:
            continue
        folders.append(folder)
    return folders


# =============================================================================
# Reports
# =============================================================================

def file_summary_to_dict(summary: FileSummary) -> Dict[str, Any]:
    return {
        "county": summary.county,
        "status": summary.status,
        "input_file": summary.input_file,
        "output_file": summary.output_file,
        "total_rows": summary.total_rows,
        "missing_legal_acreage_before": summary.missing_before,
        "rows_targeted_for_cad_scrape": summary.targeted_rows,
        "already_completed_rows": summary.already_completed_rows,
        "filled_from_cad_property_land": summary.filled_from_cad,
        "success_no_fill": summary.success_no_fill,
        "failed": summary.failed,
        "blocked": summary.blocked,
        "property_not_found": summary.property_not_found,
        "no_land_table": summary.no_land_table,
        "no_acreage_value": summary.no_acreage_value,
        "site_error": summary.site_error,
        "unprocessed": summary.unprocessed,
        "missing_legal_acreage_after": summary.missing_after,
        "worker_count_used": summary.worker_count,
        "elapsed_seconds": summary.elapsed_seconds,
        "error": summary.error,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def write_summary_csv(summaries: Sequence[FileSummary], output_root: Path) -> Path:
    path = output_root / f"cad_legal_acreage_scrape_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    pd.DataFrame([file_summary_to_dict(s) for s in summaries]).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def write_events_csv(events: Sequence[Dict[str, Any]], output_root: Path) -> Optional[Path]:
    if not WRITE_DETAILED_SCRAPE_EVENTS_CSV:
        return None
    path = output_root / f"cad_legal_acreage_scrape_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    pd.DataFrame(list(events)).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def write_probe_csv(probe_reports: Sequence[Dict[str, Any]], output_root: Path) -> Optional[Path]:
    if not probe_reports:
        return None
    path = output_root / f"cad_legal_acreage_capacity_probe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    pd.DataFrame(list(probe_reports)).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def write_html_report(
    summaries: Sequence[FileSummary],
    summary_csv: Path,
    events_csv: Optional[Path],
    probe_csv: Optional[Path],
    output_root: Path,
) -> Optional[Path]:
    if not WRITE_HTML_REPORT:
        return None

    total_files = len(summaries)
    ok_files = sum(1 for s in summaries if s.status in {"ok", "no_pending_rows"})
    total_targeted = sum(s.targeted_rows for s in summaries)
    total_filled = sum(s.filled_from_cad for s in summaries)
    total_missing_after = sum(s.missing_after for s in summaries if s.status in {"ok", "no_pending_rows", "partial_incomplete"})
    total_blocked = sum(s.blocked for s in summaries)
    total_site_error = sum(s.site_error for s in summaries)
    total_unprocessed = sum(s.unprocessed for s in summaries)
    total_failed = sum(s.failed for s in summaries)

    rows_html = []
    for s in summaries:
        rows_html.append(
            "<tr>"
            f"<td>{html_escape(s.county)}</td>"
            f"<td>{html_escape(s.status)}</td>"
            f"<td>{html_escape(Path(s.input_file).name if s.input_file else '')}</td>"
            f"<td class='num'>{s.total_rows:,}</td>"
            f"<td class='num'>{s.missing_before:,}</td>"
            f"<td class='num'>{s.targeted_rows:,}</td>"
            f"<td class='num'>{s.filled_from_cad:,}</td>"
            f"<td class='num'>{s.missing_after:,}</td>"
            f"<td class='num'>{s.worker_count}</td>"
            f"<td class='num'>{s.blocked:,}</td>"
            f"<td class='num'>{s.site_error:,}</td>"
            f"<td class='num'>{s.failed:,}</td>"
            f"<td class='num'>{s.unprocessed:,}</td>"
            f"<td>{html_escape(s.error)}</td>"
            "</tr>"
        )

    html_text = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>CAD Legal Acreage Scrape Report</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
h1, h2 {{ color: #17365d; }}
.cards {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 16px 0 24px; }}
.card {{ border: 1px solid #ddd; border-radius: 8px; padding: 14px 16px; min-width: 180px; background: #fafafa; }}
.card .label {{ font-size: 12px; color: #666; text-transform: uppercase; }}
.card .value {{ font-size: 24px; font-weight: bold; margin-top: 4px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
th {{ background: #eef3f7; text-align: left; }}
td.num, th.num {{ text-align: right; }}
.note {{ background: #fff8dc; border-left: 4px solid #f0c36d; padding: 12px; margin: 16px 0; }}
.ok {{ color: #2f6b2f; font-weight: bold; }}
.warn {{ color: #9a6700; font-weight: bold; }}
</style>
</head>
<body>
<h1>CAD Legal Acreage Scrape Report</h1>
<p>Generated at {html_escape(datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC'))}</p>

<div class="note">
<strong>What this step fills:</strong> only rows where <code>legal_acreage</code> was still blank when this script ran.
The value comes from the CAD property page's <strong>Property Land &rarr; Acreage</strong> table. If a parcel has multiple land rows, this script uses <code>{html_escape(LAND_ACREAGE_AGGREGATION)}</code> aggregation.
</div>

<div class="cards">
  <div class="card"><div class="label">Files processed</div><div class="value">{total_files:,}</div></div>
  <div class="card"><div class="label">Files OK</div><div class="value">{ok_files:,}</div></div>
  <div class="card"><div class="label">Rows targeted</div><div class="value">{total_targeted:,}</div></div>
  <div class="card"><div class="label">Filled from CAD</div><div class="value ok">{total_filled:,}</div></div>
  <div class="card"><div class="label">Missing after</div><div class="value warn">{total_missing_after:,}</div></div>
  <div class="card"><div class="label">Blocked rows</div><div class="value warn">{total_blocked:,}</div></div>
  <div class="card"><div class="label">Site error rows</div><div class="value warn">{total_site_error:,}</div></div>
  <div class="card"><div class="label">Unprocessed rows</div><div class="value warn">{total_unprocessed:,}</div></div>
  <div class="card"><div class="label">Failed rows</div><div class="value warn">{total_failed:,}</div></div>
</div>

<h2>Generated files</h2>
<ul>
  <li>Summary CSV: <code>{html_escape(str(summary_csv))}</code></li>
  <li>Detailed events CSV: <code>{html_escape(str(events_csv) if events_csv else 'not generated')}</code></li>
  <li>Capacity probe CSV: <code>{html_escape(str(probe_csv) if probe_csv else 'not generated')}</code></li>
</ul>

<h2>County/File Results</h2>
<table>
<thead>
<tr>
<th>County</th><th>Status</th><th>File</th><th class="num">Total Rows</th><th class="num">Missing Before</th><th class="num">Targeted</th><th class="num">Filled</th><th class="num">Missing After</th><th class="num">Workers</th><th class="num">Blocked</th><th class="num">Site Errors</th><th class="num">Failed</th><th class="num">Unprocessed</th><th>Error</th>
</tr>
</thead>
<tbody>
{''.join(rows_html)}
</tbody>
</table>
</body>
</html>
"""

    path = output_root / f"cad_legal_acreage_scrape_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    path.write_text(html_text, encoding="utf-8")
    return path


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    logging.info("Starting CAD legal acreage scraper.")
    logging.info("INPUT_ROOT=%s", INPUT_ROOT)
    logging.info("OUTPUT_ROOT=%s | IN_PLACE_UPDATE=%s", OUTPUT_ROOT, IN_PLACE_UPDATE)
    logging.info("HEADLESS=%s | MAX_COUNTY_WORKERS=%s | MAX_BROWSER_WORKERS_PER_COUNTY=%s", HEADLESS, MAX_COUNTY_WORKERS, MAX_BROWSER_WORKERS_PER_COUNTY)
    logging.info("FAST_HTTP_FIRST=%s | FALLBACK_TO_BROWSER=%s | delay=%.2f-%.2fs | adaptive_max=%.2fx", USE_FAST_HTTP_FIRST, FALLBACK_TO_BROWSER_AFTER_HTTP_FAILURE, MIN_DELAY_SECONDS, MAX_DELAY_SECONDS, ADAPTIVE_MAX_DELAY_MULTIPLIER)

    maybe_cleanup_uc_cache()

    input_root = Path(INPUT_ROOT)
    if IN_PLACE_UPDATE:
        output_root = input_root
    else:
        output_root = Path(OUTPUT_ROOT) if OUTPUT_ROOT else input_root.with_name(input_root.name + "_cad_legal_acreage_filled")
        output_root.mkdir(parents=True, exist_ok=True)

    website_lookup = WebsiteLinkLookup()
    county_folders = discover_county_folders(input_root, website_lookup)
    if not county_folders:
        logging.warning("No county folders found to process.")
        return 0

    logging.info("County folders selected: %s", ", ".join(folder.name for folder in county_folders))

    all_summaries: List[FileSummary] = []
    scrape_events: List[Dict[str, Any]] = []
    probe_reports: List[Dict[str, Any]] = []
    shared_events_lock = threading.Lock()
    shared_probe_lock = threading.Lock()

    # Wrap shared lists so county workers do not write to them concurrently without a lock.
    def process_county_threadsafe(folder: Path) -> List[FileSummary]:
        local_events: List[Dict[str, Any]] = []
        local_probes: List[Dict[str, Any]] = []
        summaries = process_county(folder, website_lookup, output_root, local_events, local_probes)
        with shared_events_lock:
            scrape_events.extend(local_events)
        with shared_probe_lock:
            probe_reports.extend(local_probes)
        return summaries

    max_counties = max(1, min(int(MAX_COUNTY_WORKERS), len(county_folders)))
    if max_counties == 1:
        for folder in county_folders:
            all_summaries.extend(process_county_threadsafe(folder))
    else:
        with futures.ThreadPoolExecutor(max_workers=max_counties, thread_name_prefix="county") as pool:
            future_map = {pool.submit(process_county_threadsafe, folder): folder.name for folder in county_folders}
            for fut in futures.as_completed(future_map):
                county_name = future_map[fut]
                try:
                    all_summaries.extend(fut.result())
                    logging.info("[%s] County finished.", county_name)
                except Exception as exc:
                    logging.exception("[%s] County crashed: %s", county_name, exc)
                    all_summaries.append(FileSummary(county=county_name, input_file="", output_file="", status="failed_county_exception", error=str(exc)[:250]))

    summary_csv = write_summary_csv(all_summaries, output_root)
    events_csv = write_events_csv(scrape_events, output_root)
    probe_csv = write_probe_csv(probe_reports, output_root)
    html_report = write_html_report(all_summaries, summary_csv, events_csv, probe_csv, output_root)

    total_targeted = sum(s.targeted_rows for s in all_summaries)
    total_filled = sum(s.filled_from_cad for s in all_summaries)
    total_missing_after = sum(s.missing_after for s in all_summaries if s.status in {"ok", "no_pending_rows", "partial_incomplete"})

    logging.info("Finished CAD legal acreage scraper.")
    logging.info("Rows targeted: %s | filled from CAD: %s | missing after: %s", total_targeted, total_filled, total_missing_after)
    logging.info("Summary CSV: %s", summary_csv)
    if events_csv:
        logging.info("Events CSV: %s", events_csv)
    if probe_csv:
        logging.info("Capacity probe CSV: %s", probe_csv)
    if html_report:
        logging.info("HTML report: %s", html_report)

    print("\n" + "=" * 88)
    print("CAD LEGAL ACREAGE SCRAPER COMPLETE")
    print(f"Rows targeted: {total_targeted:,}")
    print(f"Filled from CAD Property Land table: {total_filled:,}")
    print(f"Still missing after this step: {total_missing_after:,}")
    print(f"Summary CSV: {summary_csv}")
    if events_csv:
        print(f"Events CSV: {events_csv}")
    if probe_csv:
        print(f"Capacity probe CSV: {probe_csv}")
    if html_report:
        print(f"HTML report: {html_report}")
    print("=" * 88)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception as exc:
        logging.error("Fatal error: %s", exc)
        logging.debug(traceback.format_exc())
        raise
