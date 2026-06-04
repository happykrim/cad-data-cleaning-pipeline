#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Pipeline utility: subtract parcels that already exist in Supabase.

Recommended script name:
    8_subtract_existing_supabase_parcels_no_arg.py

Fix note:
- This version fixes a pandas StringDtype assignment error that occurred when
  partially filtering a row and writing an integer lands_count/owned_lands value
  back into a string-typed row.

Purpose:
- Iterates county folders under INPUT_FOLDER.
- Connects to Supabase.
- For each county, fetches existing parcel IDs from `unique_owner_non_enriched` using:
      land_id
      id_list
- Reads the local CSV/XLSX pipeline files and uses the local `id` field plus `id_list`
  to identify the parcel IDs represented by each row.
- Removes parcels that already exist in Supabase for the same county.
- If a row contains multiple parcels and only some already exist, the script keeps the row
  but filters the aligned list fields so only new parcel IDs remain.
- Saves the remaining rows into a new output folder while mirroring the county-folder structure.
- Generates dated HTML/CSV reports.

Important matching rule:
- Supabase `unique_id` is NOT used for matching.
- Local file `id` is treated as the parcel/land ID.
- Supabase `land_id` and Supabase `id_list` are both treated as existing parcel identifiers.

Dependencies:
    pip install pandas python-dotenv supabase openpyxl

Environment:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY

Optional environment overrides:
    INPUT_FOLDER
    OUTPUT_FOLDER
    SUBTRACT_EXISTING_INPUT_FOLDER
    SUBTRACT_EXISTING_OUTPUT_FOLDER
    REPORT_FOLDER
    SUBTRACT_EXISTING_REPORT_FOLDER
    DRY_RUN=true/false
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html import escape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import ast
import json
import math
import os
import re
import time
import traceback

import pandas as pd
from dotenv import find_dotenv, load_dotenv

try:
    from supabase import Client, create_client
except Exception:  # pragma: no cover - allows syntax checks where supabase is not installed.
    Client = Any  # type: ignore[misc,assignment]
    create_client = None  # type: ignore[assignment]


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# Folder containing county folders. Each county folder should contain the CSV/XLSX
# files produced by the latest local pipeline step.
DEFAULT_INPUT_FOLDER = Path(
    r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_8_addresses_enriched"
)

INPUT_FOLDER = Path(
    os.environ.get("SUBTRACT_EXISTING_INPUT_FOLDER")
    or os.environ.get("INPUT_FOLDER")
    or DEFAULT_INPUT_FOLDER
)

# New output step. The script mirrors the county-folder structure inside this folder.
DEFAULT_OUTPUT_FOLDER = INPUT_FOLDER.parent / "step_9_subtracted_existing_supabase_parcels"
OUTPUT_FOLDER = Path(
    os.environ.get("SUBTRACT_EXISTING_OUTPUT_FOLDER")
    or os.environ.get("OUTPUT_FOLDER")
    or DEFAULT_OUTPUT_FOLDER
)

COUNTY_TABLE = "county"
EXISTING_OWNER_TABLE = "unique_owner_non_enriched"

# Local source columns.
LOCAL_ID_COLUMN = "id"
LOCAL_ID_LIST_COLUMN = "id_list"
LOCAL_LANDS_COUNT_COLUMNS = ["lands_count", "owned_lands"]
LOCAL_ACRES_COLUMN = "acres"
LOCAL_FULL_NAME_COLUMN = "full_name"

# These columns are aligned with id_list. When a row is partially kept, these are
# filtered using the same positions as the remaining parcel IDs.
ALIGNED_LIST_COLUMNS = [
    "id_list",
    "acres_list",
    "market_value_list",
    "owner_tax_year_list",
    "deed_date_list",
    "legal_acreage_filled_by_script",
    "Empty_Legal_Acreage",
]

# If True, output list fields are written as JSON arrays: ["10522", "10821"].
# This matches the current pipeline list style and is safer in CSV than pipe strings.
WRITE_LISTS_AS_JSON = True

# If True, when a row with multiple parcel IDs is partially kept, `id` is changed
# to the first remaining parcel ID. This is useful because later DB insert scripts
# usually map local `id` -> Supabase `land_id`.
UPDATE_ROW_ID_TO_FIRST_REMAINING_ID = True

# If True, `acres` is recomputed as the sum of the remaining acres_list values
# whenever acres_list contains at least one numeric value.
RECOMPUTE_ACRES_FOR_PARTIAL_ROWS = True
ACRES_ROUND_DECIMALS = 6

# Output behavior.
OUTPUT_FILE_SUFFIX = "_subtracted_existing_parcels"
OUTPUT_SUFFIX = ".csv"
WRITE_ROOT_SUMMARY_CSV = True
SUMMARY_CSV_NAME = "subtract_existing_supabase_parcels_summary.csv"

# Dry run only reads files/DB and writes reports. It does not write cleaned output CSVs.
DRY_RUN_DEFAULT = False
DRY_RUN = str(os.environ.get("DRY_RUN", str(DRY_RUN_DEFAULT))).strip().lower() in {"1", "true", "yes", "y"}

# Optional county restriction, e.g. ["Hays", "Bastrop"]. Leave None for all folders.
SELECTED_COUNTIES: Optional[List[str]] = None

# Batching / retries.
FETCH_PAGE_SIZE = 1000
RETRY_ATTEMPTS = 3
RETRY_SLEEP_SEC = 2.0
PROGRESS_EVERY_ROWS = 10000

# Reports. A dated subfolder is created automatically under this base folder.
REPORT_BASE_FOLDER = Path(
    os.environ.get("SUBTRACT_EXISTING_REPORT_FOLDER")
    or os.environ.get("REPORT_FOLDER")
    or "./reports/supabase_subtract_existing_parcels"
)
RUN_DATE_LABEL = datetime.now().strftime("%Y-%m-%d")
RUN_TIME_LABEL = datetime.now().strftime("%H%M%S")
REPORT_FOLDER = REPORT_BASE_FOLDER / RUN_DATE_LABEL
REPORT_HTML_PATH = REPORT_FOLDER / f"subtract_existing_supabase_parcels_{RUN_TIME_LABEL}.html"
REPORT_SUMMARY_CSV_PATH = REPORT_FOLDER / f"subtract_existing_supabase_parcels_summary_{RUN_TIME_LABEL}.csv"
DROPPED_ROWS_CSV_PATH = REPORT_FOLDER / f"subtract_existing_supabase_parcels_dropped_rows_{RUN_TIME_LABEL}.csv"
PARTIAL_ROWS_CSV_PATH = REPORT_FOLDER / f"subtract_existing_supabase_parcels_partial_rows_{RUN_TIME_LABEL}.csv"
SKIPPED_ROWS_CSV_PATH = REPORT_FOLDER / f"subtract_existing_supabase_parcels_skipped_rows_{RUN_TIME_LABEL}.csv"

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm"}
CSV_READ_KWARGS = {
    "dtype": str,
    "keep_default_na": False,
}

# Pandas may keep rows as StringDtype/ExtensionArray when CSV/XLSX files are
# read with dtype=str. Assigning an int into that kind of row can raise:
# "Invalid value '1' for dtype 'str'". Keep working rows as object values
# and write count fields as strings because the output target is CSV.
CAST_INPUT_DATAFRAME_TO_OBJECT = True
WRITE_COUNT_FIELDS_AS_TEXT = True


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class CountyMeta:
    county_id: int
    county_name: str
    cad_category: Optional[int]


@dataclass
class ExistingParcelStats:
    county_name: str
    county_id: int
    db_rows_fetched: int
    rows_with_land_id: int
    rows_with_id_list: int
    existing_parcel_id_keys: int
    existing_parcel_ids_from_land_id: int
    existing_parcel_ids_from_id_list: int


@dataclass
class FileReport:
    county_name: str
    source_file: str
    output_file: str
    input_rows: int
    output_rows: int
    rows_kept_unchanged: int
    rows_dropped_fully_existing: int
    rows_partially_filtered: int
    parcels_seen_in_input: int
    parcels_removed_as_existing: int
    parcels_kept_as_new: int
    rows_missing_id: int
    rows_missing_valid_ids: int
    rows_with_length_mismatch: int
    status: str
    error: Optional[str] = None


@dataclass
class DroppedRowAudit:
    county_name: str
    source_file: str
    source_row_number: int
    id: str
    full_name: str
    parcel_ids: str
    matched_existing_ids: str
    reason: str


@dataclass
class PartialRowAudit:
    county_name: str
    source_file: str
    source_row_number: int
    original_id: str
    final_id: str
    full_name: str
    original_parcel_ids: str
    removed_existing_ids: str
    remaining_new_ids: str
    original_lands_count: int
    final_lands_count: int


@dataclass
class SkippedRowAudit:
    county_name: str
    source_file: str
    source_row_number: int
    id: str
    reason: str
    details: str


# =============================================================================
# LOGGING / BASIC HELPERS
# =============================================================================

def log(message: str) -> None:
    print(message, flush=True)


def now_label() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def retry_call(fn, *args, **kwargs):
    last_exc: Optional[BaseException] = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            log(f"[RETRY] Attempt {attempt}/{RETRY_ATTEMPTS} failed: {exc}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_SLEEP_SEC)
    raise last_exc  # type: ignore[misc]


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if isinstance(value, float) and math.isnan(value):
            return True
    except Exception:
        pass
    if isinstance(value, str):
        s = value.strip()
        return s == "" or s.lower() in {"nan", "none", "null", "na", "n/a"}
    return False


def clean_scalar_text(value: Any) -> str:
    if is_missing(value):
        return ""
    return str(value).replace("\ufeff", "").strip()


def json_dumps_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


# =============================================================================
# SUPABASE HELPERS
# =============================================================================

def get_supabase_client(env_path: Optional[str] = None) -> Client:
    if create_client is None:
        raise RuntimeError("The 'supabase' package is not installed. Install it with: pip install supabase")

    if env_path is None:
        default_env = Path(__file__).resolve().parent / ".env"
        if default_env.exists():
            load_dotenv(default_env, override=False)
        else:
            load_dotenv(find_dotenv(usecwd=True), override=False)
    else:
        load_dotenv(env_path, override=False)

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. "
            "Add them to your .env file or environment variables."
        )

    return create_client(url, key)  # type: ignore[misc]


def fetch_county_metadata(client: Client) -> Dict[str, CountyMeta]:
    log("[META] Fetching county metadata from Supabase...")
    resp = retry_call(
        client.table(COUNTY_TABLE)
        .select("county_id, county_name, cad_category")
        .execute
    )
    rows = resp.data or []
    out: Dict[str, CountyMeta] = {}
    for row in rows:
        county_name = clean_scalar_text(row.get("county_name"))
        if not county_name:
            continue
        out[county_name.lower()] = CountyMeta(
            county_id=int(row["county_id"]),
            county_name=county_name,
            cad_category=row.get("cad_category"),
        )
    log(f"[META] Loaded {len(out):,} county records.")
    return out


def fetch_existing_parcel_keys_for_county(
    client: Client,
    county_meta: CountyMeta,
) -> Tuple[Set[str], ExistingParcelStats]:
    """Fetch existing parcel IDs from Supabase land_id and id_list for one county."""
    log(f"[DB] Fetching existing parcels for {county_meta.county_name} from {EXISTING_OWNER_TABLE}...")
    existing_keys: Set[str] = set()
    db_rows_fetched = 0
    rows_with_land_id = 0
    rows_with_id_list = 0
    ids_from_land_id = 0
    ids_from_id_list = 0

    start = 0
    page = 0
    while True:
        end = start + FETCH_PAGE_SIZE - 1
        page += 1
        resp = retry_call(
            client.table(EXISTING_OWNER_TABLE)
            .select("unique_id, county_id, county_name, land_id, id_list")
            .eq("county_id", county_meta.county_id)
            .range(start, end)
            .execute
        )
        data = resp.data or []
        if not data:
            break

        db_rows_fetched += len(data)
        for row in data:
            land_id = clean_scalar_text(row.get("land_id"))
            if land_id:
                rows_with_land_id += 1
                before = len(existing_keys)
                existing_keys.update(id_key_variants(land_id))
                ids_from_land_id += max(0, len(existing_keys) - before)

            raw_id_list = parse_list_like_cell(row.get("id_list"))
            if raw_id_list:
                rows_with_id_list += 1
            for item in raw_id_list:
                before = len(existing_keys)
                existing_keys.update(id_key_variants(item))
                ids_from_id_list += max(0, len(existing_keys) - before)

        log(f"[DB] {county_meta.county_name}: fetched page {page:,} ({db_rows_fetched:,} DB rows so far)")
        if len(data) < FETCH_PAGE_SIZE:
            break
        start += FETCH_PAGE_SIZE

    stats = ExistingParcelStats(
        county_name=county_meta.county_name,
        county_id=county_meta.county_id,
        db_rows_fetched=db_rows_fetched,
        rows_with_land_id=rows_with_land_id,
        rows_with_id_list=rows_with_id_list,
        existing_parcel_id_keys=len(existing_keys),
        existing_parcel_ids_from_land_id=ids_from_land_id,
        existing_parcel_ids_from_id_list=ids_from_id_list,
    )
    log(
        f"[DB] {county_meta.county_name}: rows={db_rows_fetched:,}, "
        f"existing parcel keys={len(existing_keys):,}"
    )
    return existing_keys, stats


# =============================================================================
# FILE DISCOVERY / READERS / WRITERS
# =============================================================================

def list_county_folders(input_folder: Path) -> List[Path]:
    if not input_folder.exists():
        raise FileNotFoundError(f"Input folder does not exist: {input_folder}")
    if not input_folder.is_dir():
        raise NotADirectoryError(f"Input path is not a folder: {input_folder}")

    folders = [p for p in input_folder.iterdir() if p.is_dir()]
    if SELECTED_COUNTIES:
        selected = {c.lower() for c in SELECTED_COUNTIES}
        folders = [p for p in folders if p.name.lower() in selected]
    return sorted(folders, key=lambda p: p.name.lower())


def list_data_files(county_folder: Path) -> List[Path]:
    files = [p for p in county_folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_SUFFIXES]
    files = [p for p in files if not p.name.startswith("~$")]
    return sorted(files, key=lambda p: p.name.lower())


def read_data_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, **CSV_READ_KWARGS)
    elif suffix in {".xlsx", ".xls", ".xlsm"}:
        df = pd.read_excel(path, engine="openpyxl", dtype=str, keep_default_na=False)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    # Keep the local pipeline data editable even when pandas uses StringDtype.
    # This prevents failures when partial rows need numeric-looking fields such
    # as lands_count/owned_lands/acres to be rewritten.
    if CAST_INPUT_DATAFRAME_TO_OBJECT:
        df = df.astype(object)
    return df.where(pd.notna(df), "")


def output_path_for_file(county_name: str, input_file: Path, county_file_count: int) -> Path:
    county_output_folder = OUTPUT_FOLDER / county_name
    ensure_folder(county_output_folder)
    safe_county = sanitize_filename(county_name)
    if county_file_count <= 1:
        filename = f"{safe_county}{OUTPUT_FILE_SUFFIX}{OUTPUT_SUFFIX}"
    else:
        safe_stem = sanitize_filename(input_file.stem)
        filename = f"{safe_county}_{safe_stem}{OUTPUT_FILE_SUFFIX}{OUTPUT_SUFFIX}"
    return county_output_folder / filename


def write_output_file(df: pd.DataFrame, path: Path) -> None:
    ensure_folder(path.parent)
    if not DRY_RUN:
        df.to_csv(path, index=False)


def sanitize_filename(value: str) -> str:
    s = clean_scalar_text(value) or "county"
    s = re.sub(r"[^A-Za-z0-9_-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "county"


# =============================================================================
# PARSING HELPERS
# =============================================================================

def parse_list_like_cell(value: Any) -> List[Any]:
    """
    Parses values such as:
      ["123", "456"]
      ['123', '456']
      123 | 456
      123,456
      scalar
    Empty cells return an empty list.
    """
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, pd.Series):
        return list(value.values)
    if isinstance(value, (datetime, pd.Timestamp)):
        return [value]
    if isinstance(value, (int, float, Decimal)):
        if is_missing(value):
            return []
        return [value]

    s = clean_scalar_text(value)
    if not s or s == "[]":
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        inner = s[1:-1].strip()
        if not inner:
            return []
        return [p.strip().strip("'\"") for p in inner.split(",")]

    if " | " in s:
        return [p.strip() for p in s.split(" | ")]

    # For comma-separated lists, avoid splitting date-like values such as 01/01/2020.
    if "," in s and not re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", s):
        return [p.strip() for p in s.split(",")]

    return [s]


def normalize_land_id_text(value: Any) -> str:
    s = clean_scalar_text(value)
    if not s:
        return ""
    s = s.strip().strip("'\"")
    if re.fullmatch(r"-?\d+\.0+", s):
        return str(int(float(s)))
    return s


def parse_integer_value(value: Any) -> Optional[int]:
    if is_missing(value):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return int(value)
    s = normalize_land_id_text(value)
    if not s:
        return None
    s = s.replace(",", "")
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    if re.fullmatch(r"-?\d+\.0+", s):
        return int(float(s))
    return None


def parse_numeric_value(value: Any) -> Optional[float]:
    if is_missing(value):
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        try:
            if isinstance(value, float) and math.isnan(value):
                return None
            return float(value)
        except Exception:
            return None
    s = clean_scalar_text(value).strip().strip("'\"")
    if not s:
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def id_key_variants(value: Any) -> Set[str]:
    """Return comparable variants for a parcel/land ID.

    This allows matching DB id_list integers like 10522 against local strings like
    "10522" or Excel strings like "10522.0" while still preserving raw text when needed.
    """
    variants: Set[str] = set()
    s = normalize_land_id_text(value)
    if s:
        variants.add(s)
    integer_value = parse_integer_value(value)
    if integer_value is not None:
        variants.add(str(integer_value))
    return variants


def id_exists_in_db(value: Any, existing_keys: Set[str]) -> bool:
    variants = id_key_variants(value)
    return bool(variants and variants.intersection(existing_keys))


def clean_list_item_for_output(value: Any) -> Any:
    if is_missing(value):
        return ""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date().isoformat()
    return clean_scalar_text(value)


def list_to_cell(values: Sequence[Any]) -> str:
    cleaned = [clean_list_item_for_output(v) for v in values]
    if WRITE_LISTS_AS_JSON:
        return json_dumps_compact(cleaned)
    return " | ".join(clean_scalar_text(v) for v in cleaned)


def pad_or_trim_for_positions(values: List[Any], target_length: int) -> Tuple[List[Any], bool]:
    mismatch = len(values) != target_length
    if target_length <= 0:
        return values, mismatch
    if len(values) < target_length:
        values = values + [""] * (target_length - len(values))
    elif len(values) > target_length:
        values = values[:target_length]
    return values, mismatch


def recompute_acres_from_list(acres_items: Sequence[Any]) -> Optional[float]:
    nums = [parse_numeric_value(v) for v in acres_items]
    nums = [v for v in nums if v is not None]
    if not nums:
        return None
    return round(float(sum(nums)), ACRES_ROUND_DECIMALS)


def format_numeric_for_cell(value: float) -> str:
    text = f"{value:.{ACRES_ROUND_DECIMALS}f}"
    text = text.rstrip("0").rstrip(".")
    return text or "0"


# =============================================================================
# SUBTRACTION LOGIC
# =============================================================================

def extract_row_id_items(row: pd.Series) -> Tuple[List[Any], bool]:
    """Return parcel IDs represented by this row and whether they came from id_list."""
    raw_ids: List[Any] = []
    used_id_list = False
    if LOCAL_ID_LIST_COLUMN in row.index:
        raw_ids = parse_list_like_cell(row.get(LOCAL_ID_LIST_COLUMN))
        used_id_list = bool(raw_ids)
    if not raw_ids:
        raw_ids = [row.get(LOCAL_ID_COLUMN, "")]
        used_id_list = False
    return raw_ids, used_id_list


def filter_row_against_existing_ids(
    row: pd.Series,
    existing_keys: Set[str],
    county_name: str,
    source_file: str,
    source_row_number: int,
) -> Tuple[Optional[pd.Series], str, Optional[DroppedRowAudit], Optional[PartialRowAudit], Optional[SkippedRowAudit], Dict[str, int]]:
    """Filter one row.

    Returns:
      final row or None,
      action: kept | dropped | partial | skipped_kept,
      audit objects,
      stats dict
    """
    stats = {
        "parcels_seen": 0,
        "parcels_removed": 0,
        "parcels_kept": 0,
        "length_mismatch": 0,
    }

    source_id = clean_scalar_text(row.get(LOCAL_ID_COLUMN, ""))
    full_name = clean_scalar_text(row.get(LOCAL_FULL_NAME_COLUMN, "")) if LOCAL_FULL_NAME_COLUMN in row.index else ""

    if not source_id:
        skip = SkippedRowAudit(
            county_name=county_name,
            source_file=source_file,
            source_row_number=source_row_number,
            id="",
            reason="missing_local_id",
            details=f"Missing required local `{LOCAL_ID_COLUMN}` value. Row was kept to avoid accidental loss.",
        )
        return row.copy(), "skipped_kept", None, None, skip, stats

    raw_ids, used_id_list = extract_row_id_items(row)
    valid_positions: List[int] = []
    valid_ids: List[Any] = []
    invalid_count = 0

    for pos, item in enumerate(raw_ids):
        if id_key_variants(item):
            valid_positions.append(pos)
            valid_ids.append(item)
        else:
            invalid_count += 1

    if not valid_ids:
        skip = SkippedRowAudit(
            county_name=county_name,
            source_file=source_file,
            source_row_number=source_row_number,
            id=source_id,
            reason="no_valid_parcel_ids",
            details="Neither id_list nor id contained a valid comparable parcel ID. Row was kept to avoid accidental loss.",
        )
        return row.copy(), "skipped_kept", None, None, skip, stats

    stats["parcels_seen"] = len(valid_ids)
    keep_positions: List[int] = []
    removed_positions: List[int] = []
    for pos, item in zip(valid_positions, valid_ids):
        if id_exists_in_db(item, existing_keys):
            removed_positions.append(pos)
        else:
            keep_positions.append(pos)

    stats["parcels_removed"] = len(removed_positions)
    stats["parcels_kept"] = len(keep_positions)

    if not removed_positions:
        return row.copy(), "kept", None, None, None, stats

    original_ids_clean = [normalize_land_id_text(x) for x in valid_ids]
    removed_ids_clean = [normalize_land_id_text(raw_ids[pos]) for pos in removed_positions]

    if not keep_positions:
        dropped = DroppedRowAudit(
            county_name=county_name,
            source_file=source_file,
            source_row_number=source_row_number,
            id=source_id,
            full_name=full_name,
            parcel_ids=list_to_cell(original_ids_clean),
            matched_existing_ids=list_to_cell(removed_ids_clean),
            reason="all_row_parcels_already_exist_in_supabase",
        )
        return None, "dropped", dropped, None, None, stats

    # Partial row: keep only the remaining parcel IDs and filter aligned columns.
    # Convert the row to object dtype before edits. This avoids pandas StringDtype
    # assignment errors when we update count/acres fields after partial filtering.
    new_row = row.copy(deep=True).astype(object)
    remaining_ids = [raw_ids[pos] for pos in keep_positions]
    remaining_ids_clean = [normalize_land_id_text(x) for x in remaining_ids]

    if UPDATE_ROW_ID_TO_FIRST_REMAINING_ID and remaining_ids_clean:
        new_row[LOCAL_ID_COLUMN] = remaining_ids_clean[0]

    final_id = clean_scalar_text(new_row.get(LOCAL_ID_COLUMN, ""))

    # We only filter aligned lists when the row truly had an id_list. If it did not,
    # the only partial state should not occur because a scalar row has one ID.
    if used_id_list:
        original_list_length = len(raw_ids)
        for col in ALIGNED_LIST_COLUMNS:
            if col not in new_row.index:
                continue
            values = parse_list_like_cell(new_row.get(col))
            values, mismatch = pad_or_trim_for_positions(values, original_list_length)
            if mismatch:
                stats["length_mismatch"] = 1
            filtered = [values[pos] if pos < len(values) else "" for pos in keep_positions]
            new_row[col] = list_to_cell(filtered)
    else:
        # If no id_list existed but the scalar id survived, keep the row unchanged.
        pass

    # Make sure id_list exists for a partially filtered row, even if the original cell was strange.
    if LOCAL_ID_LIST_COLUMN in new_row.index:
        new_row[LOCAL_ID_LIST_COLUMN] = list_to_cell(remaining_ids_clean)

    final_lands_count = len(remaining_ids_clean)
    for count_col in LOCAL_LANDS_COUNT_COLUMNS:
        if count_col in new_row.index:
            new_row[count_col] = str(final_lands_count) if WRITE_COUNT_FIELDS_AS_TEXT else final_lands_count

    if RECOMPUTE_ACRES_FOR_PARTIAL_ROWS and "acres_list" in new_row.index and LOCAL_ACRES_COLUMN in new_row.index:
        filtered_acres_items = parse_list_like_cell(new_row.get("acres_list"))
        acres_sum = recompute_acres_from_list(filtered_acres_items)
        if acres_sum is not None:
            new_row[LOCAL_ACRES_COLUMN] = format_numeric_for_cell(acres_sum)

    partial = PartialRowAudit(
        county_name=county_name,
        source_file=source_file,
        source_row_number=source_row_number,
        original_id=source_id,
        final_id=final_id,
        full_name=full_name,
        original_parcel_ids=list_to_cell(original_ids_clean),
        removed_existing_ids=list_to_cell(removed_ids_clean),
        remaining_new_ids=list_to_cell(remaining_ids_clean),
        original_lands_count=len(original_ids_clean),
        final_lands_count=final_lands_count,
    )
    return new_row, "partial", None, partial, None, stats


def process_file(
    county_name: str,
    file_path: Path,
    output_path: Path,
    existing_keys: Set[str],
) -> Tuple[FileReport, List[DroppedRowAudit], List[PartialRowAudit], List[SkippedRowAudit]]:
    log(f"[FILE] Reading {county_name}: {file_path.name}")
    df = read_data_file(file_path)

    if LOCAL_ID_COLUMN not in df.columns:
        raise KeyError(f"Missing required local column `{LOCAL_ID_COLUMN}` in {file_path.name}")

    output_rows: List[pd.Series] = []
    dropped_audit: List[DroppedRowAudit] = []
    partial_audit: List[PartialRowAudit] = []
    skipped_audit: List[SkippedRowAudit] = []

    rows_kept_unchanged = 0
    rows_dropped = 0
    rows_partial = 0
    rows_missing_id = 0
    rows_missing_valid_ids = 0
    rows_with_length_mismatch = 0
    parcels_seen = 0
    parcels_removed = 0
    parcels_kept = 0

    total = len(df)
    for idx, row in df.iterrows():
        row_number = int(idx) + 2
        if row_number == 2 or row_number % PROGRESS_EVERY_ROWS == 0 or row_number == total + 1:
            log(f"[FILTER] {county_name}/{file_path.name}: row {row_number - 1:,}/{total:,}")

        try:
            final_row, action, dropped, partial, skipped, stats = filter_row_against_existing_ids(
                row=row,
                existing_keys=existing_keys,
                county_name=county_name,
                source_file=file_path.name,
                source_row_number=row_number,
            )
        except Exception as row_exc:
            # Do not let one unexpected row-format issue destroy the whole county file.
            # Keep the row unchanged and record the issue in the skipped audit report.
            if row_number == 2 or row_number % PROGRESS_EVERY_ROWS == 0:
                log(f"[ROW WARN] {county_name}/{file_path.name}: row {row_number:,} kept after row error: {row_exc}")
            final_row = row.copy(deep=True).astype(object)
            action = "skipped_kept"
            dropped = None
            partial = None
            skipped = SkippedRowAudit(
                county_name=county_name,
                source_file=file_path.name,
                source_row_number=row_number,
                id=clean_scalar_text(row.get(LOCAL_ID_COLUMN, "")),
                reason="row_processing_error",
                details=str(row_exc),
            )
            stats = {}

        parcels_seen += stats.get("parcels_seen", 0)
        parcels_removed += stats.get("parcels_removed", 0)
        parcels_kept += stats.get("parcels_kept", 0)
        rows_with_length_mismatch += stats.get("length_mismatch", 0)

        if skipped is not None:
            skipped_audit.append(skipped)
            if skipped.reason == "missing_local_id":
                rows_missing_id += 1
            elif skipped.reason == "no_valid_parcel_ids":
                rows_missing_valid_ids += 1

        if dropped is not None:
            dropped_audit.append(dropped)
        if partial is not None:
            partial_audit.append(partial)

        if action in {"kept", "skipped_kept"}:
            rows_kept_unchanged += 1
        elif action == "dropped":
            rows_dropped += 1
        elif action == "partial":
            rows_partial += 1

        if final_row is not None:
            output_rows.append(final_row)

    out_df = pd.DataFrame(output_rows, columns=df.columns)
    write_output_file(out_df, output_path)

    log(
        f"[SAVE] {county_name}: {len(df):,} -> {len(out_df):,} rows | "
        f"dropped={rows_dropped:,} partial={rows_partial:,} kept={rows_kept_unchanged:,} | {output_path}"
    )

    report = FileReport(
        county_name=county_name,
        source_file=file_path.name,
        output_file=str(output_path),
        input_rows=len(df),
        output_rows=len(out_df),
        rows_kept_unchanged=rows_kept_unchanged,
        rows_dropped_fully_existing=rows_dropped,
        rows_partially_filtered=rows_partial,
        parcels_seen_in_input=parcels_seen,
        parcels_removed_as_existing=parcels_removed,
        parcels_kept_as_new=parcels_kept,
        rows_missing_id=rows_missing_id,
        rows_missing_valid_ids=rows_missing_valid_ids,
        rows_with_length_mismatch=rows_with_length_mismatch,
        status="ok",
    )
    return report, dropped_audit, partial_audit, skipped_audit


# =============================================================================
# REPORTING
# =============================================================================

def write_csv_reports(
    file_reports: List[FileReport],
    existing_stats: List[ExistingParcelStats],
    dropped_rows: List[DroppedRowAudit],
    partial_rows: List[PartialRowAudit],
    skipped_rows: List[SkippedRowAudit],
) -> None:
    ensure_folder(REPORT_FOLDER)
    ensure_folder(OUTPUT_FOLDER)

    # Report folder CSVs.
    pd.DataFrame([asdict(r) for r in file_reports]).to_csv(REPORT_SUMMARY_CSV_PATH, index=False)
    log(f"[REPORT] Summary CSV written: {REPORT_SUMMARY_CSV_PATH}")

    pd.DataFrame([asdict(r) for r in dropped_rows]).to_csv(DROPPED_ROWS_CSV_PATH, index=False)
    log(f"[REPORT] Dropped rows CSV written: {DROPPED_ROWS_CSV_PATH}")

    pd.DataFrame([asdict(r) for r in partial_rows]).to_csv(PARTIAL_ROWS_CSV_PATH, index=False)
    log(f"[REPORT] Partial rows CSV written: {PARTIAL_ROWS_CSV_PATH}")

    pd.DataFrame([asdict(r) for r in skipped_rows]).to_csv(SKIPPED_ROWS_CSV_PATH, index=False)
    log(f"[REPORT] Skipped rows CSV written: {SKIPPED_ROWS_CSV_PATH}")

    db_stats_path = REPORT_FOLDER / f"subtract_existing_supabase_parcels_db_existing_stats_{RUN_TIME_LABEL}.csv"
    pd.DataFrame([asdict(r) for r in existing_stats]).to_csv(db_stats_path, index=False)
    log(f"[REPORT] DB existing stats CSV written: {db_stats_path}")

    # Root output summary for the pipeline folder.
    if WRITE_ROOT_SUMMARY_CSV and not DRY_RUN:
        root_summary_path = OUTPUT_FOLDER / SUMMARY_CSV_NAME
        pd.DataFrame([asdict(r) for r in file_reports]).to_csv(root_summary_path, index=False)
        log(f"[REPORT] Output-root summary written: {root_summary_path}")


def generate_html_report(
    file_reports: List[FileReport],
    existing_stats: List[ExistingParcelStats],
    dropped_rows: List[DroppedRowAudit],
    partial_rows: List[PartialRowAudit],
    skipped_rows: List[SkippedRowAudit],
    started_at: datetime,
    ended_at: datetime,
) -> None:
    ensure_folder(REPORT_HTML_PATH.parent)

    totals = {
        "counties": len(set(r.county_name for r in file_reports)),
        "files": len(file_reports),
        "input_rows": sum(r.input_rows for r in file_reports),
        "output_rows": sum(r.output_rows for r in file_reports),
        "dropped_rows": sum(r.rows_dropped_fully_existing for r in file_reports),
        "partial_rows": sum(r.rows_partially_filtered for r in file_reports),
        "kept_rows": sum(r.rows_kept_unchanged for r in file_reports),
        "parcels_seen": sum(r.parcels_seen_in_input for r in file_reports),
        "parcels_removed": sum(r.parcels_removed_as_existing for r in file_reports),
        "parcels_kept": sum(r.parcels_kept_as_new for r in file_reports),
        "db_rows": sum(s.db_rows_fetched for s in existing_stats),
        "db_existing_keys": sum(s.existing_parcel_id_keys for s in existing_stats),
        "skipped_rows": len(skipped_rows),
    }

    duration = (ended_at - started_at).total_seconds()
    mode_label = "DRY RUN - no cleaned output CSVs were written" if DRY_RUN else "LIVE FILE RUN - cleaned output CSVs were written"

    rows_html = []
    for r in file_reports:
        status_class = "ok" if r.status == "ok" else "warn"
        rows_html.append(f"""
        <tr>
          <td>{escape(r.county_name)}</td>
          <td>{escape(r.source_file)}</td>
          <td>{escape(Path(r.output_file).name)}</td>
          <td class="num">{r.input_rows:,}</td>
          <td class="num good">{r.output_rows:,}</td>
          <td class="num">{r.rows_kept_unchanged:,}</td>
          <td class="num warn-text">{r.rows_dropped_fully_existing:,}</td>
          <td class="num warn-text">{r.rows_partially_filtered:,}</td>
          <td class="num">{r.parcels_seen_in_input:,}</td>
          <td class="num warn-text">{r.parcels_removed_as_existing:,}</td>
          <td class="num good">{r.parcels_kept_as_new:,}</td>
          <td class="num">{r.rows_with_length_mismatch:,}</td>
          <td><span class="badge {status_class}">{escape(r.status)}</span></td>
          <td><code>{escape(r.error or '')}</code></td>
        </tr>
        """)
    rows_html_text = "\n".join(rows_html) if rows_html else "<tr><td colspan='14' class='empty'>No files processed.</td></tr>"

    db_stats_html = []
    for s in existing_stats:
        db_stats_html.append(f"""
        <tr>
          <td>{escape(s.county_name)}</td>
          <td class="num">{s.county_id:,}</td>
          <td class="num">{s.db_rows_fetched:,}</td>
          <td class="num">{s.rows_with_land_id:,}</td>
          <td class="num">{s.rows_with_id_list:,}</td>
          <td class="num good">{s.existing_parcel_id_keys:,}</td>
          <td class="num">{s.existing_parcel_ids_from_land_id:,}</td>
          <td class="num">{s.existing_parcel_ids_from_id_list:,}</td>
        </tr>
        """)
    db_stats_html_text = "\n".join(db_stats_html) if db_stats_html else "<tr><td colspan='8' class='empty'>No DB county stats.</td></tr>"

    skip_reason_counts: Dict[str, int] = {}
    for row in skipped_rows:
        skip_reason_counts[row.reason] = skip_reason_counts.get(row.reason, 0) + 1
    skip_html = "".join(
        f"<tr><td>{escape(reason)}</td><td class='num'>{count:,}</td></tr>"
        for reason, count in sorted(skip_reason_counts.items(), key=lambda x: (-x[1], x[0]))
    ) or "<tr><td colspan='2' class='empty'>No skipped rows.</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Supabase Existing Parcel Subtraction Report</title>
<style>
  body {{
    background-color: #f0f0f0;
    font-family: Arial, sans-serif;
    padding: 20px;
    color: #222;
  }}
  h1, h2 {{ color: #333; }}
  .summary-box {{
    background: #fff;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 18px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
    font-size: 14px;
  }}
  .cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 12px;
    margin: 14px 0;
  }}
  .card {{
    background: #fff;
    border-radius: 8px;
    padding: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }}
  .card .label {{
    color: #666;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .03em;
  }}
  .card .value {{
    margin-top: 6px;
    font-size: 24px;
    font-weight: bold;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    background: #fff;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
    margin-bottom: 20px;
  }}
  th, td {{
    padding: 8px 10px;
    border-bottom: 1px solid #e2e2e2;
    font-size: 13px;
    vertical-align: top;
  }}
  th {{
    background: #e9e9e9;
    text-align: left;
    position: sticky;
    top: 0;
  }}
  tr:nth-child(even) td {{ background: #fafafa; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .good {{ color: #0a7b28; font-weight: bold; }}
  .warn-text {{ color: #9a5b00; font-weight: bold; }}
  .badge {{
    display: inline-block;
    padding: 3px 8px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: bold;
  }}
  .badge.ok {{ background: #e8f5e9; color: #1b5e20; }}
  .badge.warn {{ background: #fff3e0; color: #8a4b00; }}
  code {{ color: #b00020; white-space: pre-wrap; }}
  .footer-note {{ margin-top: 16px; font-size: 12px; color: #666; }}
  .empty {{ text-align:center; color:#666; }}
</style>
</head>
<body>
  <h1>Supabase Existing Parcel Subtraction Report</h1>

  <div class="summary-box">
    <h2>Run Summary</h2>
    <p><strong>Mode:</strong> {escape(mode_label)}</p>
    <p><strong>Input folder:</strong> {escape(str(INPUT_FOLDER))}</p>
    <p><strong>Output folder:</strong> {escape(str(OUTPUT_FOLDER))}</p>
    <p><strong>Report folder:</strong> {escape(str(REPORT_FOLDER))}</p>
    <p><strong>Started:</strong> {escape(started_at.strftime('%Y-%m-%d %H:%M:%S'))}</p>
    <p><strong>Finished:</strong> {escape(ended_at.strftime('%Y-%m-%d %H:%M:%S'))}</p>
    <p><strong>Duration:</strong> {duration:,.1f} seconds</p>
    <p><strong>Database table used for subtraction:</strong> <code>{escape(EXISTING_OWNER_TABLE)}</code></p>
    <p><strong>Matching strategy:</strong> local <code>id</code> and local <code>id_list</code> are compared against Supabase <code>land_id</code> and Supabase <code>id_list</code> for the same county.</p>
  </div>

  <div class="cards">
    <div class="card"><div class="label">Counties processed</div><div class="value">{totals['counties']:,}</div></div>
    <div class="card"><div class="label">Files processed</div><div class="value">{totals['files']:,}</div></div>
    <div class="card"><div class="label">Input rows</div><div class="value">{totals['input_rows']:,}</div></div>
    <div class="card"><div class="label">Output rows</div><div class="value good">{totals['output_rows']:,}</div></div>
    <div class="card"><div class="label">Rows dropped</div><div class="value warn-text">{totals['dropped_rows']:,}</div></div>
    <div class="card"><div class="label">Rows partially filtered</div><div class="value warn-text">{totals['partial_rows']:,}</div></div>
    <div class="card"><div class="label">Parcels seen</div><div class="value">{totals['parcels_seen']:,}</div></div>
    <div class="card"><div class="label">Parcels removed</div><div class="value warn-text">{totals['parcels_removed']:,}</div></div>
    <div class="card"><div class="label">Parcels kept as new</div><div class="value good">{totals['parcels_kept']:,}</div></div>
    <div class="card"><div class="label">DB owner rows fetched</div><div class="value">{totals['db_rows']:,}</div></div>
    <div class="card"><div class="label">DB existing ID keys</div><div class="value">{totals['db_existing_keys']:,}</div></div>
    <div class="card"><div class="label">Skipped rows kept</div><div class="value">{totals['skipped_rows']:,}</div></div>
  </div>

  <h2>Per-File Details</h2>
  <table>
    <thead>
      <tr>
        <th>County</th>
        <th>Source File</th>
        <th>Output File</th>
        <th>Input Rows</th>
        <th>Output Rows</th>
        <th>Kept</th>
        <th>Dropped</th>
        <th>Partial</th>
        <th>Parcels Seen</th>
        <th>Parcels Removed</th>
        <th>Parcels Kept</th>
        <th>List Mismatch</th>
        <th>Status</th>
        <th>Error</th>
      </tr>
    </thead>
    <tbody>
      {rows_html_text}
    </tbody>
  </table>

  <h2>Existing Database Parcel Stats</h2>
  <table>
    <thead>
      <tr>
        <th>County</th>
        <th>County ID</th>
        <th>DB Rows</th>
        <th>Rows w/ land_id</th>
        <th>Rows w/ id_list</th>
        <th>Existing ID Keys</th>
        <th>Keys From land_id</th>
        <th>Keys From id_list</th>
      </tr>
    </thead>
    <tbody>
      {db_stats_html_text}
    </tbody>
  </table>

  <h2>Skipped / Kept for Review</h2>
  <table>
    <thead><tr><th>Reason</th><th>Count</th></tr></thead>
    <tbody>{skip_html}</tbody>
  </table>

  <p class="footer-note">
    Fully existing rows are removed from the output file. Partially existing rows are retained, but their aligned list fields are filtered to keep only the parcel IDs not already present in Supabase. The script does not update or insert any database rows.
  </p>
</body>
</html>
"""
    REPORT_HTML_PATH.write_text(html, encoding="utf-8")
    log(f"[REPORT] HTML written: {REPORT_HTML_PATH}")


# =============================================================================
# MAIN PROCESS
# =============================================================================

def process_county(
    client: Client,
    county_folder: Path,
    county_meta: CountyMeta,
) -> Tuple[List[FileReport], ExistingParcelStats, List[DroppedRowAudit], List[PartialRowAudit], List[SkippedRowAudit]]:
    county_name = county_folder.name
    existing_keys, existing_stats = fetch_existing_parcel_keys_for_county(client, county_meta)

    files = list_data_files(county_folder)
    log(f"[COUNTY] {county_name}: found {len(files):,} data file(s).")

    file_reports: List[FileReport] = []
    dropped_rows: List[DroppedRowAudit] = []
    partial_rows: List[PartialRowAudit] = []
    skipped_rows: List[SkippedRowAudit] = []

    if not files:
        file_reports.append(FileReport(
            county_name=county_name,
            source_file="",
            output_file="",
            input_rows=0,
            output_rows=0,
            rows_kept_unchanged=0,
            rows_dropped_fully_existing=0,
            rows_partially_filtered=0,
            parcels_seen_in_input=0,
            parcels_removed_as_existing=0,
            parcels_kept_as_new=0,
            rows_missing_id=0,
            rows_missing_valid_ids=0,
            rows_with_length_mismatch=0,
            status="warn",
            error="no_data_files_found",
        ))
        return file_reports, existing_stats, dropped_rows, partial_rows, skipped_rows

    for file_path in files:
        out_path = output_path_for_file(county_name, file_path, county_file_count=len(files))
        try:
            report, dropped, partial, skipped = process_file(
                county_name=county_name,
                file_path=file_path,
                output_path=out_path,
                existing_keys=existing_keys,
            )
            file_reports.append(report)
            dropped_rows.extend(dropped)
            partial_rows.extend(partial)
            skipped_rows.extend(skipped)
        except Exception as exc:
            log(f"[ERROR] {county_name}/{file_path.name} failed: {exc}")
            log(traceback.format_exc())
            file_reports.append(FileReport(
                county_name=county_name,
                source_file=file_path.name,
                output_file=str(out_path),
                input_rows=0,
                output_rows=0,
                rows_kept_unchanged=0,
                rows_dropped_fully_existing=0,
                rows_partially_filtered=0,
                parcels_seen_in_input=0,
                parcels_removed_as_existing=0,
                parcels_kept_as_new=0,
                rows_missing_id=0,
                rows_missing_valid_ids=0,
                rows_with_length_mismatch=0,
                status="error",
                error=str(exc),
            ))

    return file_reports, existing_stats, dropped_rows, partial_rows, skipped_rows


def main() -> None:
    started_at = datetime.now()
    log("=" * 90)
    log("[START] Subtract existing Supabase parcels from local county files")
    log(f"[TIME]   {now_label()}")
    log(f"[INPUT]  {INPUT_FOLDER}")
    log(f"[OUTPUT] {OUTPUT_FOLDER}")
    log(f"[REPORT] {REPORT_FOLDER}")
    log(f"[MODE]   {'DRY RUN - no output CSVs written' if DRY_RUN else 'LIVE FILE RUN'}")
    log(f"[DB]     {EXISTING_OWNER_TABLE}: land_id + id_list")
    log("=" * 90)

    ensure_folder(REPORT_FOLDER)
    if not DRY_RUN:
        ensure_folder(OUTPUT_FOLDER)

    client = get_supabase_client()
    county_meta_map = fetch_county_metadata(client)

    county_folders = list_county_folders(INPUT_FOLDER)
    log(f"[MAIN] Found {len(county_folders):,} county folder(s).")

    all_file_reports: List[FileReport] = []
    all_existing_stats: List[ExistingParcelStats] = []
    all_dropped_rows: List[DroppedRowAudit] = []
    all_partial_rows: List[PartialRowAudit] = []
    all_skipped_rows: List[SkippedRowAudit] = []

    for idx, county_folder in enumerate(county_folders, start=1):
        county_name = county_folder.name
        log("=" * 90)
        log(f"[COUNTY] ({idx:,}/{len(county_folders):,}) {county_name}")

        meta = county_meta_map.get(county_name.lower())
        if not meta:
            msg = "county_not_found_in_supabase_county_table"
            log(f"[WARN] {county_name}: {msg}. Skipping county.")
            all_file_reports.append(FileReport(
                county_name=county_name,
                source_file="",
                output_file="",
                input_rows=0,
                output_rows=0,
                rows_kept_unchanged=0,
                rows_dropped_fully_existing=0,
                rows_partially_filtered=0,
                parcels_seen_in_input=0,
                parcels_removed_as_existing=0,
                parcels_kept_as_new=0,
                rows_missing_id=0,
                rows_missing_valid_ids=0,
                rows_with_length_mismatch=0,
                status="warn",
                error=msg,
            ))
            continue

        file_reports, existing_stats, dropped, partial, skipped = process_county(
            client=client,
            county_folder=county_folder,
            county_meta=meta,
        )
        all_file_reports.extend(file_reports)
        all_existing_stats.append(existing_stats)
        all_dropped_rows.extend(dropped)
        all_partial_rows.extend(partial)
        all_skipped_rows.extend(skipped)

        county_input_rows = sum(r.input_rows for r in file_reports)
        county_output_rows = sum(r.output_rows for r in file_reports)
        county_removed_parcels = sum(r.parcels_removed_as_existing for r in file_reports)
        county_new_parcels = sum(r.parcels_kept_as_new for r in file_reports)
        log(
            f"[COUNTY DONE] {county_name}: input_rows={county_input_rows:,} output_rows={county_output_rows:,} "
            f"removed_parcels={county_removed_parcels:,} kept_new_parcels={county_new_parcels:,}"
        )

    ended_at = datetime.now()

    write_csv_reports(
        file_reports=all_file_reports,
        existing_stats=all_existing_stats,
        dropped_rows=all_dropped_rows,
        partial_rows=all_partial_rows,
        skipped_rows=all_skipped_rows,
    )
    generate_html_report(
        file_reports=all_file_reports,
        existing_stats=all_existing_stats,
        dropped_rows=all_dropped_rows,
        partial_rows=all_partial_rows,
        skipped_rows=all_skipped_rows,
        started_at=started_at,
        ended_at=ended_at,
    )

    log("=" * 90)
    log("[DONE] Existing Supabase parcel subtraction completed.")
    log(f"[MODE] {'DRY RUN - no output CSVs written' if DRY_RUN else 'LIVE FILE RUN'}")
    log(f"[OUTPUT] {OUTPUT_FOLDER}")
    log(f"[REPORT HTML] {REPORT_HTML_PATH}")
    log(f"[SUMMARY CSV] {REPORT_SUMMARY_CSV_PATH}")
    log(f"[DROPPED CSV] {DROPPED_ROWS_CSV_PATH}")
    log(f"[PARTIAL CSV] {PARTIAL_ROWS_CSV_PATH}")
    log(f"[SKIPPED CSV] {SKIPPED_ROWS_CSV_PATH}")
    log("=" * 90)


if __name__ == "__main__":
    main()
