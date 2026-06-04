#!/usr/bin/env python
"""
Pipeline step 5: create mailing-address group rows while preserving owner rows.

Recommended script name:
    5_rollup_by_mailing_address_add_group_rows_no_arg.py

Run after:
    4_dedup_owners_add_lists_no_arg.py

What it does:
- Reads the county folders produced by 4_dedup_owners_add_lists_no_arg.py.
- Finds repeated, non-blank mailing_address values.
- Keeps the original owner rows instead of deleting them.
- For original rows that belong to a repeated mailing_address group, clears the
  mailing_address cell so that only the new group row keeps that mailing address.
- Adds one synthetic mailing-address group row per repeated mailing_address.
- The synthetic group row has:
    legal_address = blank
    mailing_address = the shared mailing address
    acres = sum of the merged parcel acres
    lands_count = number of merged parcel entries
    id = the representative owner/parcel id chosen from the group
         (owner with most parcels; ties use the highest-acre parcel)
    full_name = MA#<HASH> | primary owner (+N others)
- Merges parcel-level lists in the same order:
    id_list
    acres_list
    market_value_list
    owner_tax_year_list
    deed_date_list
    legal_acreage_filled_by_script
    Empty_Legal_Acreage

Important alignment rule:
- id_list, acres_list, market_value_list, owner_tax_year_list, deed_date_list,
  legal_acreage_filled_by_script, and Empty_Legal_Acreage are all merged as aligned lists.
- Position 1 in every list belongs to the same parcel, position 2 belongs to the same
  parcel, and so on.

Requirements:
    pip install pandas openpyxl

No command-line arguments are required. Edit CONFIG values below if needed.
You can also override INPUT_ROOT and OUTPUT_ROOT with these environment variables:
    CAD_MAILING_ROLLUP_INPUT_ROOT
    CAD_MAILING_ROLLUP_OUTPUT_ROOT
    INPUT_ROOT
    OUTPUT_ROOT
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
import os
import re
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# This should match OUTPUT_ROOT from 4_dedup_owners_add_lists_no_arg.py.
DEFAULT_INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_5_no_owner_duplicates"

# New pipeline output folder.
DEFAULT_OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_6_no_mailing_address_duplicates"

INPUT_ROOT = os.environ.get("CAD_MAILING_ROLLUP_INPUT_ROOT") or os.environ.get("INPUT_ROOT") or DEFAULT_INPUT_ROOT
OUTPUT_ROOT = os.environ.get("CAD_MAILING_ROLLUP_OUTPUT_ROOT") or os.environ.get("OUTPUT_ROOT") or DEFAULT_OUTPUT_ROOT

# Required core columns expected after owner dedupe.
COL_ID = "id"
COL_FULL_NAME = "full_name"
COL_LEGAL_ADDRESS = "legal_address"
COL_MAILING = "mailing_address"
COL_ACRES = "acres"
COL_CITY = "city"
COL_ZIP = "zip"
COL_STATE = "state"
COL_LANDS_COUNT = "lands_count"
COL_ACRES_LIST = "acres_list"
COL_ID_LIST = "id_list"

# Additional aligned list columns to preserve and merge with id_list/acres_list.
ALIGNED_LIST_COLUMNS = [
    "market_value_list",
    "owner_tax_year_list",
    "deed_date_list",
    "legal_acreage_filled_by_script",
    "Empty_Legal_Acreage",
]

REQUIRED_COLS = [
    COL_ID,
    COL_FULL_NAME,
    COL_LEGAL_ADDRESS,
    COL_MAILING,
    COL_ACRES,
    COL_CITY,
    COL_ZIP,
    COL_STATE,
    COL_LANDS_COUNT,
    COL_ACRES_LIST,
    COL_ID_LIST,
]

# If True, missing market/tax/deed aligned list columns are created as blank lists.
CREATE_MISSING_ALIGNED_LIST_COLUMNS = True

# Output naming.
OUTPUT_FILE_SUFFIX = "_no_mailing_address_duplicates"
OUTPUT_SUFFIX = ".csv"
WRITE_SUMMARY_CSV = True
SUMMARY_CSV_NAME = "rollup_by_mailing_address_summary.csv"

# JSON keeps list positions explicit and safe in CSV.
# Example: ["12345", "67890"]
LIST_OUTPUT_FORMAT = "json"  # valid: "json" or "pipe"
PIPE_SEPARATOR = " | "

# True means duplicate parcel ids inside the same mailing-address rollup are collapsed
# and the first occurrence keeps its aligned acres/market/year/deed values.
DEDUPLICATE_IDS_WITHIN_MAILING_GROUP = True

# Blank parcel ids are preserved as separate entries so list positions do not collapse.
KEEP_BLANK_ID_ENTRIES = True

# New mailing-address rollup behavior.
# True means original owner rows are kept, and one new synthetic row is appended for
# each repeated non-blank mailing address.
KEEP_ORIGINAL_ROWS_AND_ADD_MAILING_GROUP_ROWS = True

# True means source rows included in a repeated mailing-address group have their
# mailing_address cleared. This leaves exactly one row with that shared mailing address:
# the synthetic group row.
CLEAR_MAILING_ADDRESS_ON_SOURCE_ROWS_IN_DUPLICATE_GROUPS = True

# Group rows are appended after all original/source rows. This matches the visual
# example: source rows first, mailing-address group rows at the bottom.
APPEND_MAILING_GROUP_ROWS_AT_BOTTOM = True

# Group-row id behavior.
# Default requested behavior:
#   - use the id from the owner/entity row with the highest number of parcel entries;
#   - if parcel counts tie, use the id tied to the highest-acre parcel;
#   - if no usable id exists, fall back to the deterministic MA#<HASH> id.
# Legacy modes are kept for rollback/testing.
GROUP_ROW_ID_MODE = "best_owner_or_largest_acre_id"  # valid: "best_owner_or_largest_acre_id", "mailing_hash", "representative_id"
GROUP_ROW_ID_PREFIX = "MA#"

# Full-name label for the synthetic group rows.
# Default requested behavior produces:
#     MA#ABC123 | PRIMARY OWNER (+N others)
# Legacy modes are kept for rollback/testing:
#   - "mailing_address_group": MA#ABC123 | MAILING ADDRESS GROUP (+3 owners)
#   - "canonical_owner":       MA#ABC123 | OWNER NAME (+2)
GROUP_ROW_FULL_NAME_MODE = "primary_owner_then_others"  # valid: "primary_owner_then_others", "mailing_address_group", "canonical_owner"
GROUP_ROW_LABEL = "MAILING ADDRESS GROUP"
GROUP_ROW_OWNER_NAME_SEPARATOR = " | "
GROUP_ROW_OWNER_NAME_LIMIT = 50  # legacy only; not used by the default summarized-name mode
GROUP_ROW_FULL_NAME_MAX_LENGTH = 150  # keeps synthetic group names short for CSV/database usage

# Optional compatibility with files that already have split-name columns.
# These columns are updated only if they already exist in the input file.
SET_OPTIONAL_NAME_COLUMNS_ON_GROUP_ROW = True
OPTIONAL_FIRST_NAME_COLUMN = "first_name_1"
OPTIONAL_LAST_NAME_COLUMN = "last_name_1"
GROUP_ROW_LAST_NAME_LABEL = "DEDUP ROW"

# Grouping behavior.
# False matches the original script more closely: group by the visible mailing_address value.
# True groups values after trimming whitespace and uppercasing/collapsing spaces.
NORMALIZE_MAILING_GROUP_KEYS = False

# Minimal terminal output.
QUIET = False

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm"}
TEMP_COLUMNS = {"_mail_key", "__original_order"}


# =============================================================================
# Logging and basic helpers
# =============================================================================

def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def log(message: str, verbose: bool = True) -> None:
    if verbose:
        print(message, flush=True)


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    try:
        if isinstance(value, float) and math.isnan(value):
            return True
    except Exception:
        pass
    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text.lower() in {"", "nan", "none", "null", "n/a", "na", "--", "unknown", "not available"}


def clean_cell(value: Any) -> str:
    """Convert a cell value to a clean string for CSV/list output."""
    if is_missing(value):
        return ""

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float) and not isinstance(value, bool):
        if math.isnan(value):
            return ""
        if value.is_integer():
            return str(int(value))
        return ("%f" % value).rstrip("0").rstrip(".")

    text = str(value).replace("\n", " ").replace("\t", " ").strip()
    text = re.sub(r"\s+", " ", text)

    # Clean common Excel numeric-string artifacts such as 2024.0 or 78666.0.
    if re.fullmatch(r"-?\d+\.0", text):
        text = text[:-2]

    return text


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def parse_number(value: Any) -> Optional[float]:
    if is_missing(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            if math.isnan(float(value)):
                return None
        except Exception:
            pass
        return float(value)
    text = clean_cell(value).replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def format_number_for_list(value: Any) -> str:
    parsed = parse_number(value)
    if parsed is None:
        return ""
    if float(parsed).is_integer():
        return str(int(parsed))
    return ("%.12f" % parsed).rstrip("0").rstrip(".")


# =============================================================================
# List parsing and serialization helpers
# =============================================================================

def _literal_to_list(value: Any) -> Optional[List[Any]]:
    """Return a list when value is a JSON/Python list-like value; otherwise None."""
    if isinstance(value, list):
        return value
    if isinstance(value, (tuple, set)):
        return list(value)
    if is_missing(value):
        return []

    text = clean_cell(value)
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        # Try JSON first, then Python literal syntax.
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, (tuple, set)):
                return list(parsed)
            return [parsed]
        except Exception:
            return None

    return None


def parse_list_cell(value: Any, allow_pipe: bool = False) -> List[str]:
    """
    Parse a cell that may contain a JSON list, Python-list string, actual list,
    or scalar value.

    Pipe splitting is optional because a scalar like "100 | 200" may represent
    multiple source values for one parcel id from the earlier id-dedupe step.
    """
    parsed = _literal_to_list(value)
    if parsed is not None:
        return [clean_cell(item) for item in parsed]

    text = clean_cell(value)
    if not text:
        return []

    if allow_pipe and PIPE_SEPARATOR in text:
        return [part.strip() for part in text.split(PIPE_SEPARATOR)]

    return [text]


def parse_aligned_list_cell(value: Any, target_len: int) -> List[str]:
    """Parse an aligned list column and split pipes only when it clearly matches target length."""
    parsed = _literal_to_list(value)
    if parsed is not None:
        return [clean_cell(item) for item in parsed]

    text = clean_cell(value)
    if not text:
        return []

    if target_len > 1 and PIPE_SEPARATOR in text:
        parts = [part.strip() for part in text.split(PIPE_SEPARATOR)]
        if len(parts) == target_len:
            return parts

    return [text]


def align_list(values: List[str], target_len: int, fill_value: str = "") -> List[str]:
    out = list(values)
    if len(out) < target_len:
        out.extend([fill_value] * (target_len - len(out)))
    elif len(out) > target_len:
        out = out[:target_len]
    return out


def serialize_list(values: List[Any]) -> str:
    cleaned = [clean_cell(value) for value in values]
    if LIST_OUTPUT_FORMAT.lower() == "pipe":
        return PIPE_SEPARATOR.join(cleaned)
    return json.dumps(cleaned, ensure_ascii=False)


def serialize_acres_list(values: List[Any]) -> str:
    cleaned = [format_number_for_list(value) for value in values]
    if LIST_OUTPUT_FORMAT.lower() == "pipe":
        return PIPE_SEPARATOR.join(cleaned)
    return json.dumps(cleaned, ensure_ascii=False)


def normalize_id(value: Any) -> str:
    text = clean_cell(value)
    if not text:
        return ""
    try:
        numeric = float(text.replace(",", ""))
        if numeric.is_integer():
            return str(int(numeric))
        return ("%.12f" % numeric).rstrip("0").rstrip(".")
    except Exception:
        return text


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        key = clean_cell(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


# =============================================================================
# Mailing-address helpers
# =============================================================================

def is_blank_mailing_address(value: Any) -> bool:
    return is_missing(value)


def normalize_mailing_for_grouping(value: Any) -> str:
    text = clean_cell(value)
    if not NORMALIZE_MAILING_GROUP_KEYS:
        return text
    text = text.upper()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_mailing_for_hash(value: Any) -> str:
    text = clean_cell(value).upper()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^A-Z0-9 ]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def mailing_hash(value: Any, n: int = 6) -> str:
    normalized = normalize_mailing_for_hash(value)
    if not normalized:
        return "0" * n
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest().upper()[:n]


def format_synthetic_mailing_id(mailing_address: Any) -> str:
    """Return a deterministic synthetic id for a mailing-address group row."""
    return f"{GROUP_ROW_ID_PREFIX}{mailing_hash(mailing_address, n=6)}"


def enforce_max_text_length(text: str, max_length: int) -> str:
    """Trim text to max_length while keeping the end/suffix readable."""
    text = clean_cell(text)
    try:
        max_length = int(max_length)
    except Exception:
        max_length = 150

    if max_length <= 0 or len(text) <= max_length:
        return text

    if max_length <= 3:
        return text[:max_length]

    return text[: max_length - 3].rstrip() + "..."


def format_primary_owner_summary_name(
    code: str,
    primary: str,
    other_owner_count: int,
) -> str:
    """Build: MA#ABC123 | PRIMARY OWNER (+N others), capped at configured length."""
    try:
        max_length = int(GROUP_ROW_FULL_NAME_MAX_LENGTH)
    except Exception:
        max_length = 150

    prefix = f"MA#{code} | "
    if other_owner_count > 0:
        plural = "other" if other_owner_count == 1 else "others"
        suffix = f" (+{other_owner_count} {plural})"
    else:
        suffix = ""

    available_for_primary = max_length - len(prefix) - len(suffix)
    if available_for_primary > 0 and len(primary) > available_for_primary:
        if available_for_primary <= 3:
            primary = primary[:available_for_primary]
        else:
            primary = primary[: available_for_primary - 3].rstrip() + "..."

    return enforce_max_text_length(f"{prefix}{primary}{suffix}", max_length)


def format_full_name_for_mailing_rollup(
    mailing_address: Any,
    primary_full_name: Any,
    owner_names: List[str],
) -> str:
    """Return the full_name value for the synthetic mailing-address group row."""
    code = mailing_hash(mailing_address, n=6)
    mode = clean_cell(GROUP_ROW_FULL_NAME_MODE).lower()

    primary = clean_cell(primary_full_name)
    if not primary:
        primary = "UNKNOWN OWNER"

    ordered_names = unique_preserve_order(
        [name for name in [primary, *owner_names] if clean_cell(name)]
    )
    if not ordered_names:
        ordered_names = [primary]

    if mode == "primary_owner_then_others":
        # Requested compact label:
        #   MA#ABC123 | PRIMARY OWNER (+N others)
        # We do not include every other owner name in the full_name field, because those
        # names can make the cell too long for practical CSV/database usage.
        other_owner_count = max(0, len(ordered_names) - 1)
        return format_primary_owner_summary_name(code, primary, other_owner_count)

    if mode == "canonical_owner":
        extra = max(0, len(ordered_names) - 1)
        if extra > 0:
            return enforce_max_text_length(f"MA#{code} | {primary} (+{extra})", GROUP_ROW_FULL_NAME_MAX_LENGTH)
        return enforce_max_text_length(f"MA#{code} | {primary}", GROUP_ROW_FULL_NAME_MAX_LENGTH)

    label = clean_cell(GROUP_ROW_LABEL) or "MAILING ADDRESS GROUP"
    distinct_names_count = len(ordered_names)
    if distinct_names_count > 1:
        return enforce_max_text_length(f"MA#{code} | {label} (+{distinct_names_count} owners)", GROUP_ROW_FULL_NAME_MAX_LENGTH)
    return enforce_max_text_length(f"MA#{code} | {label}", GROUP_ROW_FULL_NAME_MAX_LENGTH)

def row_max_parcel_acres(row: pd.Series) -> float:
    acres_values = [parse_number(x) for x in parse_list_cell(row.get(COL_ACRES_LIST, ""), allow_pipe=True)]
    acres_numeric = [x for x in acres_values if x is not None]
    if acres_numeric:
        return float(max(acres_numeric))
    parsed = parse_number(row.get(COL_ACRES, ""))
    return float(parsed or 0.0)


def row_total_parcel_acres(row: pd.Series) -> float:
    """Return the sum of parcel acres represented by one owner/entity row."""
    total = 0.0
    for entry in row_to_parcel_entries(row):
        parsed = parse_number(entry.get(COL_ACRES, ""))
        if parsed is not None:
            total += parsed
    if total > 0:
        return float(total)
    parsed = parse_number(row.get(COL_ACRES, ""))
    return float(parsed or 0.0)


def owner_parcel_count(row: pd.Series) -> int:
    """Return how many parcels an owner/entity row represents."""
    parsed_lands = parse_number(row.get(COL_LANDS_COUNT, ""))
    if parsed_lands is not None and parsed_lands > 0:
        return int(round(parsed_lands))

    entries = row_to_parcel_entries(row)
    if entries:
        return len(entries)

    if not is_missing(row.get(COL_ID, "")):
        return 1
    return 0


def best_parcel_id_and_acres_for_row(row: pd.Series) -> Tuple[str, float]:
    """Return the id/acres pair for the largest-acre parcel inside one owner/entity row."""
    fallback_id = normalize_id(row.get(COL_ID, ""))
    fallback_acres = parse_number(row.get(COL_ACRES, "")) or 0.0

    best_id = fallback_id
    best_acres = float(fallback_acres)

    for entry in row_to_parcel_entries(row):
        parcel_id = normalize_id(entry.get(COL_ID, ""))
        acres = parse_number(entry.get(COL_ACRES, "")) or 0.0
        if acres > best_acres:
            best_acres = float(acres)
            best_id = parcel_id
        elif acres == best_acres and not best_id and parcel_id:
            best_id = parcel_id

    return clean_cell(best_id), float(best_acres)


def first_nonblank_parcel_id_for_row(row: pd.Series) -> str:
    """Return the first usable parcel id from id, then id_list."""
    direct_id = normalize_id(row.get(COL_ID, ""))
    if direct_id:
        return direct_id
    for entry in row_to_parcel_entries(row):
        parcel_id = normalize_id(entry.get(COL_ID, ""))
        if parcel_id:
            return parcel_id
    return ""


def select_primary_owner_for_mailing_group(group_ordered: pd.DataFrame) -> Dict[str, Any]:
    """
    Select the owner/entity row that should identify the mailing-address group.

    Priority:
      1. Owner/entity row with the most parcel entries.
      2. If parcel counts tie, row containing the highest-acre parcel.
      3. If still tied, row with the largest total acreage.
      4. If still tied, original input order.
    """
    row_infos: List[Dict[str, Any]] = []

    for ordinal, (idx, row) in enumerate(group_ordered.iterrows()):
        parcel_count = owner_parcel_count(row)
        best_parcel_id, best_parcel_acres = best_parcel_id_and_acres_for_row(row)
        total_acres = row_total_parcel_acres(row)
        direct_id = first_nonblank_parcel_id_for_row(row)
        row_infos.append(
            {
                "index": idx,
                "ordinal": ordinal,
                "full_name": clean_cell(row.get(COL_FULL_NAME, "")),
                "parcel_count": int(parcel_count),
                "best_parcel_id": clean_cell(best_parcel_id),
                "best_parcel_acres": float(best_parcel_acres),
                "total_acres": float(total_acres),
                "direct_id": clean_cell(direct_id),
            }
        )

    if not row_infos:
        return {
            "index": None,
            "full_name": "",
            "representative_id": "",
            "parcel_count": 0,
            "best_parcel_acres": 0.0,
            "selection_rule": "no_rows",
            "all_counts_equal": True,
        }

    counts = [info["parcel_count"] for info in row_infos]
    all_counts_equal = len(set(counts)) <= 1
    max_count = max(counts)
    max_count_tie_count = sum(1 for count in counts if count == max_count)

    row_infos_sorted = sorted(
        row_infos,
        key=lambda info: (
            -info["parcel_count"],
            -info["best_parcel_acres"],
            -info["total_acres"],
            info["ordinal"],
        ),
    )
    selected = row_infos_sorted[0]

    # If counts tie, the id should come from the highest-acre parcel. Otherwise,
    # use the selected owner/entity row id, falling back to that row's largest parcel id.
    if all_counts_equal or max_count_tie_count > 1:
        representative_id = selected.get("best_parcel_id") or selected.get("direct_id") or ""
        selection_rule = "highest_parcel_acres_tie_break"
    else:
        representative_id = selected.get("direct_id") or selected.get("best_parcel_id") or ""
        selection_rule = "highest_owner_parcel_count"

    selected = dict(selected)
    selected["representative_id"] = clean_cell(representative_id)
    selected["selection_rule"] = selection_rule
    selected["all_counts_equal"] = all_counts_equal
    selected["max_count_tie_count"] = max_count_tie_count
    return selected


# =============================================================================
# File helpers
# =============================================================================

def read_data_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return clean_dataframe(pd.read_csv(path, dtype=object, low_memory=False))
    if suffix in {".xlsx", ".xlsm"}:
        return clean_dataframe(pd.read_excel(path, dtype=object, engine="openpyxl"))
    if suffix == ".xls":
        return clean_dataframe(pd.read_excel(path, dtype=object))
    raise ValueError(f"Unsupported file type: {suffix} for file {path}")


def write_output_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_folder(path.parent)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def find_data_files(folder: Path) -> List[Path]:
    if not folder.exists() or not folder.is_dir():
        return []
    return sorted(
        p for p in folder.iterdir()
        if p.is_file()
        and p.suffix.lower() in SUPPORTED_SUFFIXES
        and not p.name.startswith("~$")
        and not p.name.lower().endswith("_summary.csv")
        and p.name.lower() != SUMMARY_CSV_NAME.lower()
    )


def find_county_folders(base_folder: Path) -> List[Path]:
    county_folders = sorted([p for p in base_folder.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
    if county_folders:
        return county_folders

    # Fallback: allow INPUT_ROOT itself to directly contain CSV/XLSX files.
    if find_data_files(base_folder):
        return [base_folder]

    return []


def output_path_for(input_file: Path, county_folder: Path, input_root: Path, output_root: Path) -> Path:
    if county_folder.resolve() == input_root.resolve():
        county_name = input_file.stem
    else:
        county_name = county_folder.name
    return output_root / county_name / f"{county_name}{OUTPUT_FILE_SUFFIX}{OUTPUT_SUFFIX}"


# =============================================================================
# Rollup logic
# =============================================================================

def check_required_columns(df: pd.DataFrame, input_file: Path) -> None:
    missing = [col for col in REQUIRED_COLS if col not in df.columns]
    if missing:
        raise KeyError(
            f"Missing required column(s) in {input_file}: {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def ensure_aligned_list_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
    out = df.copy()
    present: List[str] = []
    created: List[str] = []

    for col in ALIGNED_LIST_COLUMNS:
        if col in out.columns:
            present.append(col)
        elif CREATE_MISSING_ALIGNED_LIST_COLUMNS:
            out[col] = ""
            created.append(col)
        else:
            raise KeyError(f"Missing required aligned-list column: {col}")

    return out, present, created


def build_mailing_keys(df: pd.DataFrame) -> List[str]:
    keys: List[str] = []
    for index, addr in enumerate(df[COL_MAILING].tolist()):
        if is_blank_mailing_address(addr):
            keys.append(f"__MISSING_MAILING__{index}")
        else:
            keys.append(normalize_mailing_for_grouping(addr))
    return keys


def row_to_parcel_entries(row: pd.Series) -> List[Dict[str, str]]:
    """
    Expand a row from owner dedupe into parcel-level entries.

    Each returned entry has the same keys:
        id, acres, market_value_list, owner_tax_year_list, deed_date_list
    """
    id_values = [normalize_id(x) for x in parse_list_cell(row.get(COL_ID_LIST, ""), allow_pipe=True)]
    if not id_values and not is_missing(row.get(COL_ID, "")):
        id_values = [normalize_id(row.get(COL_ID, ""))]

    acres_values = [format_number_for_list(x) for x in parse_list_cell(row.get(COL_ACRES_LIST, ""), allow_pipe=True)]
    if not acres_values and not is_missing(row.get(COL_ACRES, "")):
        acres_values = [format_number_for_list(row.get(COL_ACRES, ""))]

    base_len = max(len(id_values), len(acres_values), 1)

    aligned_values_by_col: Dict[str, List[str]] = {}
    for col in ALIGNED_LIST_COLUMNS:
        values = parse_aligned_list_cell(row.get(col, ""), target_len=base_len)
        aligned_values_by_col[col] = values
        base_len = max(base_len, len(values))

    id_values = align_list(id_values, base_len)
    acres_values = align_list(acres_values, base_len)
    for col in ALIGNED_LIST_COLUMNS:
        aligned_values_by_col[col] = align_list(aligned_values_by_col[col], base_len)

    entries: List[Dict[str, str]] = []
    for idx in range(base_len):
        entry: Dict[str, str] = {
            COL_ID: id_values[idx],
            COL_ACRES: acres_values[idx],
        }
        for col in ALIGNED_LIST_COLUMNS:
            entry[col] = aligned_values_by_col[col][idx]
        entries.append(entry)

    return entries


def merge_parcel_entries(entries: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], int, int]:
    """
    Deduplicate parcel entries by id while preserving order and aligned values.

    Returns:
      merged_entries, duplicate_id_entries_removed, blank_id_entries
    """
    if not DEDUPLICATE_IDS_WITHIN_MAILING_GROUP:
        blank_count = sum(1 for entry in entries if not clean_cell(entry.get(COL_ID, "")))
        return entries, 0, blank_count

    seen_ids = set()
    merged: List[Dict[str, str]] = []
    duplicates_removed = 0
    blank_count = 0

    for pos, entry in enumerate(entries):
        parcel_id = normalize_id(entry.get(COL_ID, ""))
        if not parcel_id:
            blank_count += 1
            if KEEP_BLANK_ID_ENTRIES:
                merged.append(entry)
            else:
                duplicates_removed += 1
            continue

        key = parcel_id.lower()
        if key in seen_ids:
            duplicates_removed += 1
            continue
        seen_ids.add(key)
        entry[COL_ID] = parcel_id
        merged.append(entry)

    return merged, duplicates_removed, blank_count


def entries_to_lists(entries: List[Dict[str, str]]) -> Dict[str, List[str]]:
    result = {
        COL_ID_LIST: [clean_cell(entry.get(COL_ID, "")) for entry in entries],
        COL_ACRES_LIST: [format_number_for_list(entry.get(COL_ACRES, "")) for entry in entries],
    }
    for col in ALIGNED_LIST_COLUMNS:
        result[col] = [clean_cell(entry.get(col, "")) for entry in entries]
    return result


def sum_acres_from_entries(entries: List[Dict[str, str]]) -> float:
    total = 0.0
    for entry in entries:
        parsed = parse_number(entry.get(COL_ACRES, ""))
        if parsed is not None:
            total += parsed
    return total


def clean_representative_core_values(row: pd.Series) -> pd.Series:
    out = row.copy()
    for col in [COL_ID, COL_FULL_NAME, COL_LEGAL_ADDRESS, COL_MAILING, COL_CITY, COL_ZIP, COL_STATE]:
        if col in out.index:
            out[col] = clean_cell(out[col])
    if COL_ACRES in out.index:
        out[COL_ACRES] = format_number_for_list(out[COL_ACRES])
    return out


def rollup_by_mailing_address_for_dataframe(df: pd.DataFrame, input_file: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    check_required_columns(df, input_file=input_file)
    df, aligned_columns_present, aligned_columns_created = ensure_aligned_list_columns(df)

    before_rows = len(df)
    if before_rows == 0:
        summary = {
            "status": "ok_empty",
            "input_file": str(input_file),
            "total_rows_before": 0,
            "rows_after": 0,
            "source_rows_after": 0,
            "mailing_group_rows_created": 0,
            "removed_mailing_duplicate_rows": 0,
            "duplicate_mailing_groups": 0,
            "source_rows_with_mailing_address_cleared": 0,
            "blank_mailing_rows_kept_unmerged": 0,
            "unique_nonblank_mailing_rows_kept_as_is": 0,
            "max_lands_for_one_mailing_address": 0,
            "duplicate_id_entries_removed_inside_mailing_groups": 0,
            "blank_id_entries": 0,
            "aligned_list_columns_present": ", ".join(aligned_columns_present),
            "aligned_list_columns_created_blank": ", ".join(aligned_columns_created),
            "generated_at_utc": utc_now_text(),
        }
        return df.copy(), summary

    work = df.copy()
    work["__original_order"] = range(len(work))
    work["_mail_key"] = build_mailing_keys(work)

    # Start with all original/source rows. We will only clear mailing_address on the
    # source rows that belong to a repeated non-blank mailing-address group.
    source_work = work.copy()
    mailing_group_rows: List[pd.Series] = []

    duplicate_mailing_groups = 0
    blank_mailing_rows_kept_unmerged = 0
    unique_nonblank_mailing_rows_kept_as_is = 0
    source_rows_with_mailing_cleared = 0
    max_lands_for_one_mailing = 0
    duplicate_id_entries_removed_total = 0
    blank_id_entries_total = 0
    total_parcel_entries_after_merge = 0

    for _, group in work.groupby("_mail_key", sort=False):
        group_ordered = group.sort_values("__original_order")
        mail_key = clean_cell(group_ordered["_mail_key"].iloc[0])
        is_blank_mail_group = mail_key.startswith("__MISSING_MAILING__")

        if is_blank_mail_group:
            # Blank mailing-address rows are not grouped together and no synthetic row is added.
            blank_mailing_rows_kept_unmerged += len(group_ordered)
            continue

        if len(group_ordered) <= 1:
            # Unique non-blank mailing address: keep the original row as-is and do not add a rollup row.
            unique_nonblank_mailing_rows_kept_as_is += len(group_ordered)
            continue

        duplicate_mailing_groups += 1

        if CLEAR_MAILING_ADDRESS_ON_SOURCE_ROWS_IN_DUPLICATE_GROUPS:
            source_work.loc[group_ordered.index, COL_MAILING] = ""
            source_rows_with_mailing_cleared += len(group_ordered)

        # Primary owner/entity row supplies city/state/zip and other non-list fields for
        # the synthetic row. The selection rule favors the owner with the most parcels;
        # ties use the highest-acre parcel. The legal_address is intentionally blanked
        # later because the synthetic row represents the mailing-address group, not one
        # parcel's situs/legal address.
        primary_info = select_primary_owner_for_mailing_group(group_ordered)
        canonical_idx = primary_info.get("index")
        if canonical_idx is None:
            canonical_idx = group_ordered.index[0]
        canonical_row = clean_representative_core_values(group_ordered.loc[canonical_idx])

        parcel_entries: List[Dict[str, str]] = []
        for _, row in group_ordered.iterrows():
            parcel_entries.extend(row_to_parcel_entries(row))

        merged_entries, duplicate_ids_removed, blank_id_entries = merge_parcel_entries(parcel_entries)
        duplicate_id_entries_removed_total += duplicate_ids_removed
        blank_id_entries_total += blank_id_entries
        total_parcel_entries_after_merge += len(merged_entries)
        max_lands_for_one_mailing = max(max_lands_for_one_mailing, len(merged_entries))

        list_values = entries_to_lists(merged_entries)

        out = canonical_row.copy()

        names = [clean_cell(value) for value in group_ordered[COL_FULL_NAME].tolist()]
        distinct_names = unique_preserve_order([name for name in names if name])
        primary_name = clean_cell(primary_info.get("full_name", "")) or clean_cell(canonical_row.get(COL_FULL_NAME, ""))
        ordered_names = unique_preserve_order([primary_name] + [name for name in distinct_names if name != primary_name])

        mailing_address_value = canonical_row.get(COL_MAILING, "")
        out[COL_FULL_NAME] = format_full_name_for_mailing_rollup(
            mailing_address=mailing_address_value,
            primary_full_name=primary_name,
            owner_names=ordered_names,
        )

        group_row_id_mode = clean_cell(GROUP_ROW_ID_MODE).lower()
        if group_row_id_mode == "representative_id":
            out[COL_ID] = clean_cell(canonical_row.get(COL_ID, "")) or format_synthetic_mailing_id(mailing_address_value)
        elif group_row_id_mode == "mailing_hash":
            out[COL_ID] = format_synthetic_mailing_id(mailing_address_value)
        else:
            out[COL_ID] = clean_cell(primary_info.get("representative_id", "")) or format_synthetic_mailing_id(mailing_address_value)

        # Requested behavior: the synthetic mailing-address group row keeps the shared mailing
        # address, but does not keep any individual legal/situs address.
        out[COL_MAILING] = clean_cell(mailing_address_value)
        out[COL_LEGAL_ADDRESS] = ""

        # Optional compatibility for downstream tables that already have split-name columns.
        if SET_OPTIONAL_NAME_COLUMNS_ON_GROUP_ROW:
            group_short_label = clean_cell(primary_name) or f"{GROUP_ROW_LABEL} {mailing_hash(mailing_address_value, n=6)}"
            if OPTIONAL_FIRST_NAME_COLUMN in out.index:
                out[OPTIONAL_FIRST_NAME_COLUMN] = group_short_label
            if OPTIONAL_LAST_NAME_COLUMN in out.index:
                out[OPTIONAL_LAST_NAME_COLUMN] = GROUP_ROW_LAST_NAME_LABEL

        out[COL_ACRES_LIST] = serialize_acres_list(list_values[COL_ACRES_LIST])
        out[COL_ID_LIST] = serialize_list(list_values[COL_ID_LIST])
        for col in ALIGNED_LIST_COLUMNS:
            out[col] = serialize_list(list_values[col])

        total_acres = sum_acres_from_entries(merged_entries)
        if total_acres > 0:
            out[COL_ACRES] = format_number_for_list(total_acres)
        else:
            fallback_total = group_ordered[COL_ACRES].map(parse_number).fillna(0.0).sum()
            out[COL_ACRES] = format_number_for_list(fallback_total)

        out[COL_LANDS_COUNT] = int(len(merged_entries))
        out = out.drop(labels=[col for col in TEMP_COLUMNS if col in out.index])
        mailing_group_rows.append(out)

    # Remove internal helper columns from the retained original/source rows.
    source_df = source_work.drop(columns=[col for col in TEMP_COLUMNS if col in source_work.columns], errors="ignore")

    group_df = pd.DataFrame(mailing_group_rows)

    if APPEND_MAILING_GROUP_ROWS_AT_BOTTOM:
        out_df = pd.concat([source_df, group_df], ignore_index=True, sort=False)
    else:
        # Current default is bottom-appending. This fallback still keeps all source rows and group rows.
        out_df = pd.concat([source_df, group_df], ignore_index=True, sort=False)

    # Friendly column order while preserving any extra columns from the input.
    preferred_order = [
        COL_ID,
        COL_FULL_NAME,
        COL_LEGAL_ADDRESS,
        COL_MAILING,
        COL_ACRES,
        COL_CITY,
        COL_ZIP,
        COL_STATE,
        COL_LANDS_COUNT,
        COL_ID_LIST,
        COL_ACRES_LIST,
        *ALIGNED_LIST_COLUMNS,
    ]
    ordered_cols = [col for col in preferred_order if col in out_df.columns]
    remaining_cols = [col for col in out_df.columns if col not in ordered_cols and col not in TEMP_COLUMNS]
    out_df = out_df[ordered_cols + remaining_cols]

    if COL_LANDS_COUNT in out_df.columns:
        out_df[COL_LANDS_COUNT] = pd.to_numeric(out_df[COL_LANDS_COUNT], errors="coerce").fillna(0).astype(int)

    after_rows = len(out_df)
    mailing_group_rows_created = len(mailing_group_rows)

    summary = {
        "status": "ok",
        "input_file": str(input_file),
        "total_rows_before": before_rows,
        "rows_after": after_rows,
        "source_rows_after": len(source_df),
        "mailing_group_rows_created": mailing_group_rows_created,
        "removed_mailing_duplicate_rows": 0,
        "mailing_groups": duplicate_mailing_groups,
        "duplicate_mailing_groups": duplicate_mailing_groups,
        "source_rows_with_mailing_address_cleared": source_rows_with_mailing_cleared,
        "blank_mailing_rows_kept_unmerged": blank_mailing_rows_kept_unmerged,
        "unique_nonblank_mailing_rows_kept_as_is": unique_nonblank_mailing_rows_kept_as_is,
        "total_parcel_entries_after_merge": total_parcel_entries_after_merge,
        "max_lands_for_one_mailing_address": max_lands_for_one_mailing,
        "duplicate_id_entries_removed_inside_mailing_groups": duplicate_id_entries_removed_total,
        "blank_id_entries": blank_id_entries_total,
        "group_column": COL_MAILING,
        "blank_mailing_rule": "blank mailing_address rows are not merged together",
        "row_output_rule": "original rows are retained; duplicate source rows have mailing_address cleared; one synthetic mailing-address group row keeps the mailing address",
        "group_row_legal_address_rule": "synthetic mailing-address group rows have blank legal_address",
        "group_row_id_rule": GROUP_ROW_ID_MODE,
        "group_row_full_name_rule": GROUP_ROW_FULL_NAME_MODE,
        "representative_row_rule": "owner/entity with most parcels; ties use highest-acre parcel; used to populate id, full_name order, city/state/zip and non-list fields on synthetic row",
        "list_order_rule": "owner-row input order, then parcel order inside each row",
        "list_output_format": LIST_OUTPUT_FORMAT,
        "aligned_list_columns_present": ", ".join(aligned_columns_present),
        "aligned_list_columns_created_blank": ", ".join(aligned_columns_created),
        "generated_at_utc": utc_now_text(),
    }
    return out_df, summary


# =============================================================================
# Folder processing
# =============================================================================

COUNTY_FILE_SUMMARIES: List[Dict[str, Any]] = []


def process_county_folder(county_folder: Path, input_root: Path, output_root: Path, verbose: bool = True) -> Dict[str, Any]:
    county_name = county_folder.name
    data_files = find_data_files(county_folder)

    stats: Dict[str, Any] = {
        "county": county_name,
        "files": 0,
        "before": 0,
        "after": 0,
        "removed": 0,
        "mailing_group_rows_created": 0,
        "source_rows_with_mailing_address_cleared": 0,
        "duplicate_mailing_groups": 0,
        "blank_mailing_rows_kept_unmerged": 0,
        "max_lands_for_one_mailing_address": 0,
        "duplicate_id_entries_removed_inside_mailing_groups": 0,
        "errors": 0,
    }

    log(f"\n[COUNTY] {county_name} ({len(data_files):,} file(s) found)", verbose)

    if not data_files:
        log("[SKIP] No supported CSV/Excel files found.", verbose)
        return stats

    for file_number, data_path in enumerate(data_files, start=1):
        output_file = output_path_for(data_path, county_folder, input_root, output_root)

        try:
            log(f"  [FILE {file_number:,}/{len(data_files):,}] {data_path.name}", verbose)
            log(f"    [READ] {data_path}", verbose)

            df = read_data_file(data_path)
            log(f"    [ROWS BEFORE] {len(df):,}", verbose)

            rolled_df, summary = rollup_by_mailing_address_for_dataframe(df, input_file=data_path)
            summary["county"] = county_name
            summary["output_file"] = str(output_file)

            write_output_csv(rolled_df, output_file)

            stats["files"] += 1
            stats["before"] += int(summary.get("total_rows_before", 0) or 0)
            stats["after"] += int(summary.get("rows_after", 0) or 0)
            stats["removed"] += int(summary.get("removed_mailing_duplicate_rows", 0) or 0)
            stats["mailing_group_rows_created"] += int(summary.get("mailing_group_rows_created", 0) or 0)
            stats["source_rows_with_mailing_address_cleared"] += int(summary.get("source_rows_with_mailing_address_cleared", 0) or 0)
            stats["duplicate_mailing_groups"] += int(summary.get("duplicate_mailing_groups", 0) or 0)
            stats["blank_mailing_rows_kept_unmerged"] += int(summary.get("blank_mailing_rows_kept_unmerged", 0) or 0)
            stats["duplicate_id_entries_removed_inside_mailing_groups"] += int(summary.get("duplicate_id_entries_removed_inside_mailing_groups", 0) or 0)
            stats["max_lands_for_one_mailing_address"] = max(
                int(stats["max_lands_for_one_mailing_address"]),
                int(summary.get("max_lands_for_one_mailing_address", 0) or 0),
            )

            COUNTY_FILE_SUMMARIES.append(summary)

            log(f"    [DUPLICATE MAILING GROUPS] {summary.get('duplicate_mailing_groups', 0):,}", verbose)
            log(f"    [GROUP ROWS CREATED] {summary.get('mailing_group_rows_created', 0):,}", verbose)
            log(f"    [SOURCE MAILING CLEARED] {summary.get('source_rows_with_mailing_address_cleared', 0):,}", verbose)
            log(f"    [MAX LANDS / MAILING GROUP] {summary.get('max_lands_for_one_mailing_address', 0):,}", verbose)
            log(f"    [ROWS AFTER] {summary.get('rows_after', 0):,}", verbose)
            log(f"    [REMOVED ROWS] {summary.get('removed_mailing_duplicate_rows', 0):,}", verbose)
            if summary.get("aligned_list_columns_created_blank"):
                log(f"    [WARN] Missing aligned-list columns created as blank: {summary['aligned_list_columns_created_blank']}", verbose)
            if int(summary.get("duplicate_id_entries_removed_inside_mailing_groups", 0) or 0) > 0:
                log(f"    [INFO] Duplicate parcel-id entries removed inside mailing groups: {summary['duplicate_id_entries_removed_inside_mailing_groups']:,}", verbose)
            log(f"    [SAVE] {output_file}", verbose)

        except Exception as exc:
            stats["errors"] += 1
            error_summary = {
                "status": "error",
                "county": county_name,
                "input_file": str(data_path),
                "output_file": str(output_file),
                "total_rows_before": 0,
                "rows_after": 0,
                "removed_mailing_duplicate_rows": 0,
                "duplicate_mailing_groups": 0,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=5),
                "generated_at_utc": utc_now_text(),
            }
            COUNTY_FILE_SUMMARIES.append(error_summary)
            log(f"    [ERROR] Failed to process {data_path.name}: {exc}", True)

    log(
        f"[COUNTY DONE] {county_name}: {stats['files']:,} file(s), "
        f"{stats['before']:,} -> {stats['after']:,} rows "
        f"(group rows created {stats['mailing_group_rows_created']:,}; "
        f"source mailing cleared {stats['source_rows_with_mailing_address_cleared']:,}; "
        f"removed {stats['removed']:,}; errors {stats['errors']:,})",
        verbose,
    )

    return stats


def process_parent_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(f"Input root folder does not exist or is not a directory: {input_root}")

    ensure_folder(output_root)
    county_folders = find_county_folders(input_root)

    log("=" * 90, verbose)
    log("[START] Creating mailing-address group rows", verbose)
    log(f"[TIME] {now_text()}", verbose)
    log(f"[INPUT ROOT]  {input_root}", verbose)
    log(f"[OUTPUT ROOT] {output_root}", verbose)
    log(f"[GROUP BY]    {COL_MAILING}", verbose)
    log("[BLANK RULE]  blank mailing_address rows are not merged", verbose)
    log("[SOURCE ROWS] original rows are kept; duplicate source mailing_address values are cleared", verbose)
    log("[GROUP ROWS]  one synthetic row is appended for each repeated non-blank mailing_address", verbose)
    log("[GROUP ID]    group-row id uses owner with most parcels; ties use highest-acre parcel", verbose)
    log("[FULL NAME]   MA#hash | primary owner (+N others)", verbose)
    log("[LEGAL ADDR]  synthetic group rows use blank legal_address", verbose)
    log("[LIST ORDER]  id/acres/market/tax-year/deed-date/legal-acreage flags stay aligned", verbose)
    log(f"[COUNTIES]    {len(county_folders):,} folder(s) found", verbose)
    log("=" * 90, verbose)

    county_stats: List[Dict[str, Any]] = []

    for index, county_folder in enumerate(county_folders, start=1):
        log(f"\n[PROGRESS] County {index:,}/{len(county_folders):,}", verbose)
        stats = process_county_folder(
            county_folder=county_folder,
            input_root=input_root,
            output_root=output_root,
            verbose=verbose,
        )
        county_stats.append(stats)

    if WRITE_SUMMARY_CSV:
        summary_path = output_root / SUMMARY_CSV_NAME
        pd.DataFrame(COUNTY_FILE_SUMMARIES).to_csv(summary_path, index=False, encoding="utf-8-sig")
        log("\n" + "-" * 90, verbose)
        log(f"[SUMMARY CSV] {summary_path}", verbose)

    return county_stats


def main() -> None:
    input_root = Path(INPUT_ROOT).resolve()
    output_root = Path(OUTPUT_ROOT).resolve()
    verbose = not QUIET

    county_stats = process_parent_folder(input_root=input_root, output_root=output_root, verbose=verbose)

    total_counties = len(county_stats)
    total_files = sum(int(s.get("files", 0) or 0) for s in county_stats)
    total_before = sum(int(s.get("before", 0) or 0) for s in county_stats)
    total_after = sum(int(s.get("after", 0) or 0) for s in county_stats)
    total_removed = sum(int(s.get("removed", 0) or 0) for s in county_stats)
    total_group_rows_created = sum(int(s.get("mailing_group_rows_created", 0) or 0) for s in county_stats)
    total_source_rows_mailing_cleared = sum(int(s.get("source_rows_with_mailing_address_cleared", 0) or 0) for s in county_stats)
    total_duplicate_groups = sum(int(s.get("duplicate_mailing_groups", 0) or 0) for s in county_stats)
    total_blank_mailing_rows = sum(int(s.get("blank_mailing_rows_kept_unmerged", 0) or 0) for s in county_stats)
    total_duplicate_ids_removed = sum(int(s.get("duplicate_id_entries_removed_inside_mailing_groups", 0) or 0) for s in county_stats)
    total_errors = sum(int(s.get("errors", 0) or 0) for s in county_stats)
    max_lands = max([int(s.get("max_lands_for_one_mailing_address", 0) or 0) for s in county_stats] or [0])

    if verbose:
        print("\n" + "=" * 90)
        print("[DONE] Mailing-address group-row creation completed.")
        print(f"Total counties processed             : {total_counties:,}")
        print(f"Total files processed                : {total_files:,}")
        print(f"Total rows BEFORE                    : {total_before:,}")
        print(f"Total rows AFTER                     : {total_after:,}")
        print(f"Total rows REMOVED                   : {total_removed:,}")
        print(f"Mailing group rows created           : {total_group_rows_created:,}")
        print(f"Source mailing_address cells cleared : {total_source_rows_mailing_cleared:,}")
        print(f"Duplicate mailing groups found       : {total_duplicate_groups:,}")
        print(f"Blank mailing rows kept unmerged     : {total_blank_mailing_rows:,}")
        print(f"Duplicate id entries removed         : {total_duplicate_ids_removed:,}")
        print(f"Max lands for one mailing group      : {max_lands:,}")
        print(f"Files with errors                    : {total_errors:,}")
        print(f"Output root                          : {output_root}")
        print("=" * 90)


if __name__ == "__main__":
    main()
