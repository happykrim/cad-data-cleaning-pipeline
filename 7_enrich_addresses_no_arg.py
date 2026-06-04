#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Pipeline step 7: enrich address fields after university-row removal.

Recommended script name:
    7_enrich_addresses_no_arg.py

Run after:
    6_remove_university_rows_no_arg.py

What it does:
- Reads the county folders produced by 6_remove_university_rows_no_arg.py.
- Looks at the mailing_address column.
- Fills missing city, state, and zip values when they can be parsed from mailing_address.
- Keeps the full mailing_address intact. It only normalizes spacing and commas.
- Uses a ZIP-only fallback when city/state cannot be confidently parsed.
- Preserves all existing columns, including aligned parcel-list fields such as:
    id_list
    acres_list
    market_value_list
    owner_tax_year_list
    deed_date_list
    legal_acreage_filled_by_script
    Empty_Legal_Acreage
- Saves enriched files into a new output root while mirroring the county-folder structure.
- Prints clear progress while running.
- Writes a root-level summary CSV.

Requirements:
    pip install pandas openpyxl

No command-line arguments are required. Edit CONFIG values below if needed.
You can also override INPUT_ROOT and OUTPUT_ROOT with these environment variables:
    CAD_ENRICH_ADDRESSES_INPUT_ROOT
    CAD_ADDRESS_ENRICHMENT_INPUT_ROOT
    CAD_ENRICH_ADDRESSES_OUTPUT_ROOT
    CAD_ADDRESS_ENRICHMENT_OUTPUT_ROOT
    INPUT_ROOT
    OUTPUT_ROOT
"""

from __future__ import annotations

import os
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# This should match OUTPUT_ROOT from 6_remove_university_rows_no_arg.py.
DEFAULT_INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_7_no_excluded_owners"

# New pipeline output folder.
DEFAULT_OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_8_addresses_enriched"

INPUT_ROOT = (
    os.environ.get("CAD_ENRICH_ADDRESSES_INPUT_ROOT")
    or os.environ.get("CAD_ADDRESS_ENRICHMENT_INPUT_ROOT")
    or os.environ.get("INPUT_ROOT")
    or DEFAULT_INPUT_ROOT
)
OUTPUT_ROOT = (
    os.environ.get("CAD_ENRICH_ADDRESSES_OUTPUT_ROOT")
    or os.environ.get("CAD_ADDRESS_ENRICHMENT_OUTPUT_ROOT")
    or os.environ.get("OUTPUT_ROOT")
    or DEFAULT_OUTPUT_ROOT
)

# Source and target columns.
ADDRESS_COLUMN = "mailing_address"
CITY_COLUMN = "city"
STATE_COLUMN = "state"
ZIP_COLUMN = "zip"

# ZIP-only fallback state. This is used only when a ZIP is found but no state was
# confidently parsed and the state cell is blank.
DEFAULT_STATE = "TX"

# If True, existing city/state/zip cells are preserved and only blanks are filled.
# If False, parsed values can overwrite existing city/state/zip values.
FILL_ONLY_BLANK_TARGETS = True

# Keep mailing_address as the full address, but normalize whitespace and commas.
NORMALIZE_MAILING_ADDRESS = True

# Output behavior.
OUTPUT_FILE_SUFFIX = "_addresses_enriched"
OUTPUT_SUFFIX = ".csv"
WRITE_SUMMARY_CSV = True
SUMMARY_CSV_NAME = "enrich_addresses_summary.csv"

# If True, files with names like *_summary.csv or audit files are ignored.
SKIP_SUMMARY_AND_AUDIT_FILES = True

# Minimal terminal output.
QUIET = False

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm"}


# =============================================================================
# Address parsing settings
# =============================================================================

# Handles normal ZIPs, ZIP+4, and ZIP values that were accidentally read as 78666.0.
ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?(?:\.0)?\b")

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}

# For splitting city vs street.
STREET_SUFFIXES = {
    "RD", "ROAD", "DR", "DRIVE", "ST", "STREET", "LN", "LANE",
    "BLVD", "AV", "AVE", "AVENUE", "HWY", "HIGHWAY", "PKWY",
    "PL", "PLAZA", "CIR", "CIRCLE", "TRAIL", "TRL", "WAY",
    "CT", "COURT", "CV", "COVE", "TER", "TERRACE", "LOOP", "SPUR",
    "FM", "CR", "RANCH", "PIKE", "PASS", "RUN", "BND", "BEND",
    "SQ", "SQUARE", "FWY", "FREEWAY", "EXPY", "EXPRESSWAY", "RTE",
    "ROUTE", "XING", "CROSSING", "CTR", "CENTER", "CENTRE",
}

NON_ADDRESS_MARKERS = {
    "ATTN", "ATTN:", "C/O", "CO", "PO", "P.O.", "BOX", "APT", "STE",
    "SUITE", "UNIT", "#", "DEPT", "BLDG", "RM", "LOT", "TRLR",
}


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


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "null", "n/a", "na", "--", "unknown", "not available"}


def clean_cell(value: Any) -> str:
    if is_missing(value):
        return ""
    text = str(value).replace("\n", " ").replace("\t", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if re.fullmatch(r"-?\d+\.0", text):
        text = text[:-2]
    return text


def normalize_column_name(name: Any) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def find_column(df: pd.DataFrame, desired_name: str) -> Optional[str]:
    """Find a column by exact name first, then normalized name."""
    if desired_name in df.columns:
        return desired_name

    desired_lower = desired_name.strip().lower()
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    if desired_lower in lower_map:
        return lower_map[desired_lower]

    desired_normalized = normalize_column_name(desired_name)
    normalized_map = {normalize_column_name(c): c for c in df.columns}
    return normalized_map.get(desired_normalized)


def get_or_create_column(df: pd.DataFrame, desired_name: str) -> str:
    """Return an existing matching column or create the desired column."""
    found = find_column(df, desired_name)
    if found is not None:
        return found
    df[desired_name] = ""
    return desired_name


def pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


# =============================================================================
# Address parsing helpers
# =============================================================================

def normalize_address(text: Any) -> str:
    if is_missing(text):
        return ""
    t = str(text)
    t = t.replace("\n", " ").replace("\t", " ")
    t = t.replace("%", " ")
    t = re.sub(r"\s*,\s*", ", ", t)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip().strip(",")


def find_last_zip_span(s: str):
    last = None
    for m in ZIP_RE.finditer(s):
        last = m
    return last


def find_last_state_before(s: str, end_pos: int):
    last_state = None
    for m in re.finditer(r"\b([A-Z]{2})\b", s):
        if m.start() < end_pos and m.group(1) in US_STATES:
            last_state = m
    return last_state


def smart_title(text: str) -> str:
    if not text:
        return text
    keep_upper = {
        "PO", "BOX", "FM", "HWY", "US", "CR", "RTE",
        "N", "S", "E", "W", "NW", "NE", "SW", "SE",
        "C/O", "ATTN:", "DC",
    }
    out: List[str] = []
    for tok in text.split():
        clean_tok = tok.strip(",")
        if clean_tok.upper() in keep_upper:
            out.append(clean_tok.upper())
        else:
            out.append(clean_tok.capitalize())
    return " ".join(out)


def extract_city_and_street(prefix_orig: str) -> Tuple[str, str]:
    """
    Given the part before the state token, return (city, street).

    Heuristic:
    - Walk backwards from the end of the prefix.
    - Always include the last token as part of the city.
    - Keep adding tokens to the left while they do not look like numbers,
      street suffixes, or address markers.
    - Cap city length at three tokens to avoid swallowing too much of the street.
    """
    if not prefix_orig:
        return "", ""

    tokens_orig = prefix_orig.strip(" ,").split()
    tokens_up = [t.strip(",").upper() for t in tokens_orig]
    if not tokens_up:
        return "", prefix_orig.strip(" ,")

    i = len(tokens_up) - 1
    city_tokens = [tokens_orig[i]]
    i -= 1

    while (
        i >= 0
        and len(city_tokens) < 3
        and not tokens_up[i].isdigit()
        and tokens_up[i] not in STREET_SUFFIXES
        and tokens_up[i] not in NON_ADDRESS_MARKERS
    ):
        city_tokens.insert(0, tokens_orig[i])
        i -= 1

    street_tokens = tokens_orig[: i + 1]
    city = " ".join(city_tokens).strip(" ,")
    street = " ".join(street_tokens).strip(" ,")

    if not city:
        city = ""
        street = prefix_orig.strip(" ,")

    return city, street


def parse_us_address_line(addr: Any) -> Optional[Tuple[str, str, str, str]]:
    """Return (street, city, state, zip5) or None if not confident."""
    original = normalize_address(addr)
    if not original:
        return None

    work = original.upper()
    zip_m = find_last_zip_span(work)
    if not zip_m:
        return None

    zip5 = zip_m.group(1)
    state_m = find_last_state_before(work, zip_m.start())
    if not state_m:
        return None

    state = state_m.group(1)
    prefix_orig = original[: state_m.start()].strip(" ,")
    city, street = extract_city_and_street(prefix_orig)

    if city.upper() in STREET_SUFFIXES or city.upper() in NON_ADDRESS_MARKERS:
        city = ""

    return street, city, state, zip5


def find_zip_only(addr: Any) -> str:
    work = normalize_address(addr).upper()
    zip_m = find_last_zip_span(work)
    if zip_m:
        return zip_m.group(1)
    return ""


def set_target_value(df: pd.DataFrame, idx: Any, col: str, value: Any) -> Tuple[bool, bool]:
    """
    Set a parsed value in df and return (filled_or_changed, overwritten_existing).
    Existing values are preserved when FILL_ONLY_BLANK_TARGETS is True.
    """
    if is_missing(value):
        return False, False

    current = df.at[idx, col] if col in df.columns else ""
    current_missing = is_missing(current)

    if FILL_ONLY_BLANK_TARGETS and not current_missing:
        return False, False

    new_value = clean_cell(value)
    if new_value == "":
        return False, False

    overwritten = (not current_missing) and clean_cell(current) != new_value
    df.at[idx, col] = new_value
    return True, overwritten


# =============================================================================
# File helpers
# =============================================================================

def should_skip_file(path: Path) -> bool:
    if path.name.startswith("~$"):
        return True
    if path.suffix.lower() not in SUPPORTED_SUFFIXES:
        return True
    if not SKIP_SUMMARY_AND_AUDIT_FILES:
        return False

    stem_lower = path.stem.lower()
    name_lower = path.name.lower()
    skip_markers = [
        "summary",
        "audit",
        "removed_university_rows",
        "removed_universities",
    ]
    return any(marker in stem_lower or marker in name_lower for marker in skip_markers)


def find_county_folders(base_folder: Path) -> List[Path]:
    if not base_folder.exists() or not base_folder.is_dir():
        return []
    return sorted([p for p in base_folder.iterdir() if p.is_dir() and not p.name.startswith("_")], key=lambda p: p.name.lower())


def find_data_files(folder: Path) -> List[Path]:
    if not folder.exists() or not folder.is_dir():
        return []
    return sorted(
        [p for p in folder.iterdir() if p.is_file() and not should_skip_file(p)],
        key=lambda p: p.name.lower(),
    )


def read_data_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return clean_dataframe(pd.read_excel(path, dtype=str, keep_default_na=False, engine="openpyxl"))

    if suffix == ".csv":
        encodings = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
        last_error: Optional[Exception] = None
        for encoding in encodings:
            try:
                return clean_dataframe(
                    pd.read_csv(
                        path,
                        dtype=str,
                        keep_default_na=False,
                        encoding=encoding,
                        low_memory=False,
                    )
                )
            except UnicodeDecodeError as exc:
                last_error = exc
                continue
        if last_error is not None:
            raise last_error

    raise ValueError(f"Unsupported file type: {path.suffix} for file {path}")


def write_output_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_folder(path.parent)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def output_path_for(input_file: Path, county_folder: Optional[Path], input_root: Path, output_root: Path) -> Path:
    if county_folder is None:
        relative_folder = Path()
        county_name = input_file.stem
    else:
        try:
            relative_folder = county_folder.relative_to(input_root)
        except ValueError:
            relative_folder = Path(county_folder.name)
        county_name = relative_folder.name if str(relative_folder) else county_folder.name

    return output_root / relative_folder / f"{county_name}{OUTPUT_FILE_SUFFIX}{OUTPUT_SUFFIX}"


# =============================================================================
# Address enrichment logic
# =============================================================================

def enrich_addresses_for_dataframe(df: pd.DataFrame, input_file: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    out = df.copy()
    before_rows = len(out)

    address_col = find_column(out, ADDRESS_COLUMN)
    if address_col is None:
        summary = {
            "status": "skipped_missing_address_column",
            "input_file": str(input_file),
            "address_column": ADDRESS_COLUMN,
            "city_column": CITY_COLUMN,
            "state_column": STATE_COLUMN,
            "zip_column": ZIP_COLUMN,
            "total_rows": before_rows,
            "rows_with_mailing_address": 0,
            "mailing_address_normalized": 0,
            "fully_parsed_rows": 0,
            "zip_only_fallback_rows": 0,
            "unparsed_address_rows": 0,
            "city_values_filled": 0,
            "state_values_filled": 0,
            "zip_values_filled": 0,
            "target_cells_overwritten": 0,
            "missing_city_after": 0,
            "missing_state_after": 0,
            "missing_zip_after": 0,
            "parse_success_pct_of_address_rows": 0.0,
            "error": f"Column '{ADDRESS_COLUMN}' was not found. File was written unchanged.",
            "generated_at_utc": utc_now_text(),
        }
        return out, summary

    city_col = get_or_create_column(out, CITY_COLUMN)
    state_col = get_or_create_column(out, STATE_COLUMN)
    zip_col = get_or_create_column(out, ZIP_COLUMN)

    rows_with_mailing_address = 0
    mailing_address_normalized = 0
    fully_parsed_rows = 0
    zip_only_fallback_rows = 0
    unparsed_address_rows = 0
    city_values_filled = 0
    state_values_filled = 0
    zip_values_filled = 0
    target_cells_overwritten = 0

    for idx, row in out.iterrows():
        raw_address = row.get(address_col, "")
        if is_missing(raw_address):
            continue

        rows_with_mailing_address += 1
        normalized_address = normalize_address(raw_address)

        if NORMALIZE_MAILING_ADDRESS and normalized_address != str(raw_address).strip():
            out.at[idx, address_col] = normalized_address
            mailing_address_normalized += 1

        parsed = parse_us_address_line(normalized_address)

        if parsed:
            _street, city, state, zip5 = parsed
            fully_parsed_rows += 1

            changed, overwritten = set_target_value(out, idx, city_col, smart_title(city) if city else "")
            city_values_filled += int(changed)
            target_cells_overwritten += int(overwritten)

            changed, overwritten = set_target_value(out, idx, state_col, state)
            state_values_filled += int(changed)
            target_cells_overwritten += int(overwritten)

            changed, overwritten = set_target_value(out, idx, zip_col, zip5)
            zip_values_filled += int(changed)
            target_cells_overwritten += int(overwritten)
            continue

        zip5 = find_zip_only(normalized_address)
        if zip5:
            zip_only_fallback_rows += 1

            changed, overwritten = set_target_value(out, idx, zip_col, zip5)
            zip_values_filled += int(changed)
            target_cells_overwritten += int(overwritten)

            changed, overwritten = set_target_value(out, idx, state_col, DEFAULT_STATE)
            state_values_filled += int(changed)
            target_cells_overwritten += int(overwritten)
            continue

        unparsed_address_rows += 1

    missing_city_after = int(out[city_col].map(is_missing).sum()) if city_col in out.columns else 0
    missing_state_after = int(out[state_col].map(is_missing).sum()) if state_col in out.columns else 0
    missing_zip_after = int(out[zip_col].map(is_missing).sum()) if zip_col in out.columns else 0

    summary = {
        "status": "ok",
        "input_file": str(input_file),
        "address_column": address_col,
        "city_column": city_col,
        "state_column": state_col,
        "zip_column": zip_col,
        "fill_only_blank_targets": FILL_ONLY_BLANK_TARGETS,
        "default_state_for_zip_only_fallback": DEFAULT_STATE,
        "total_rows": before_rows,
        "rows_with_mailing_address": rows_with_mailing_address,
        "mailing_address_normalized": mailing_address_normalized,
        "fully_parsed_rows": fully_parsed_rows,
        "zip_only_fallback_rows": zip_only_fallback_rows,
        "unparsed_address_rows": unparsed_address_rows,
        "city_values_filled": city_values_filled,
        "state_values_filled": state_values_filled,
        "zip_values_filled": zip_values_filled,
        "target_cells_overwritten": target_cells_overwritten,
        "missing_city_after": missing_city_after,
        "missing_state_after": missing_state_after,
        "missing_zip_after": missing_zip_after,
        "parse_success_pct_of_address_rows": pct(fully_parsed_rows + zip_only_fallback_rows, rows_with_mailing_address),
        "generated_at_utc": utc_now_text(),
    }
    return out, summary


# =============================================================================
# Folder processing
# =============================================================================

FILE_SUMMARIES: List[Dict[str, Any]] = []


def process_file(input_file: Path, output_file: Path, county_name: str, verbose: bool = True) -> Dict[str, Any]:
    log(f"    [READ] {input_file}", verbose)
    df = read_data_file(input_file)
    log(f"    [ROWS] {len(df):,}", verbose)

    enriched_df, summary = enrich_addresses_for_dataframe(df, input_file=input_file)
    summary["county"] = county_name
    summary["output_file"] = str(output_file)

    write_output_csv(enriched_df, output_file)

    log(f"    [MAILING ADDRESS ROWS] {summary.get('rows_with_mailing_address', 0):,}", verbose)
    log(f"    [FULLY PARSED] {summary.get('fully_parsed_rows', 0):,}", verbose)
    log(f"    [ZIP-ONLY FALLBACK] {summary.get('zip_only_fallback_rows', 0):,}", verbose)
    log(
        "    [FILLED] "
        f"city={summary.get('city_values_filled', 0):,}, "
        f"state={summary.get('state_values_filled', 0):,}, "
        f"zip={summary.get('zip_values_filled', 0):,}",
        verbose,
    )
    log(f"    [SAVE] {output_file}", verbose)

    if summary.get("status") != "ok":
        log(f"    [WARN] {summary.get('error', 'File was not fully enriched.')}", True)

    return summary


def process_county_folder(county_folder: Path, input_root: Path, output_root: Path, verbose: bool = True) -> Dict[str, Any]:
    county_name = county_folder.name
    data_files = find_data_files(county_folder)

    stats: Dict[str, Any] = {
        "county": county_name,
        "files": 0,
        "rows": 0,
        "address_rows": 0,
        "fully_parsed": 0,
        "zip_only": 0,
        "city_filled": 0,
        "state_filled": 0,
        "zip_filled": 0,
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
            summary = process_file(
                input_file=data_path,
                output_file=output_file,
                county_name=county_name,
                verbose=verbose,
            )

            stats["files"] += 1
            stats["rows"] += int(summary.get("total_rows", 0) or 0)
            stats["address_rows"] += int(summary.get("rows_with_mailing_address", 0) or 0)
            stats["fully_parsed"] += int(summary.get("fully_parsed_rows", 0) or 0)
            stats["zip_only"] += int(summary.get("zip_only_fallback_rows", 0) or 0)
            stats["city_filled"] += int(summary.get("city_values_filled", 0) or 0)
            stats["state_filled"] += int(summary.get("state_values_filled", 0) or 0)
            stats["zip_filled"] += int(summary.get("zip_values_filled", 0) or 0)
            FILE_SUMMARIES.append(summary)

        except Exception as exc:
            stats["errors"] += 1
            error_summary = {
                "status": "error",
                "county": county_name,
                "input_file": str(data_path),
                "output_file": str(output_file),
                "address_column": ADDRESS_COLUMN,
                "city_column": CITY_COLUMN,
                "state_column": STATE_COLUMN,
                "zip_column": ZIP_COLUMN,
                "total_rows": 0,
                "rows_with_mailing_address": 0,
                "fully_parsed_rows": 0,
                "zip_only_fallback_rows": 0,
                "unparsed_address_rows": 0,
                "city_values_filled": 0,
                "state_values_filled": 0,
                "zip_values_filled": 0,
                "target_cells_overwritten": 0,
                "missing_city_after": 0,
                "missing_state_after": 0,
                "missing_zip_after": 0,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=5),
                "generated_at_utc": utc_now_text(),
            }
            FILE_SUMMARIES.append(error_summary)
            log(f"    [ERROR] Failed to process {data_path.name}: {exc}", True)

    log(
        f"[COUNTY DONE] {county_name}: {stats['files']:,} file(s), "
        f"{stats['rows']:,} row(s), fully parsed {stats['fully_parsed']:,}, "
        f"ZIP-only {stats['zip_only']:,}, errors {stats['errors']:,}",
        verbose,
    )

    return stats


def process_flat_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    data_files = find_data_files(input_root)
    log("=" * 90, verbose)
    log("[START] Enriching address fields from flat folder", verbose)
    log(f"[TIME] {now_text()}", verbose)
    log(f"[INPUT ROOT]  {input_root}", verbose)
    log(f"[OUTPUT ROOT] {output_root}", verbose)
    log(f"[FILES]       {len(data_files):,} file(s) found", verbose)
    log("=" * 90, verbose)

    stats = {
        "county": "",
        "files": 0,
        "rows": 0,
        "address_rows": 0,
        "fully_parsed": 0,
        "zip_only": 0,
        "city_filled": 0,
        "state_filled": 0,
        "zip_filled": 0,
        "errors": 0,
    }

    for file_number, data_path in enumerate(data_files, start=1):
        output_file = output_path_for(data_path, None, input_root, output_root)

        try:
            log(f"\n[FILE {file_number:,}/{len(data_files):,}] {data_path.name}", verbose)
            summary = process_file(
                input_file=data_path,
                output_file=output_file,
                county_name="",
                verbose=verbose,
            )
            stats["files"] += 1
            stats["rows"] += int(summary.get("total_rows", 0) or 0)
            stats["address_rows"] += int(summary.get("rows_with_mailing_address", 0) or 0)
            stats["fully_parsed"] += int(summary.get("fully_parsed_rows", 0) or 0)
            stats["zip_only"] += int(summary.get("zip_only_fallback_rows", 0) or 0)
            stats["city_filled"] += int(summary.get("city_values_filled", 0) or 0)
            stats["state_filled"] += int(summary.get("state_values_filled", 0) or 0)
            stats["zip_filled"] += int(summary.get("zip_values_filled", 0) or 0)
            FILE_SUMMARIES.append(summary)
        except Exception as exc:
            stats["errors"] += 1
            error_summary = {
                "status": "error",
                "county": "",
                "input_file": str(data_path),
                "output_file": str(output_file),
                "address_column": ADDRESS_COLUMN,
                "city_column": CITY_COLUMN,
                "state_column": STATE_COLUMN,
                "zip_column": ZIP_COLUMN,
                "total_rows": 0,
                "rows_with_mailing_address": 0,
                "fully_parsed_rows": 0,
                "zip_only_fallback_rows": 0,
                "unparsed_address_rows": 0,
                "city_values_filled": 0,
                "state_values_filled": 0,
                "zip_values_filled": 0,
                "target_cells_overwritten": 0,
                "missing_city_after": 0,
                "missing_state_after": 0,
                "missing_zip_after": 0,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=5),
                "generated_at_utc": utc_now_text(),
            }
            FILE_SUMMARIES.append(error_summary)
            log(f"    [ERROR] Failed to process {data_path.name}: {exc}", True)

    return [stats]


def write_summary_csv(output_root: Path, verbose: bool = True) -> Path:
    summary_path = output_root / SUMMARY_CSV_NAME
    ensure_folder(summary_path.parent)
    pd.DataFrame(FILE_SUMMARIES).to_csv(summary_path, index=False, encoding="utf-8-sig")
    log("\n" + "-" * 90, verbose)
    log(f"[SUMMARY CSV] {summary_path}", verbose)
    return summary_path


def process_parent_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(f"Input root folder does not exist or is not a directory: {input_root}")

    ensure_folder(output_root)
    county_folders = find_county_folders(input_root)

    # Fallback: if there are no county subfolders, process data files directly in INPUT_ROOT.
    if not county_folders:
        stats = process_flat_folder(input_root=input_root, output_root=output_root, verbose=verbose)
        if WRITE_SUMMARY_CSV:
            write_summary_csv(output_root, verbose=verbose)
        return stats

    log("=" * 90, verbose)
    log("[START] Enriching city, state, and ZIP from mailing_address", verbose)
    log(f"[TIME] {now_text()}", verbose)
    log(f"[INPUT ROOT]  {input_root}", verbose)
    log(f"[OUTPUT ROOT] {output_root}", verbose)
    log(f"[ADDRESS COLUMN] {ADDRESS_COLUMN}", verbose)
    log(f"[FILL MODE] Only blank city/state/zip targets: {FILL_ONLY_BLANK_TARGETS}", verbose)
    log(f"[COUNTIES] {len(county_folders):,} folder(s) found", verbose)
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
        write_summary_csv(output_root, verbose=verbose)

    return county_stats


def main() -> None:
    input_root = Path(INPUT_ROOT).resolve()
    output_root = Path(OUTPUT_ROOT).resolve()
    verbose = not QUIET

    county_stats = process_parent_folder(input_root=input_root, output_root=output_root, verbose=verbose)

    total_counties = len(county_stats)
    total_files = sum(int(s.get("files", 0) or 0) for s in county_stats)
    total_rows = sum(int(s.get("rows", 0) or 0) for s in county_stats)
    total_address_rows = sum(int(s.get("address_rows", 0) or 0) for s in county_stats)
    total_fully_parsed = sum(int(s.get("fully_parsed", 0) or 0) for s in county_stats)
    total_zip_only = sum(int(s.get("zip_only", 0) or 0) for s in county_stats)
    total_city_filled = sum(int(s.get("city_filled", 0) or 0) for s in county_stats)
    total_state_filled = sum(int(s.get("state_filled", 0) or 0) for s in county_stats)
    total_zip_filled = sum(int(s.get("zip_filled", 0) or 0) for s in county_stats)
    total_errors = sum(int(s.get("errors", 0) or 0) for s in county_stats)

    if verbose:
        print("\n" + "=" * 90)
        print("[DONE] Address enrichment completed.")
        print(f"Total county groups processed   : {total_counties:,}")
        print(f"Total files processed           : {total_files:,}")
        print(f"Total rows processed            : {total_rows:,}")
        print(f"Rows with mailing_address       : {total_address_rows:,}")
        print(f"Rows fully parsed               : {total_fully_parsed:,}")
        print(f"Rows with ZIP-only fallback     : {total_zip_only:,}")
        print(f"City values filled              : {total_city_filled:,}")
        print(f"State values filled             : {total_state_filled:,}")
        print(f"ZIP values filled               : {total_zip_filled:,}")
        print(f"Files with errors               : {total_errors:,}")
        print(f"Output root                     : {output_root}")
        if WRITE_SUMMARY_CSV:
            print(f"Summary CSV                     : {output_root / SUMMARY_CSV_NAME}")
        print("=" * 90)


if __name__ == "__main__":
    main()
