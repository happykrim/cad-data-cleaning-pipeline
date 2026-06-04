#!/usr/bin/env python
"""
Pipeline step 4: deduplicate owners and preserve parcel-level lists.

Recommended script name:
    4_dedup_owners_add_lists_no_arg.py

Run after:
    3_remove_id_duplicates_no_arg.py

What it does:
- Reads the county folders produced by 3_remove_id_duplicates_no_arg.py.
- Groups rows by owner identity:
    full_name + mailing_address
- Important: blank/empty mailing_address rows are NOT grouped together; each blank-address row stays separate.
- Keeps one representative row per owner group using this safer rule:
    1. Prefer the largest-acre row that has a non-empty legal_address.
    2. If every row in the owner group has an empty legal_address, fall back to the largest-acre row.
- This prevents unrelated owners from being merged only because their mailing_address is missing.
- It also prevents the kept representative row from losing legal_address when another row in the same owner group has a usable legal_address.
- Adds owner-level list/count columns:
    lands_count
    acres_list
    id_list
- Also converts these fields into owner-level aligned lists:
    market_value_list
    owner_tax_year_list
    deed_date_list
    legal_acreage_filled_by_script
    Empty_Legal_Acreage

Important ordering rule:
- id_list, acres_list, market_value_list, owner_tax_year_list, deed_date_list,
  legal_acreage_filled_by_script, and Empty_Legal_Acreage are all built from
  the same ordered group of rows.
- This means position 1 in every list belongs to the same parcel, position 2 belongs
  to the same parcel, and so on.

Requirements:
    pip install pandas openpyxl

No command-line arguments are required. Edit CONFIG values below if needed.
You can also override INPUT_ROOT and OUTPUT_ROOT with these environment variables:
    CAD_OWNER_DEDUPE_INPUT_ROOT
    CAD_OWNER_DEDUPE_OUTPUT_ROOT
    INPUT_ROOT
    OUTPUT_ROOT
"""

from __future__ import annotations

import json
import math
import os
import re
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# This should match OUTPUT_ROOT from 3_remove_id_duplicates_no_arg.py.
DEFAULT_INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_4_no_id_duplicates"

# New pipeline output folder.
DEFAULT_OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_5_no_owner_duplicates"

INPUT_ROOT = os.environ.get("CAD_OWNER_DEDUPE_INPUT_ROOT") or os.environ.get("INPUT_ROOT") or DEFAULT_INPUT_ROOT
OUTPUT_ROOT = os.environ.get("CAD_OWNER_DEDUPE_OUTPUT_ROOT") or os.environ.get("OUTPUT_ROOT") or DEFAULT_OUTPUT_ROOT

# Columns expected in the remapped files.
COL_FULL_NAME = "full_name"
COL_MAILING = "mailing_address"
COL_ACRES = "acres"
COL_ID = "id"
COL_LEGAL_ADDRESS = "legal_address"

# Owner-level columns created or overwritten by this script.
COL_LANDS_COUNT = "lands_count"
COL_ACRES_LIST = "acres_list"
COL_ID_LIST = "id_list"
ALIGNED_LIST_COLUMNS = [
    "market_value_list",
    "owner_tax_year_list",
    "deed_date_list",
    "legal_acreage_filled_by_script",
    "Empty_Legal_Acreage",
]

# Output naming.
OUTPUT_FILE_SUFFIX = "_no_owner_duplicates"
WRITE_SUMMARY_CSV = True
SUMMARY_CSV_NAME = "dedup_owners_summary.csv"

# Always writing CSV keeps the pipeline simple and consistent.
OUTPUT_SUFFIX = ".csv"

# If True, missing aligned-list columns are created as blank list values instead of failing.
CREATE_MISSING_ALIGNED_LIST_COLUMNS = True

# Lists are written as JSON arrays, e.g. ["100000", "250000"].
# This is safer in CSV than Python list repr and preserves blank values for alignment.
LIST_OUTPUT_FORMAT = "json"  # valid: "json" or "pipe"
PIPE_SEPARATOR = " | "

# Grouping behavior.
# False matches the original owner-dedup script more closely: group by the visible values.
# True groups values after trimming whitespace and lowercasing.
NORMALIZE_OWNER_GROUP_KEYS = False

# Safety rule requested for this pipeline:
# False means rows with blank/empty mailing_address are NEVER grouped together, even when
# full_name is the same. Each blank-address row receives its own internal group key.
# This avoids merging unrelated parcels just because the mailing address is missing.
GROUP_ROWS_WITH_BLANK_MAILING_ADDRESS = False

# Minimal terminal output.
QUIET = False

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm"}
TEMP_COLUMNS = {"__owner_group_key", "__mailing_group_key", "__blank_mailing_address", "__acres_numeric", "__has_legal_address", "__original_order"}


# =============================================================================
# Logging and formatting helpers
# =============================================================================

def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def log(message: str, verbose: bool = True) -> None:
    if verbose:
        print(message, flush=True)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if isinstance(value, float) and math.isnan(value):
            return True
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text.lower() in {"", "nan", "none", "null", "n/a", "na", "--", "unknown", "not available"}


def clean_cell(value: Any) -> str:
    """Convert a cell to a clean string for CSV/list output."""
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


def group_key(value: Any) -> str:
    text = clean_cell(value)
    if NORMALIZE_OWNER_GROUP_KEYS:
        return re.sub(r"\s+", " ", text).strip().lower()
    return text


def has_usable_legal_address(value: Any) -> bool:
    """Return True when legal_address contains a real usable value."""
    return not is_missing(value)


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


def serialize_list(values: List[Any]) -> str:
    """
    Serialize list values for CSV output.

    JSON preserves the number of items and keeps blanks, which matters because these
    lists are aligned by position. Example:
      id_list[0], acres_list[0], and market_value_list[0] all describe the same parcel.
    """
    cleaned = [clean_cell(value) for value in values]

    if LIST_OUTPUT_FORMAT.lower() == "pipe":
        return PIPE_SEPARATOR.join(cleaned)

    return json.dumps(cleaned, ensure_ascii=False)


def serialize_acres_list(values: List[Any]) -> str:
    cleaned = [format_number_for_list(value) for value in values]

    if LIST_OUTPUT_FORMAT.lower() == "pipe":
        return PIPE_SEPARATOR.join(cleaned)

    return json.dumps(cleaned, ensure_ascii=False)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


# =============================================================================
# File helpers
# =============================================================================

def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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
# Owner deduplication logic
# =============================================================================

def check_required_columns(df: pd.DataFrame, input_file: Path) -> None:
    required = [COL_FULL_NAME, COL_MAILING, COL_ACRES, COL_ID]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(
            f"Missing required column(s) in {input_file}: {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def ensure_aligned_list_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    Make sure aligned list columns exist.

    Returns:
      (df, present_columns, created_columns)
    """
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


def prepare_dataframe_for_grouping(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["__original_order"] = range(len(out))
    out["__owner_group_key"] = out[COL_FULL_NAME].map(group_key)
    out["__mailing_group_key"] = out[COL_MAILING].map(group_key)
    out["__blank_mailing_address"] = out[COL_MAILING].map(is_missing)

    # Requested safety rule:
    # Only group rows when BOTH full_name and mailing_address match, and the
    # mailing_address is not blank. Blank mailing addresses should not be treated
    # as the same address, because different owners/parcels can all have missing
    # address values.
    if not GROUP_ROWS_WITH_BLANK_MAILING_ADDRESS:
        blank_mask = out["__blank_mailing_address"].fillna(False).astype(bool)
        out.loc[blank_mask, "__mailing_group_key"] = out.loc[blank_mask, "__original_order"].map(
            lambda row_number: f"__blank_mailing_address_row_{row_number}__"
        )

    out["__acres_numeric"] = out[COL_ACRES].map(parse_number).fillna(0.0)
    if COL_LEGAL_ADDRESS in out.columns:
        out["__has_legal_address"] = out[COL_LEGAL_ADDRESS].map(has_usable_legal_address)
    else:
        out["__has_legal_address"] = False
    return out


def collect_aligned_values(group: pd.DataFrame, column: str) -> List[str]:
    """Collect one value per row, preserving blanks so list positions remain aligned."""
    if column not in group.columns:
        return [""] * len(group)
    return [clean_cell(value) for value in group[column].tolist()]


def choose_representative_row(group_ordered: pd.DataFrame) -> Tuple[pd.Series, Dict[str, Any]]:
    """
    Choose the representative row for an owner group.

    Preferred rule:
    - Use the largest-acre row that has a non-empty legal_address.
    - If no row in the group has legal_address, fall back to the largest-acre row.

    This keeps the representative row useful for downstream mailing-address rollups
    without changing the order of id_list/acres_list/aligned parcel lists.
    """
    idx_largest_any = group_ordered["__acres_numeric"].idxmax()
    largest_any = group_ordered.loc[idx_largest_any]

    has_legal_series = group_ordered.get("__has_legal_address")
    if has_legal_series is None:
        return largest_any.copy(), {
            "representative_selection_method": "largest_acres_legal_address_column_missing",
            "largest_acres_row_had_blank_legal_address": False,
            "representative_switched_to_non_empty_legal_address": False,
            "all_legal_addresses_empty_in_group": False,
        }

    has_legal_mask = has_legal_series.fillna(False).astype(bool)
    largest_any_has_legal = bool(largest_any.get("__has_legal_address", False))

    if has_legal_mask.any():
        legal_candidates = group_ordered.loc[has_legal_mask]
        idx_best_with_legal = legal_candidates["__acres_numeric"].idxmax()
        chosen = group_ordered.loc[idx_best_with_legal]
        switched = idx_best_with_legal != idx_largest_any
        return chosen.copy(), {
            "representative_selection_method": (
                "largest_acres_with_non_empty_legal_address"
                if not switched
                else "largest_acres_with_non_empty_legal_address_over_blank_largest_acres_row"
            ),
            "largest_acres_row_had_blank_legal_address": not largest_any_has_legal,
            "representative_switched_to_non_empty_legal_address": bool(switched),
            "all_legal_addresses_empty_in_group": False,
        }

    return largest_any.copy(), {
        "representative_selection_method": "largest_acres_all_legal_addresses_empty",
        "largest_acres_row_had_blank_legal_address": True,
        "representative_switched_to_non_empty_legal_address": False,
        "all_legal_addresses_empty_in_group": True,
    }


def dedupe_owners_for_dataframe(df: pd.DataFrame, input_file: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Group by (full_name, mailing_address), choose the safest representative row, and create aligned lists.
    """
    check_required_columns(df, input_file=input_file)
    df, aligned_columns_present, aligned_columns_created = ensure_aligned_list_columns(df)

    before_rows = len(df)
    if before_rows == 0:
        summary = {
            "status": "ok_empty",
            "input_file": str(input_file),
            "total_rows_before": 0,
            "rows_after": 0,
            "removed_owner_duplicate_rows": 0,
            "owner_groups": 0,
            "duplicate_owner_groups": 0,
            "max_lands_for_one_owner": 0,
            "groups_where_largest_acres_row_had_blank_legal_address": 0,
            "representative_rows_switched_to_non_empty_legal_address": 0,
            "groups_all_legal_addresses_empty": 0,
            "rows_with_blank_mailing_address": 0,
            "blank_mailing_address_rows_kept_separate": 0,
            "aligned_list_columns_present": ", ".join(aligned_columns_present),
            "aligned_list_columns_created_blank": ", ".join(aligned_columns_created),
            "generated_at_utc": utc_now_text(),
        }
        return df.copy(), summary

    work = prepare_dataframe_for_grouping(df)
    rows_with_blank_mailing_address = int(work["__blank_mailing_address"].fillna(False).astype(bool).sum())
    blank_mailing_address_rows_kept_separate = (
        rows_with_blank_mailing_address if not GROUP_ROWS_WITH_BLANK_MAILING_ADDRESS else 0
    )

    grouped = work.groupby(["__owner_group_key", "__mailing_group_key"], dropna=False, sort=False)

    records: List[pd.Series] = []
    duplicate_owner_groups = 0
    max_lands_for_one_owner = 0
    total_lands_in_duplicate_owner_groups = 0
    groups_where_largest_acres_row_had_blank_legal_address = 0
    representative_rows_switched_to_non_empty_legal_address = 0
    groups_all_legal_addresses_empty = 0

    for _, group in grouped:
        # Keep the same row order for every list so the lists are aligned by position.
        group_ordered = group.sort_values("__original_order")
        lands_count = int(len(group_ordered))
        max_lands_for_one_owner = max(max_lands_for_one_owner, lands_count)

        if lands_count > 1:
            duplicate_owner_groups += 1
            total_lands_in_duplicate_owner_groups += lands_count

        # Representative row: prefer the largest-acre row with a non-empty legal_address.
        # If all legal addresses are blank in the group, fall back to the largest-acre row.
        rep_row, rep_choice = choose_representative_row(group_ordered)
        if rep_choice.get("largest_acres_row_had_blank_legal_address"):
            groups_where_largest_acres_row_had_blank_legal_address += 1
        if rep_choice.get("representative_switched_to_non_empty_legal_address"):
            representative_rows_switched_to_non_empty_legal_address += 1
        if rep_choice.get("all_legal_addresses_empty_in_group"):
            groups_all_legal_addresses_empty += 1

        # Clean core representative values.
        rep_row[COL_ID] = clean_cell(rep_row.get(COL_ID, ""))
        rep_row[COL_FULL_NAME] = clean_cell(rep_row.get(COL_FULL_NAME, ""))
        rep_row[COL_MAILING] = clean_cell(rep_row.get(COL_MAILING, ""))
        rep_row[COL_ACRES] = format_number_for_list(rep_row.get(COL_ACRES, ""))

        # Owner-level list/count fields.
        rep_row[COL_LANDS_COUNT] = lands_count
        rep_row[COL_ID_LIST] = serialize_list(collect_aligned_values(group_ordered, COL_ID))
        rep_row[COL_ACRES_LIST] = serialize_acres_list(group_ordered[COL_ACRES].tolist())

        # Requested aligned list fields. These overwrite the per-parcel values with owner-level lists.
        for col in ALIGNED_LIST_COLUMNS:
            rep_row[col] = serialize_list(collect_aligned_values(group_ordered, col))

        # Remove internal helper columns before output.
        rep_row = rep_row.drop(labels=[col for col in TEMP_COLUMNS if col in rep_row.index])
        records.append(rep_row)

    deduped_df = pd.DataFrame(records)

    # Put key owner/list columns in a friendly order, while preserving all other input columns.
    preferred_order = [
        COL_ID,
        COL_FULL_NAME,
        COL_LEGAL_ADDRESS,
        COL_MAILING,
        COL_ACRES,
        "city",
        "zip",
        "state",
        COL_LANDS_COUNT,
        COL_ID_LIST,
        COL_ACRES_LIST,
        *ALIGNED_LIST_COLUMNS,
    ]
    ordered_cols = [col for col in preferred_order if col in deduped_df.columns]
    remaining_cols = [col for col in deduped_df.columns if col not in ordered_cols and col not in TEMP_COLUMNS]
    deduped_df = deduped_df[ordered_cols + remaining_cols]

    after_rows = len(deduped_df)
    removed_rows = before_rows - after_rows

    summary = {
        "status": "ok",
        "input_file": str(input_file),
        "total_rows_before": before_rows,
        "rows_after": after_rows,
        "removed_owner_duplicate_rows": removed_rows,
        "owner_groups": after_rows,
        "duplicate_owner_groups": duplicate_owner_groups,
        "lands_in_duplicate_owner_groups": total_lands_in_duplicate_owner_groups,
        "max_lands_for_one_owner": max_lands_for_one_owner,
        "groups_where_largest_acres_row_had_blank_legal_address": groups_where_largest_acres_row_had_blank_legal_address,
        "representative_rows_switched_to_non_empty_legal_address": representative_rows_switched_to_non_empty_legal_address,
        "groups_all_legal_addresses_empty": groups_all_legal_addresses_empty,
        "rows_with_blank_mailing_address": rows_with_blank_mailing_address,
        "blank_mailing_address_rows_kept_separate": blank_mailing_address_rows_kept_separate,
        "group_columns": f"{COL_FULL_NAME}, {COL_MAILING}",
        "representative_row_rule": (
            "largest acres with non-empty legal_address when available; "
            "fallback to largest acres when all legal_address values are blank; "
            "blank mailing addresses are kept separate"
        ),
        "blank_mailing_address_grouping_rule": (
            "kept separate; blank mailing addresses are not treated as matching"
            if not GROUP_ROWS_WITH_BLANK_MAILING_ADDRESS
            else "grouped when owner name also matches"
        ),
        "list_order_rule": "input row order within each owner group",
        "list_output_format": LIST_OUTPUT_FORMAT,
        "aligned_list_columns_present": ", ".join(aligned_columns_present),
        "aligned_list_columns_created_blank": ", ".join(aligned_columns_created),
        "generated_at_utc": utc_now_text(),
    }
    return deduped_df, summary


# =============================================================================
# Folder processing
# =============================================================================

def process_county_folder(county_folder: Path, input_root: Path, output_root: Path, verbose: bool = True) -> Dict[str, Any]:
    county_name = county_folder.name
    data_files = find_data_files(county_folder)

    stats: Dict[str, Any] = {
        "county": county_name,
        "files": 0,
        "before": 0,
        "after": 0,
        "removed": 0,
        "duplicate_owner_groups": 0,
        "max_lands_for_one_owner": 0,
        "groups_where_largest_acres_row_had_blank_legal_address": 0,
        "representative_rows_switched_to_non_empty_legal_address": 0,
        "groups_all_legal_addresses_empty": 0,
        "rows_with_blank_mailing_address": 0,
        "blank_mailing_address_rows_kept_separate": 0,
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

            deduped_df, summary = dedupe_owners_for_dataframe(df, input_file=data_path)
            summary["county"] = county_name
            summary["output_file"] = str(output_file)

            write_output_csv(deduped_df, output_file)

            stats["files"] += 1
            stats["before"] += int(summary.get("total_rows_before", 0) or 0)
            stats["after"] += int(summary.get("rows_after", 0) or 0)
            stats["removed"] += int(summary.get("removed_owner_duplicate_rows", 0) or 0)
            stats["duplicate_owner_groups"] += int(summary.get("duplicate_owner_groups", 0) or 0)
            stats["groups_where_largest_acres_row_had_blank_legal_address"] += int(summary.get("groups_where_largest_acres_row_had_blank_legal_address", 0) or 0)
            stats["representative_rows_switched_to_non_empty_legal_address"] += int(summary.get("representative_rows_switched_to_non_empty_legal_address", 0) or 0)
            stats["groups_all_legal_addresses_empty"] += int(summary.get("groups_all_legal_addresses_empty", 0) or 0)
            stats["rows_with_blank_mailing_address"] += int(summary.get("rows_with_blank_mailing_address", 0) or 0)
            stats["blank_mailing_address_rows_kept_separate"] += int(summary.get("blank_mailing_address_rows_kept_separate", 0) or 0)
            stats["max_lands_for_one_owner"] = max(
                int(stats["max_lands_for_one_owner"]),
                int(summary.get("max_lands_for_one_owner", 0) or 0),
            )

            COUNTY_FILE_SUMMARIES.append(summary)

            log(f"    [OWNER GROUPS] {summary.get('owner_groups', 0):,}", verbose)
            log(f"    [DUPLICATE OWNER GROUPS] {summary.get('duplicate_owner_groups', 0):,}", verbose)
            log(f"    [LEGAL ADDR REP SWITCHES] {summary.get('representative_rows_switched_to_non_empty_legal_address', 0):,}", verbose)
            log(f"    [GROUPS ALL LEGAL ADDR BLANK] {summary.get('groups_all_legal_addresses_empty', 0):,}", verbose)
            log(f"    [BLANK MAILING ROWS] {summary.get('rows_with_blank_mailing_address', 0):,}", verbose)
            log(f"    [BLANK MAILING KEPT SEPARATE] {summary.get('blank_mailing_address_rows_kept_separate', 0):,}", verbose)
            log(f"    [MAX LANDS / OWNER] {summary.get('max_lands_for_one_owner', 0):,}", verbose)
            log(f"    [ROWS AFTER] {summary.get('rows_after', 0):,}", verbose)
            log(f"    [REMOVED ROWS] {summary.get('removed_owner_duplicate_rows', 0):,}", verbose)
            if summary.get("aligned_list_columns_created_blank"):
                log(f"    [WARN] Missing aligned-list columns created as blank: {summary['aligned_list_columns_created_blank']}", verbose)
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
                "removed_owner_duplicate_rows": 0,
                "duplicate_owner_groups": 0,
                "groups_where_largest_acres_row_had_blank_legal_address": 0,
                "representative_rows_switched_to_non_empty_legal_address": 0,
                "groups_all_legal_addresses_empty": 0,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=5),
                "generated_at_utc": utc_now_text(),
            }
            COUNTY_FILE_SUMMARIES.append(error_summary)
            log(f"    [ERROR] Failed to process {data_path.name}: {exc}", True)

    log(
        f"[COUNTY DONE] {county_name}: {stats['files']:,} file(s), "
        f"{stats['before']:,} -> {stats['after']:,} rows "
        f"(removed {stats['removed']:,}; errors {stats['errors']:,})",
        verbose,
    )

    return stats


COUNTY_FILE_SUMMARIES: List[Dict[str, Any]] = []


def process_parent_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(f"Input root folder does not exist or is not a directory: {input_root}")

    ensure_folder(output_root)
    county_folders = find_county_folders(input_root)

    log("=" * 90, verbose)
    log("[START] Deduplicating owners", verbose)
    log(f"[TIME] {now_text()}", verbose)
    log(f"[INPUT ROOT]  {input_root}", verbose)
    log(f"[OUTPUT ROOT] {output_root}", verbose)
    log(f"[GROUP BY]    {COL_FULL_NAME} + {COL_MAILING}", verbose)
    log("[BLANK MAILING] kept separate; blank mailing addresses are not treated as matching", verbose)
    log(f"[KEEP ROW]    largest {COL_ACRES} with non-empty {COL_LEGAL_ADDRESS} when available; fallback to largest {COL_ACRES}", verbose)
    log(f"[LIST ORDER]  same input-row order for id/acres/market/tax-year/deed-date/legal-acreage flags", verbose)
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
    total_duplicate_groups = sum(int(s.get("duplicate_owner_groups", 0) or 0) for s in county_stats)
    total_groups_largest_blank_legal = sum(int(s.get("groups_where_largest_acres_row_had_blank_legal_address", 0) or 0) for s in county_stats)
    total_rep_switched_to_legal = sum(int(s.get("representative_rows_switched_to_non_empty_legal_address", 0) or 0) for s in county_stats)
    total_groups_all_legal_blank = sum(int(s.get("groups_all_legal_addresses_empty", 0) or 0) for s in county_stats)
    total_blank_mailing_rows = sum(int(s.get("rows_with_blank_mailing_address", 0) or 0) for s in county_stats)
    total_blank_mailing_kept_separate = sum(int(s.get("blank_mailing_address_rows_kept_separate", 0) or 0) for s in county_stats)
    total_errors = sum(int(s.get("errors", 0) or 0) for s in county_stats)
    max_lands = max([int(s.get("max_lands_for_one_owner", 0) or 0) for s in county_stats] or [0])

    if verbose:
        print("\n" + "=" * 90)
        print("[DONE] Owner deduplication completed.")
        print(f"Total counties processed       : {total_counties:,}")
        print(f"Total files processed          : {total_files:,}")
        print(f"Total rows BEFORE              : {total_before:,}")
        print(f"Total rows AFTER               : {total_after:,}")
        print(f"Total rows REMOVED             : {total_removed:,}")
        print(f"Duplicate owner groups found   : {total_duplicate_groups:,}")
        print(f"Rows with blank mailing address: {total_blank_mailing_rows:,}")
        print(f"Blank mailing rows kept separate: {total_blank_mailing_kept_separate:,}")
        print(f"Max lands for one owner        : {max_lands:,}")
        print(f"Files with errors              : {total_errors:,}")
        print(f"Output root                    : {output_root}")
        print("=" * 90)


if __name__ == "__main__":
    main()
