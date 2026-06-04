#!/usr/bin/env python
"""
Pipeline step: remove duplicate parcel ids after essential-field remapping.

Recommended script name:
    3_remove_id_duplicates_no_arg.py

Run this after:
    2_remap_essential_fields_add_lists_no_arg.py

What it does:
- Reads the county subfolders produced by the essential-field remap step.
- Removes duplicate rows by the `id` column, keeping the first row by default.
- Before dropping duplicates, merges values in these list-style columns when present:
    market_value_list
    owner_tax_year_list
    deed_date_list
  This protects useful values from duplicate parcel rows before only one row is kept.
- Saves one de-duplicated CSV per county and writes a root-level summary CSV.

Requirements:
    pip install pandas openpyxl

No command-line arguments are required. Edit CONFIG values below if needed.
You can also override INPUT_ROOT and OUTPUT_ROOT with these environment variables:
    CAD_DEDUPE_INPUT_ROOT
    CAD_DEDUPE_OUTPUT_ROOT
    INPUT_ROOT
    OUTPUT_ROOT
"""

from __future__ import annotations

import math
import os
import re
import traceback
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# This should be the OUTPUT_ROOT from 2_remap_essential_fields_no_arg.py.
DEFAULT_INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_3_full_data_essential_columns"

# New pipeline output folder.
DEFAULT_OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_4_full_data_essential_columns_no_id_duplicates"

INPUT_ROOT = os.environ.get("CAD_DEDUPE_INPUT_ROOT") or os.environ.get("INPUT_ROOT") or DEFAULT_INPUT_ROOT
OUTPUT_ROOT = os.environ.get("CAD_DEDUPE_OUTPUT_ROOT") or os.environ.get("OUTPUT_ROOT") or DEFAULT_OUTPUT_ROOT

ID_COLUMN = "id"
KEEP = "first"  # pandas-compatible: "first" or "last"

# False is safer: rows with blank ids are kept rather than collapsed into one blank-id row.
DEDUPLICATE_BLANK_IDS = False

# If True, merge these list columns across duplicate ids before dropping duplicate rows.
MERGE_LIST_COLUMNS_BEFORE_KEEPING_FIRST = True
LIST_COLUMNS_TO_MERGE = ["market_value_list", "owner_tax_year_list", "deed_date_list"]

# Boolean/audit fields preserved from the legal-acreage-fill step.
# When duplicate parcel IDs exist, the kept row receives True if any duplicate row is True.
BOOLEAN_COLUMNS_TO_OR = ["legal_acreage_filled_by_script", "Empty_Legal_Acreage"]

LIST_SEPARATOR = " | "

OUTPUT_FILE_SUFFIX = "_no_id_duplicates"
WRITE_SUMMARY_CSV = True
QUIET = False

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls"}


# =============================================================================
# Helpers
# =============================================================================

def now_label() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    if re.fullmatch(r"-?\d+\.0", text):
        text = text[:-2]
    return text


def boolish_to_bool(value: Any) -> bool:
    if is_missing(value):
        return False
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return float(value) != 0.0
        except Exception:
            return False
    text = clean_cell(value).strip().lower()
    return text in {"true", "t", "yes", "y", "1"}


def bool_to_text(value: bool) -> str:
    return "True" if value else "False"


def split_list_cell(value: Any) -> List[str]:
    text = clean_cell(value)
    if not text:
        return []
    if LIST_SEPARATOR in text:
        return [part.strip() for part in text.split(LIST_SEPARATOR) if part.strip()]
    return [text]


def unique_preserve_order(values: Iterable[Any]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        for item in split_list_cell(value):
            key = item.strip().lower()
            if key and key not in seen:
                seen.add(key)
                out.append(item.strip())
    return out


def join_unique_values(values: Iterable[Any]) -> str:
    return LIST_SEPARATOR.join(unique_preserve_order(values))


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def read_data_file(input_path: Path) -> pd.DataFrame:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return clean_dataframe(pd.read_csv(input_path, dtype=object, low_memory=False))
    if suffix == ".xlsx":
        return clean_dataframe(pd.read_excel(input_path, dtype=object, engine="openpyxl"))
    if suffix == ".xls":
        return clean_dataframe(pd.read_excel(input_path, dtype=object))
    raise ValueError(f"Unsupported file type: {input_path}")


def save_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


def find_data_files(folder: Path) -> List[Path]:
    if not folder.exists() or not folder.is_dir():
        return []
    return sorted(
        f for f in folder.iterdir()
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_SUFFIXES
        and not f.name.startswith("~$")
        and not f.name.lower().endswith("_summary.csv")
    )


def get_county_folders(input_root: Path) -> List[Path]:
    county_folders = sorted([p for p in input_root.iterdir() if p.is_dir()])
    if county_folders:
        return county_folders

    # Fallback: allow a root folder that directly contains files.
    if find_data_files(input_root):
        return [input_root]
    return []


def merge_list_columns_for_duplicate_ids(df: pd.DataFrame, id_col: str, duplicate_ids: Sequence[str]) -> Tuple[pd.DataFrame, Dict[str, int]]:
    out = df.copy()
    merge_counts = {col: 0 for col in LIST_COLUMNS_TO_MERGE if col in out.columns}
    bool_merge_counts = {col: 0 for col in BOOLEAN_COLUMNS_TO_OR if col in out.columns}

    if not MERGE_LIST_COLUMNS_BEFORE_KEEPING_FIRST or not duplicate_ids:
        merge_counts.update(bool_merge_counts)
        return out, merge_counts

    cleaned_ids = out[id_col].map(clean_cell)

    for duplicate_id in duplicate_ids:
        matching_index = out.index[cleaned_ids == duplicate_id].tolist()
        if not matching_index:
            continue
        target_index = matching_index[0] if KEEP == "first" else matching_index[-1]

        for col in [c for c in LIST_COLUMNS_TO_MERGE if c in out.columns]:
            merged_value = join_unique_values(out.loc[matching_index, col].tolist())
            before_value = clean_cell(out.at[target_index, col])
            out.at[target_index, col] = merged_value
            if merged_value != before_value:
                merge_counts[col] += 1

        for col in [c for c in BOOLEAN_COLUMNS_TO_OR if c in out.columns]:
            merged_bool = any(boolish_to_bool(value) for value in out.loc[matching_index, col].tolist())
            merged_value = bool_to_text(merged_bool)
            before_value = clean_cell(out.at[target_index, col])
            out.at[target_index, col] = merged_value
            if merged_value != before_value:
                bool_merge_counts[col] += 1

    merge_counts.update(bool_merge_counts)
    return out, merge_counts


def remove_id_duplicates_for_dataframe(df: pd.DataFrame, input_file: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if ID_COLUMN not in df.columns:
        raise KeyError(f"Required column '{ID_COLUMN}' was not found in {input_file}. Available columns: {list(df.columns)}")

    out = df.copy()
    out[ID_COLUMN] = out[ID_COLUMN].map(clean_cell)

    total_rows = len(out)
    nonblank_mask = out[ID_COLUMN].map(lambda x: not is_missing(x))
    blank_id_rows = int((~nonblank_mask).sum())

    if DEDUPLICATE_BLANK_IDS:
        dedupe_mask = pd.Series([True] * len(out), index=out.index)
    else:
        dedupe_mask = nonblank_mask

    id_counts = out.loc[dedupe_mask, ID_COLUMN].value_counts(dropna=False)
    duplicate_ids = id_counts[id_counts > 1].index.astype(str).tolist()
    duplicate_id_groups = len(duplicate_ids)
    duplicate_id_rows = int(out.loc[dedupe_mask, ID_COLUMN].duplicated(keep=False).sum()) if total_rows else 0
    max_rows_for_one_id = int(id_counts.max()) if not id_counts.empty else 0

    out, merge_counts = merge_list_columns_for_duplicate_ids(out, ID_COLUMN, duplicate_ids)

    duplicate_rows_to_remove = out.loc[dedupe_mask].duplicated(subset=[ID_COLUMN], keep=KEEP)
    remove_mask = pd.Series([False] * len(out), index=out.index)
    remove_mask.loc[duplicate_rows_to_remove.index] = duplicate_rows_to_remove

    cleaned_df = out.loc[~remove_mask].copy()
    removed_rows = int(remove_mask.sum())

    summary = {
        "status": "ok",
        "input_file": str(input_file),
        "total_rows_before": total_rows,
        "rows_after": int(len(cleaned_df)),
        "removed_duplicate_rows": removed_rows,
        "duplicate_id_groups": duplicate_id_groups,
        "duplicate_id_rows": duplicate_id_rows,
        "max_rows_for_one_id": max_rows_for_one_id,
        "blank_id_rows_kept": blank_id_rows if not DEDUPLICATE_BLANK_IDS else 0,
        "deduplicated_blank_ids": bool(DEDUPLICATE_BLANK_IDS),
        "merged_list_columns": ", ".join(f"{col}:{count}" for col, count in merge_counts.items()),
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return cleaned_df, summary


def output_path_for(input_file: Path, county_folder: Path, output_root: Path) -> Path:
    county_name = county_folder.name
    return output_root / county_name / f"{county_name}{OUTPUT_FILE_SUFFIX}.csv"


def process_parent_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(f"Input root folder does not exist or is not a directory: {input_root}")

    output_root.mkdir(parents=True, exist_ok=True)
    county_folders = get_county_folders(input_root)
    summaries: List[Dict[str, Any]] = []

    log("=" * 90, verbose)
    log("[START] Removing duplicate parcel ids", verbose)
    log(f"[TIME] {now_label()}", verbose)
    log(f"[INPUT ROOT]  {input_root}", verbose)
    log(f"[OUTPUT ROOT] {output_root}", verbose)
    log(f"[COUNTIES]    {len(county_folders):,} folder(s) found", verbose)
    log("=" * 90, verbose)

    for position, county_folder in enumerate(county_folders, start=1):
        county_name = county_folder.name
        log(f"\n[COUNTY {position:,}/{len(county_folders):,}] {county_name}", verbose)

        files = find_data_files(county_folder)
        if not files:
            summary = {
                "status": "skipped_no_file",
                "county": county_name,
                "input_file": "",
                "output_file": "",
                "total_rows_before": 0,
                "rows_after": 0,
                "removed_duplicate_rows": 0,
                "error": "No supported CSV/XLSX/XLS file found in county folder.",
            }
            summaries.append(summary)
            log("[SKIP] No supported data file found.", verbose)
            continue

        for input_file in files:
            output_file = output_path_for(input_file, county_folder, output_root)
            try:
                log(f"[READ] {input_file}", verbose)
                df = read_data_file(input_file)
                log(f"[ROWS BEFORE] {len(df):,}", verbose)

                cleaned_df, summary = remove_id_duplicates_for_dataframe(df, input_file=input_file)
                summary["county"] = county_name
                summary["output_file"] = str(output_file)
                summaries.append(summary)

                save_csv(cleaned_df, output_file)

                log(f"[DUPLICATE GROUPS] {summary['duplicate_id_groups']:,}", verbose)
                log(f"[REMOVED ROWS]     {summary['removed_duplicate_rows']:,}", verbose)
                log(f"[ROWS AFTER]       {summary['rows_after']:,}", verbose)
                if summary.get("blank_id_rows_kept", 0):
                    log(f"[WARN] Blank id rows kept: {summary['blank_id_rows_kept']:,}", verbose)
                log(f"[MERGED LIST COLS] {summary.get('merged_list_columns', '')}", verbose)
                log(f"[SAVE] {output_file}", verbose)

            except Exception as exc:
                error_summary = {
                    "status": "error",
                    "county": county_name,
                    "input_file": str(input_file),
                    "output_file": str(output_file),
                    "total_rows_before": 0,
                    "rows_after": 0,
                    "removed_duplicate_rows": 0,
                    "error": str(exc),
                    "traceback": traceback.format_exc(limit=5),
                }
                summaries.append(error_summary)
                log(f"[ERROR] {county_name} failed for {input_file.name}: {exc}", True)

    if WRITE_SUMMARY_CSV:
        summary_path = output_root / "remove_id_duplicates_summary.csv"
        pd.DataFrame(summaries).to_csv(summary_path, index=False, encoding="utf-8-sig")
        log("\n" + "-" * 90, verbose)
        log(f"[SUMMARY CSV] {summary_path}", verbose)

    return summaries


def main() -> None:
    input_root = Path(INPUT_ROOT).resolve()
    output_root = Path(OUTPUT_ROOT).resolve()
    verbose = not QUIET

    summaries = process_parent_folder(input_root=input_root, output_root=output_root, verbose=verbose)

    ok_summaries = [s for s in summaries if s.get("status") == "ok"]
    error_summaries = [s for s in summaries if s.get("status") == "error"]
    skipped_summaries = [s for s in summaries if str(s.get("status", "")).startswith("skipped")]

    total_before = sum(int(s.get("total_rows_before", 0) or 0) for s in ok_summaries)
    total_after = sum(int(s.get("rows_after", 0) or 0) for s in ok_summaries)
    total_removed = sum(int(s.get("removed_duplicate_rows", 0) or 0) for s in ok_summaries)
    total_duplicate_groups = sum(int(s.get("duplicate_id_groups", 0) or 0) for s in ok_summaries)

    if verbose:
        print("\n" + "=" * 90)
        print("[DONE] ID duplicate removal completed.")
        print(f"Files/counties completed : {len(ok_summaries):,}/{len(summaries):,}")
        print(f"Skipped                 : {len(skipped_summaries):,}")
        print(f"Errored                 : {len(error_summaries):,}")
        print(f"Rows before             : {total_before:,}")
        print(f"Rows after              : {total_after:,}")
        print(f"Duplicate groups        : {total_duplicate_groups:,}")
        print(f"Rows removed            : {total_removed:,}")
        print(f"Output root             : {output_root}")
        print("=" * 90)


if __name__ == "__main__":
    main()
