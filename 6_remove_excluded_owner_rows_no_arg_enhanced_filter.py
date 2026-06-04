#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Pipeline step 6: remove non-needed owner rows after mailing-address rollup.

Recommended script name:
    6_remove_university_rows_no_arg.py

Run after:
    5_rollup_by_mailing_address_add_lists_no_arg.py

What it does:
- Reads the county folders produced by 5_rollup_by_mailing_address_add_lists_no_arg.py.
- Removes rows where full_name matches configured non-needed-owner keywords.
  The match is case-insensitive and uses whole-word / whole-phrase matching by default.
- Preserves all existing columns, including aligned parcel-list fields such as:
    id_list
    acres_list
    market_value_list
    owner_tax_year_list
    deed_date_list
    legal_acreage_filled_by_script
    Empty_Legal_Acreage
- Saves cleaned files into a new output root while mirroring the county-folder structure.
- Prints clear progress while running.
- Writes a root-level summary CSV.
- Optionally writes an audit CSV containing the removed rows for review.

Notes:
- This is an enhanced version of the prior remove-university script.
  It now filters additional institutional / non-needed owner names such as
  churches, hospitals, schools, community-development entities, and county-owned rows.
- Because this runs after mailing-address rollup, full_name may look like:
      MA#ABC123 | OWNER NAME (+2)
  The script still checks that final full_name value.

Requirements:
    pip install pandas openpyxl

No command-line arguments are required. Edit CONFIG values below if needed.
You can also override INPUT_ROOT and OUTPUT_ROOT with these environment variables:
    CAD_REMOVE_UNIVERSITY_INPUT_ROOT
    CAD_REMOVE_UNIVERSITIES_INPUT_ROOT
    CAD_REMOVE_UNIVERSITY_OUTPUT_ROOT
    CAD_REMOVE_UNIVERSITIES_OUTPUT_ROOT
    INPUT_ROOT
    OUTPUT_ROOT
"""

from __future__ import annotations

import os
import json
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# This should match OUTPUT_ROOT from 5_rollup_by_mailing_address_add_lists_no_arg.py.
DEFAULT_INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_6_no_mailing_address_duplicates"

# New pipeline output folder.
DEFAULT_OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_7_no_excluded_owners"

INPUT_ROOT = (
    os.environ.get("CAD_REMOVE_UNIVERSITY_INPUT_ROOT")
    or os.environ.get("CAD_REMOVE_UNIVERSITIES_INPUT_ROOT")
    or os.environ.get("INPUT_ROOT")
    or DEFAULT_INPUT_ROOT
)
OUTPUT_ROOT = (
    os.environ.get("CAD_REMOVE_UNIVERSITY_OUTPUT_ROOT")
    or os.environ.get("CAD_REMOVE_UNIVERSITIES_OUTPUT_ROOT")
    or os.environ.get("OUTPUT_ROOT")
    or DEFAULT_OUTPUT_ROOT
)

# Column to inspect.
COL_FULL_NAME = "full_name"

# Owner-name keywords / phrases to remove from the pipeline.
# These are checked against full_name after mailing-address rollup.
# Examples now covered:
#   BAILEY COMMUNITY DEVELOPMENT INC -> community development
#   COUNTY OF HAYS TEXAS             -> county of / county
#   HAYS COUNTY                      -> county
REMOVE_OWNER_KEYWORDS = [
    "university",
    "church",
    "ministries",
    "hospital",
    "medical",
    "school",
    "community development",
    "county of",
    "county",
]

# Backwards-compatible alias for older log/summary wording and existing custom edits.
UNIVERSITY_KEYWORDS = REMOVE_OWNER_KEYWORDS

# Matching behavior.
CASE_INSENSITIVE = True
# False is safer for terms like "county" because it matches whole words / phrases only.
# Example: matches "HAYS COUNTY" but does not match a random longer token that merely contains "county".
MATCH_AS_SUBSTRING = False

# Output behavior.
OUTPUT_FILE_SUFFIX = "_no_excluded_owners"
OUTPUT_SUFFIX = ".csv"
WRITE_SUMMARY_CSV = True
SUMMARY_CSV_NAME = "remove_excluded_owner_rows_summary.csv"

# Optional review/audit files with the removed rows.
WRITE_REMOVED_ROWS_AUDIT = True
REMOVED_ROWS_AUDIT_FOLDER_NAME = "_removed_excluded_owner_rows_audit"
REMOVED_ROWS_AUDIT_SUFFIX = "_removed_university_rows_audit.csv"

# If True, files with names like *_summary.csv or audit files are ignored.
SKIP_SUMMARY_AND_AUDIT_FILES = True

# Minimal terminal output.
QUIET = False

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm"}


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
        "removed_non_needed_owner_rows",
        "removed_owner_filter",
        "removed_filtered_owner",
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


def audit_path_for(input_file: Path, county_folder: Optional[Path], input_root: Path, output_root: Path) -> Path:
    if county_folder is None:
        relative_folder = Path()
    else:
        try:
            relative_folder = county_folder.relative_to(input_root)
        except ValueError:
            relative_folder = Path(county_folder.name)

    county_name = input_file.stem if county_folder is None else (relative_folder.name if str(relative_folder) else county_folder.name)
    return (
        output_root
        / REMOVED_ROWS_AUDIT_FOLDER_NAME
        / relative_folder
        / f"{county_name}{REMOVED_ROWS_AUDIT_SUFFIX}"
    )


# =============================================================================
# Owner-name filtering logic
# =============================================================================

def _keyword_to_regex(keyword: str) -> str:
    """
    Convert a keyword / phrase into a safe regex.

    When MATCH_AS_SUBSTRING is False, matching uses whole-word / whole-phrase
    boundaries. This is safer for broad terms like "county".
    """
    keyword = str(keyword or "").strip()
    if not keyword:
        return ""

    # Escape punctuation but allow flexible spacing in multi-word phrases.
    escaped_parts = [re.escape(part) for part in re.split(r"\s+", keyword) if part]
    phrase_pattern = r"\s+".join(escaped_parts)

    if MATCH_AS_SUBSTRING:
        return phrase_pattern

    # Treat letters/numbers as word characters for boundary purposes.
    # This handles names with punctuation, pipes, hashes, and parentheses around them.
    return rf"(?<![A-Za-z0-9]){phrase_pattern}(?![A-Za-z0-9])"


def build_keyword_regex() -> str:
    patterns = [_keyword_to_regex(keyword) for keyword in UNIVERSITY_KEYWORDS if keyword and str(keyword).strip()]
    patterns = [pattern for pattern in patterns if pattern]
    if not patterns:
        raise ValueError("UNIVERSITY_KEYWORDS / REMOVE_OWNER_KEYWORDS is empty. Add at least one keyword.")
    return r"(?:" + "|".join(patterns) + r")"


def build_keyword_patterns() -> List[Tuple[str, re.Pattern]]:
    flags = re.IGNORECASE if CASE_INSENSITIVE else 0
    patterns: List[Tuple[str, re.Pattern]] = []
    seen = set()

    for keyword in UNIVERSITY_KEYWORDS:
        keyword_clean = str(keyword or "").strip()
        if not keyword_clean:
            continue
        key = keyword_clean.lower()
        if key in seen:
            continue
        seen.add(key)
        regex_text = _keyword_to_regex(keyword_clean)
        if regex_text:
            patterns.append((keyword_clean, re.compile(regex_text, flags=flags)))

    if not patterns:
        raise ValueError("No usable keyword patterns were built.")
    return patterns


def matched_keywords_for_name(name: Any, keyword_patterns: Optional[List[Tuple[str, re.Pattern]]] = None) -> List[str]:
    text = clean_cell(name)
    if not text:
        return []
    patterns = keyword_patterns if keyword_patterns is not None else build_keyword_patterns()
    return [keyword for keyword, pattern in patterns if pattern.search(text)]


def keyword_match_counts(df: pd.DataFrame, full_name_col: str, keyword_patterns: List[Tuple[str, re.Pattern]]) -> Dict[str, int]:
    counts = {keyword: 0 for keyword, _ in keyword_patterns}
    if df.empty or full_name_col not in df.columns:
        return counts

    for value in df[full_name_col].tolist():
        matched = matched_keywords_for_name(value, keyword_patterns=keyword_patterns)
        for keyword in matched:
            counts[keyword] = counts.get(keyword, 0) + 1

    return counts


def format_keyword_counts(counts: Dict[str, int]) -> str:
    active = [(keyword, count) for keyword, count in counts.items() if int(count or 0) > 0]
    active.sort(key=lambda item: (-int(item[1]), item[0].lower()))
    return " | ".join(f"{keyword}={count}" for keyword, count in active)


def sample_removed_names(removed_df: pd.DataFrame, full_name_col: str, max_names: int = 10) -> str:
    if removed_df.empty or full_name_col not in removed_df.columns:
        return ""

    names: List[str] = []
    seen = set()
    for value in removed_df[full_name_col].tolist():
        name = clean_cell(value)
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
        if len(names) >= max_names:
            break

    return " | ".join(names)


def add_removed_keyword_audit_column(
    removed_df: pd.DataFrame,
    full_name_col: str,
    keyword_patterns: List[Tuple[str, re.Pattern]],
) -> pd.DataFrame:
    """Add a helper column to the audit file showing which keyword(s) caused removal."""
    if removed_df.empty or full_name_col not in removed_df.columns:
        return removed_df

    out = removed_df.copy()
    matched_values = [
        " | ".join(matched_keywords_for_name(value, keyword_patterns=keyword_patterns))
        for value in out[full_name_col].tolist()
    ]

    audit_col = "removed_by_owner_filter_keywords"
    if audit_col in out.columns:
        out[audit_col] = matched_values
    else:
        out.insert(0, audit_col, matched_values)
    return out


def remove_university_rows_for_dataframe(df: pd.DataFrame, input_file: Path) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    before_rows = len(df)
    full_name_col = find_column(df, COL_FULL_NAME)
    if full_name_col is None:
        raise KeyError(
            f"Column '{COL_FULL_NAME}' not found in {input_file}. "
            f"Available columns: {list(df.columns)}"
        )

    keyword_patterns = build_keyword_patterns()

    if before_rows == 0:
        keyword_counts = {keyword: 0 for keyword, _ in keyword_patterns}
        summary = {
            "status": "ok_empty",
            "input_file": str(input_file),
            "filter_column": full_name_col,
            "keywords": ", ".join(UNIVERSITY_KEYWORDS),
            "case_insensitive": CASE_INSENSITIVE,
            "match_as_substring": MATCH_AS_SUBSTRING,
            "total_rows_before": 0,
            "rows_removed_non_needed_owner": 0,
            "rows_removed_university": 0,  # backwards-compatible summary column name
            "rows_after": 0,
            "removed_pct": 0.0,
            "keyword_match_counts_json": json.dumps(keyword_counts, ensure_ascii=False),
            "top_keyword_matches": "",
            "sample_removed_full_names": "",
            "generated_at_utc": utc_now_text(),
        }
        return df.copy(), df.iloc[0:0].copy(), summary

    pattern = build_keyword_regex()
    flags = re.IGNORECASE if CASE_INSENSITIVE else 0

    # Convert to string safely. keep_default_na=False in readers should preserve blanks,
    # but this also handles any DataFrame passed directly to the function.
    names = df[full_name_col].fillna("").astype(str)
    remove_mask = names.str.contains(pattern, case=not CASE_INSENSITIVE, regex=True, na=False, flags=flags)

    removed_df_raw = df[remove_mask].copy()
    cleaned_df = df[~remove_mask].copy()

    keyword_counts = keyword_match_counts(removed_df_raw, full_name_col=full_name_col, keyword_patterns=keyword_patterns)
    removed_df = add_removed_keyword_audit_column(
        removed_df_raw,
        full_name_col=full_name_col,
        keyword_patterns=keyword_patterns,
    )

    removed_rows = int(remove_mask.sum())
    after_rows = len(cleaned_df)
    removed_pct = round((removed_rows / before_rows) * 100.0, 4) if before_rows else 0.0

    summary = {
        "status": "ok",
        "input_file": str(input_file),
        "filter_column": full_name_col,
        "keywords": ", ".join(UNIVERSITY_KEYWORDS),
        "case_insensitive": CASE_INSENSITIVE,
        "match_as_substring": MATCH_AS_SUBSTRING,
        "total_rows_before": before_rows,
        "rows_removed_non_needed_owner": removed_rows,
        "rows_removed_university": removed_rows,  # backwards-compatible summary column name
        "rows_after": after_rows,
        "removed_pct": removed_pct,
        "keyword_match_counts_json": json.dumps(keyword_counts, ensure_ascii=False),
        "top_keyword_matches": format_keyword_counts(keyword_counts),
        "sample_removed_full_names": sample_removed_names(removed_df_raw, full_name_col=full_name_col, max_names=10),
        "generated_at_utc": utc_now_text(),
    }
    return cleaned_df, removed_df, summary


# =============================================================================
# Folder processing
# =============================================================================

FILE_SUMMARIES: List[Dict[str, Any]] = []


def process_file(
    input_file: Path,
    output_file: Path,
    removed_rows_audit_file: Path,
    county_name: str,
    verbose: bool = True,
) -> Dict[str, Any]:
    log(f"    [READ] {input_file}", verbose)
    df = read_data_file(input_file)
    log(f"    [ROWS BEFORE] {len(df):,}", verbose)

    cleaned_df, removed_df, summary = remove_university_rows_for_dataframe(df, input_file=input_file)
    summary["county"] = county_name
    summary["output_file"] = str(output_file)
    summary["removed_rows_audit_file"] = str(removed_rows_audit_file) if WRITE_REMOVED_ROWS_AUDIT and not removed_df.empty else ""

    write_output_csv(cleaned_df, output_file)

    if WRITE_REMOVED_ROWS_AUDIT and not removed_df.empty:
        write_output_csv(removed_df, removed_rows_audit_file)

    log(f"    [REMOVED NON-NEEDED OWNER ROWS] {summary['rows_removed_non_needed_owner']:,}", verbose)
    log(f"    [ROWS AFTER] {summary['rows_after']:,}", verbose)
    log(f"    [SAVE] {output_file}", verbose)
    if WRITE_REMOVED_ROWS_AUDIT and not removed_df.empty:
        log(f"    [AUDIT] {removed_rows_audit_file}", verbose)
    if summary.get("top_keyword_matches"):
        log(f"    [MATCH BREAKDOWN] {summary['top_keyword_matches']}", verbose)
    if summary.get("sample_removed_full_names"):
        log(f"    [SAMPLE REMOVED] {summary['sample_removed_full_names']}", verbose)

    return summary


def process_county_folder(county_folder: Path, input_root: Path, output_root: Path, verbose: bool = True) -> Dict[str, Any]:
    county_name = county_folder.name
    data_files = find_data_files(county_folder)

    stats: Dict[str, Any] = {
        "county": county_name,
        "files": 0,
        "before": 0,
        "after": 0,
        "removed": 0,
        "errors": 0,
    }

    log(f"\n[COUNTY] {county_name} ({len(data_files):,} file(s) found)", verbose)

    if not data_files:
        log("[SKIP] No supported CSV/Excel files found.", verbose)
        return stats

    for file_number, data_path in enumerate(data_files, start=1):
        output_file = output_path_for(data_path, county_folder, input_root, output_root)
        removed_rows_audit_file = audit_path_for(data_path, county_folder, input_root, output_root)

        try:
            log(f"  [FILE {file_number:,}/{len(data_files):,}] {data_path.name}", verbose)
            summary = process_file(
                input_file=data_path,
                output_file=output_file,
                removed_rows_audit_file=removed_rows_audit_file,
                county_name=county_name,
                verbose=verbose,
            )

            stats["files"] += 1
            stats["before"] += int(summary.get("total_rows_before", 0) or 0)
            stats["after"] += int(summary.get("rows_after", 0) or 0)
            stats["removed"] += int(summary.get("rows_removed_non_needed_owner", summary.get("rows_removed_university", 0)) or 0)
            FILE_SUMMARIES.append(summary)

        except Exception as exc:
            stats["errors"] += 1
            error_summary = {
                "status": "error",
                "county": county_name,
                "input_file": str(data_path),
                "output_file": str(output_file),
                "removed_rows_audit_file": "",
                "filter_column": COL_FULL_NAME,
                "keywords": ", ".join(UNIVERSITY_KEYWORDS),
                "total_rows_before": 0,
                "rows_removed_non_needed_owner": 0,
                "rows_removed_university": 0,
                "rows_after": 0,
                "removed_pct": 0.0,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=5),
                "generated_at_utc": utc_now_text(),
            }
            FILE_SUMMARIES.append(error_summary)
            log(f"    [ERROR] Failed to process {data_path.name}: {exc}", True)

    log(
        f"[COUNTY DONE] {county_name}: {stats['files']:,} file(s), "
        f"{stats['before']:,} -> {stats['after']:,} rows "
        f"(removed {stats['removed']:,}; errors {stats['errors']:,})",
        verbose,
    )

    return stats


def process_flat_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    data_files = find_data_files(input_root)
    log("=" * 90, verbose)
    log("[START] Removing non-needed owner rows from flat folder", verbose)
    log(f"[TIME] {now_text()}", verbose)
    log(f"[INPUT ROOT]  {input_root}", verbose)
    log(f"[OUTPUT ROOT] {output_root}", verbose)
    log(f"[FILES]       {len(data_files):,} file(s) found", verbose)
    log("=" * 90, verbose)

    stats = {
        "county": "",
        "files": 0,
        "before": 0,
        "after": 0,
        "removed": 0,
        "errors": 0,
    }

    for file_number, data_path in enumerate(data_files, start=1):
        output_file = output_path_for(data_path, None, input_root, output_root)
        removed_rows_audit_file = audit_path_for(data_path, None, input_root, output_root)

        try:
            log(f"\n[FILE {file_number:,}/{len(data_files):,}] {data_path.name}", verbose)
            summary = process_file(
                input_file=data_path,
                output_file=output_file,
                removed_rows_audit_file=removed_rows_audit_file,
                county_name="",
                verbose=verbose,
            )
            stats["files"] += 1
            stats["before"] += int(summary.get("total_rows_before", 0) or 0)
            stats["after"] += int(summary.get("rows_after", 0) or 0)
            stats["removed"] += int(summary.get("rows_removed_non_needed_owner", summary.get("rows_removed_university", 0)) or 0)
            FILE_SUMMARIES.append(summary)
        except Exception as exc:
            stats["errors"] += 1
            error_summary = {
                "status": "error",
                "county": "",
                "input_file": str(data_path),
                "output_file": str(output_file),
                "removed_rows_audit_file": "",
                "filter_column": COL_FULL_NAME,
                "keywords": ", ".join(UNIVERSITY_KEYWORDS),
                "total_rows_before": 0,
                "rows_removed_non_needed_owner": 0,
                "rows_removed_university": 0,
                "rows_after": 0,
                "removed_pct": 0.0,
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
    log("[START] Removing non-needed owner rows", verbose)
    log(f"[TIME] {now_text()}", verbose)
    log(f"[INPUT ROOT]  {input_root}", verbose)
    log(f"[OUTPUT ROOT] {output_root}", verbose)
    log(f"[FILTER]      {COL_FULL_NAME} matches owner keywords: {', '.join(UNIVERSITY_KEYWORDS)}", verbose)
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
        write_summary_csv(output_root, verbose=verbose)

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
    total_errors = sum(int(s.get("errors", 0) or 0) for s in county_stats)

    if verbose:
        print("\n" + "=" * 90)
        print("[DONE] Non-needed owner-row removal completed.")
        print(f"Total county groups processed   : {total_counties:,}")
        print(f"Total files processed           : {total_files:,}")
        print(f"Total rows BEFORE               : {total_before:,}")
        print(f"Total rows AFTER                : {total_after:,}")
        print(f"Total owner rows REMOVED        : {total_removed:,}")
        print(f"Files with errors               : {total_errors:,}")
        print(f"Output root                     : {output_root}")
        if WRITE_SUMMARY_CSV:
            print(f"Summary CSV                     : {output_root / SUMMARY_CSV_NAME}")
        if WRITE_REMOVED_ROWS_AUDIT:
            print(f"Removed-rows audit folder       : {output_root / REMOVED_ROWS_AUDIT_FOLDER_NAME}")
        print("=" * 90)


if __name__ == "__main__":
    main()
