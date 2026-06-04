#!/usr/bin/env python
"""
Pipeline step 2: remap CAD parcel files to essential columns and add list fields.

Recommended script name:
    2_remap_essential_fields_add_lists_no_arg.py

Run after:
    1_fill_missing_legal_acreage_no_arg.py

What it does:
- Reads the county folders created by the legal-acreage-fill step.
- Detects the source columns for the essential output fields.
- Builds a clean, standardized output with these columns:
    id
    full_name
    legal_address
    mailing_address
    acres
    city
    zip
    state
    legal_acreage_filled_by_script
    Empty_Legal_Acreage
    market_value_list
    owner_tax_year_list
    deed_date_list
- Saves one processed CSV per county under OUTPUT_ROOT.
- Writes a root-level CSV summary showing detected mappings and missing counts.

Notes:
- This script does not call OpenAI. It uses deterministic column-candidate matching.
- Edit the CONFIG values below before running, or override INPUT_ROOT / OUTPUT_ROOT
  with environment variables.

Requirements:
    pip install pandas openpyxl
"""

from __future__ import annotations

import os
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd


# =============================================================================
# CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# This should match OUTPUT_ROOT from 1_fill_missing_legal_acreage_no_arg.py.
DEFAULT_INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_2_legal_acreage_filled"

# New pipeline output folder.
DEFAULT_OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\all_parcels_including_no_acreage\step_3_full_data_essential_columns"

# You can edit the defaults above, or override them with environment variables.
INPUT_ROOT = os.environ.get("CAD_REMAP_INPUT_ROOT") or os.environ.get("INPUT_ROOT") or DEFAULT_INPUT_ROOT
OUTPUT_ROOT = os.environ.get("CAD_REMAP_OUTPUT_ROOT") or os.environ.get("OUTPUT_ROOT") or DEFAULT_OUTPUT_ROOT

# Always writing CSV keeps the downstream cleaning steps simple and consistent.
OUTPUT_SUFFIX = ".csv"

# If True, writes a root-level summary CSV with mapping and missing-field counts.
WRITE_SUMMARY_CSV = True

# Minimal terminal output.
QUIET = False

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls"}

OUTPUT_COLUMNS = [
    "id",
    "full_name",
    "legal_address",
    "mailing_address",
    "acres",
    "city",
    "zip",
    "state",
    "legal_acreage_filled_by_script",
    "Empty_Legal_Acreage",
    "market_value_list",
    "owner_tax_year_list",
    "deed_date_list",
]


# =============================================================================
# Column candidates
# =============================================================================

DIRECT_FIELD_CANDIDATES: Dict[str, List[str]] = {
    # Prefer text-safe property IDs when available; fall back to numeric/common CAD IDs.
    "id": [
        "prop_id_text",
        "prop_id",
        "property_id",
        "property id",
        "property_number",
        "property number",
        "property_no",
        "property no",
        "parcel_id",
        "parcel id",
        "account",
        "account_no",
        "account number",
        "quickrefid",
        "quick_ref_id",
        "pid",
        "id",
        "geo_id",
        "map_id",
    ],
    "full_name": [
        "file_as_name",
        "owner_name",
        "owner name",
        "owner",
        "full_name",
        "full name",
        "name",
        "taxpayer_name",
        "taxpayer name",
        "owner1",
        "owner_1",
    ],
    # Use legal_acreage from the previous step first.
    "acres": [
        "legal_acreage",
        "legal acreage",
        "legal_acres",
        "legal acres",
        "acreage",
        "acres",
        "acre",
    ],
    # Property/situs city, state, zip.
    "city": [
        "situs_city",
        "situs city",
        "property_city",
        "property city",
        "prop_city",
        "site_city",
        "site city",
        "city",
    ],
    "zip": [
        "situs_zip",
        "situs zip",
        "situs_zip_code",
        "property_zip",
        "property zip",
        "prop_zip",
        "site_zip",
        "site zip",
        "zipcode",
        "zip_code",
    ],
    "state": [
        "situs_state",
        "situs state",
        "property_state",
        "property state",
        "prop_state",
        "site_state",
        "site state",
        "state",
    ],
    # Legal-acreage audit fields created by step 1 and preserved through the pipeline.
    "legal_acreage_filled_by_script": [
        "legal_acreage_filled_by_script",
        "legal acreage filled by script",
        "acreage_filled_by_script",
        "filled_by_script",
    ],
    "Empty_Legal_Acreage": [
        "Empty_Legal_Acreage",
        "empty_legal_acreage",
        "empty legal acreage",
    ],
    # New requested fields.
    "market_value_list": [
        "market",
        "market_value",
        "market value",
        "market_val",
        "mkt_val",
        "mkt value",
        "total_market_value",
        "total market value",
        "total_market",
        "appraised_value",
        "appraised value",
        "assessed_value",
        "assessed value",
    ],
    "owner_tax_year_list": [
        "owner_tax_yr",
        "owner_tax_year",
        "owner tax year",
        "tax_year",
        "tax year",
        "tax_yr",
        "year",
    ],
    "deed_date_list": [
        "Deed_Date",
        "deed_date",
        "deed date",
        "last_deed_date",
        "last deed date",
        "sale_date",
        "sale date",
        "recording_date",
        "recording date",
        "deed_dt",
    ],
}

LEGAL_ADDRESS_DIRECT_CANDIDATES = [
    "legal_address",
    "legal address",
    "situs_address",
    "situs address",
    "property_address",
    "property address",
    "prop_address",
    "site_address",
    "site address",
]

LEGAL_ADDRESS_COMPONENT_SPECS: List[Tuple[str, List[str]]] = [
    ("number", ["situs_num", "situs number", "situs_number", "site_num", "street_number", "street number"]),
    ("prefix", ["situs_street_prefx", "situs_street_prefix", "situs prefix", "street_prefix", "prefix"]),
    ("street", ["situs_street", "situs street", "situs_street_name", "site_street", "street_name", "street"]),
    ("suffix", ["situs_street_sufix", "situs_street_suffix", "situs suffix", "street_suffix", "suffix"]),
    ("unit", ["situs_unit", "situs unit", "unit", "apt", "apartment", "suite"]),
    ("city", ["situs_city", "situs city", "property_city", "site_city"]),
    ("state", ["situs_state", "situs state", "property_state", "site_state"]),
    ("zip", ["situs_zip", "situs zip", "situs_zip_code", "property_zip", "site_zip"]),
]

MAILING_ADDRESS_DIRECT_CANDIDATES = [
    "mailing_address",
    "mailing address",
    "mail_address",
    "mail address",
    "mail_addr",
    "owner_address",
    "owner address",
]

MAILING_ADDRESS_COMPONENT_SPECS: List[Tuple[str, List[str]]] = [
    ("line1", ["addr_line1", "addr line1", "addr_1", "address1", "address_1", "mail_addr1", "mailing_address_1"]),
    ("line2", ["addr_line2", "addr line2", "addr_2", "address2", "address_2", "mail_addr2", "mailing_address_2"]),
    ("line3", ["addr_line3", "addr line3", "addr_3", "address3", "address_3", "mail_addr3", "mailing_address_3"]),
    ("city", ["addr_city", "mail_city", "mailing_city", "owner_city"]),
    ("state", ["addr_state", "mail_state", "mailing_state", "owner_state"]),
    ("zip", ["zip", "addr_zip", "mail_zip", "mailing_zip", "owner_zip", "zip_code"]),
]


# =============================================================================
# Helpers
# =============================================================================

def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_column_name(name: Any) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "null", "n/a", "na", "--", "unknown"}


def value_to_text(value: Any) -> str:
    """Convert pandas/numpy values to clean text without losing IDs where possible."""
    if is_blank(value):
        return ""

    # Pandas timestamps from Excel date columns.
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return ""
        if value.hour == 0 and value.minute == 0 and value.second == 0:
            return value.strftime("%Y-%m-%d")
        return value.strftime("%Y-%m-%d %H:%M:%S")

    text = str(value).strip()

    # Clean common Excel/pandas string representations.
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^(.+?)\s+00:00:00$", r"\1", text)

    # Turn 12345.0 into 12345 for IDs, ZIPs, years, etc.
    if re.fullmatch(r"-?\d+\.0", text):
        text = text[:-2]

    return text


def clean_joined_text(parts: Sequence[Any]) -> str:
    values = [value_to_text(p) for p in parts]
    values = [v for v in values if v]
    text = " ".join(values)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def drop_fully_blank_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where every cell is blank after normal text cleanup."""
    if df.empty:
        return df
    nonblank_by_column = pd.DataFrame(index=df.index)
    for col in df.columns:
        nonblank_by_column[col] = df[col].map(lambda value: value_to_text(value) != "")
    keep_mask = nonblank_by_column.any(axis=1)
    return df.loc[keep_mask].reset_index(drop=True)


def find_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    if df.empty and len(df.columns) == 0:
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

    return None


def find_component_columns(df: pd.DataFrame, specs: Sequence[Tuple[str, Sequence[str]]]) -> List[str]:
    found: List[str] = []
    for _, candidates in specs:
        col = find_column(df, candidates)
        if col and col not in found:
            found.append(col)
    return found


def detect_address_columns(
    df: pd.DataFrame,
    direct_candidates: Sequence[str],
    component_specs: Sequence[Tuple[str, Sequence[str]]],
) -> List[str]:
    direct = find_column(df, direct_candidates)
    if direct:
        return [direct]
    return find_component_columns(df, component_specs)


def detect_mapping(df: pd.DataFrame) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {field: [] for field in OUTPUT_COLUMNS}

    for field, candidates in DIRECT_FIELD_CANDIDATES.items():
        col = find_column(df, candidates)
        if col:
            mapping[field] = [col]

    mapping["legal_address"] = detect_address_columns(
        df,
        direct_candidates=LEGAL_ADDRESS_DIRECT_CANDIDATES,
        component_specs=LEGAL_ADDRESS_COMPONENT_SPECS,
    )
    mapping["mailing_address"] = detect_address_columns(
        df,
        direct_candidates=MAILING_ADDRESS_DIRECT_CANDIDATES,
        component_specs=MAILING_ADDRESS_COMPONENT_SPECS,
    )

    return mapping


def build_output_series(df: pd.DataFrame, source_columns: Sequence[str]) -> pd.Series:
    if not source_columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")

    if len(source_columns) == 1:
        col = source_columns[0]
        return df[col].map(value_to_text).astype("object")

    # Combine address components into one text field, matching the style of the original pipeline.
    text_df = pd.DataFrame(index=df.index)
    for col in source_columns:
        text_df[col] = df[col].map(value_to_text)
    return text_df.apply(lambda row: clean_joined_text(row.tolist()), axis=1).astype("object")


def read_county_file(input_path: Path) -> pd.DataFrame:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        df = clean_dataframe(pd.read_csv(input_path, dtype=str, keep_default_na=False, low_memory=False))
        return drop_fully_blank_rows(df)
    if suffix in {".xlsx", ".xls"}:
        df = clean_dataframe(pd.read_excel(input_path, engine="openpyxl", dtype=object))
        return drop_fully_blank_rows(df)
    raise ValueError(f"Unsupported file type: {input_path}")


def save_output_file(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


def find_county_input_file(county_folder: Path) -> Optional[Path]:
    files = sorted(
        f for f in county_folder.iterdir()
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_SUFFIXES
        and not f.name.startswith("~$")
        and not f.name.lower().endswith("summary.csv")
    )
    if not files:
        return None

    preferred_keywords = [
        "legal_acreage_filled",
        "acreage_filled",
        "merged",
        "combined",
        "processed",
    ]
    for keyword in preferred_keywords:
        matches = [f for f in files if keyword in f.stem.lower()]
        if matches:
            return matches[0]

    return files[0]


def get_work_items(input_root: Path) -> List[Tuple[str, Path]]:
    county_folders = sorted([p for p in input_root.iterdir() if p.is_dir()])
    if county_folders:
        return [(p.name, p) for p in county_folders]

    direct_files = [
        p for p in input_root.iterdir()
        if p.is_file()
        and p.suffix.lower() in SUPPORTED_SUFFIXES
        and not p.name.startswith("~$")
    ]
    if direct_files:
        return [(input_root.name, input_root)]

    return []


def output_file_path_for(county_name: str, output_root: Path) -> Path:
    output_folder = output_root / county_name
    return output_folder / f"{county_name}_essential_columns{OUTPUT_SUFFIX}"


def remap_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    mapping = detect_mapping(df)
    output_df = pd.DataFrame(index=df.index)

    for field in OUTPUT_COLUMNS:
        output_df[field] = build_output_series(df, mapping.get(field, []))

    return output_df, mapping


def missing_count(series: pd.Series) -> int:
    return int(series.map(is_blank).sum())


def process_county(county_name: str, county_folder: Path, output_root: Path, verbose: bool = True) -> Dict[str, Any]:
    input_file = find_county_input_file(county_folder)
    if input_file is None:
        if verbose:
            print(f"[SKIP] {county_name}: no supported CSV/XLSX file found.")
        return {
            "status": "skipped_no_file",
            "county": county_name,
            "input_file": "",
            "output_file": "",
            "total_rows": 0,
            "error": "No supported CSV/XLSX file found in county folder.",
            "generated_at_utc": utc_now_text(),
        }

    output_file = output_file_path_for(county_name, output_root)

    if verbose:
        print(f"\n[COUNTY] {county_name}")
        print(f"[INPUT] {input_file}")

    try:
        df = read_county_file(input_file)
        output_df, mapping = remap_dataframe(df)
        save_output_file(output_df, output_file)

        summary: Dict[str, Any] = {
            "status": "ok",
            "county": county_name,
            "input_file": str(input_file),
            "output_file": str(output_file),
            "total_rows": len(output_df),
            "generated_at_utc": utc_now_text(),
        }

        for field in OUTPUT_COLUMNS:
            source_cols = mapping.get(field, [])
            summary[f"source_{field}"] = " + ".join(source_cols)
            summary[f"missing_{field}"] = missing_count(output_df[field])

        if verbose:
            print(f"[ROWS] {len(output_df):,}")
            print("[MAPPING]")
            for field in OUTPUT_COLUMNS:
                source = summary.get(f"source_{field}") or "NOT FOUND"
                missing = summary.get(f"missing_{field}", 0)
                print(f"  {field:<22} <- {source} | missing: {missing:,}")
            print(f"[SAVE] {output_file}")

        return summary

    except Exception as exc:
        if verbose:
            print(f"[ERROR] {county_name}: {exc}")
        return {
            "status": "error",
            "county": county_name,
            "input_file": str(input_file),
            "output_file": str(output_file),
            "total_rows": 0,
            "error": str(exc),
            "traceback": traceback.format_exc(limit=5),
            "generated_at_utc": utc_now_text(),
        }


def process_parent_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(f"Input root folder does not exist or is not a directory: {input_root}")

    output_root.mkdir(parents=True, exist_ok=True)
    work_items = get_work_items(input_root)
    summaries: List[Dict[str, Any]] = []

    if verbose:
        print("=" * 80)
        print("[START] Remapping parcel files to essential fields")
        print(f"[INPUT ROOT] {input_root}")
        print(f"[OUTPUT ROOT] {output_root}")
        print(f"[COUNTY FOLDERS] {len(work_items):,}")
        print("=" * 80)

    for county_name, county_folder in work_items:
        summaries.append(process_county(county_name, county_folder, output_root, verbose=verbose))

    if WRITE_SUMMARY_CSV:
        summary_path = output_root / "remap_essential_fields_summary.csv"
        pd.DataFrame(summaries).to_csv(summary_path, index=False, encoding="utf-8-sig")
        if verbose:
            print("\n" + "-" * 80)
            print(f"[SUMMARY CSV] {summary_path}")

    return summaries


def main() -> None:
    input_root = Path(INPUT_ROOT).resolve()
    output_root = Path(OUTPUT_ROOT).resolve() if OUTPUT_ROOT else input_root.with_name(input_root.name + "_essential_columns")
    verbose = not QUIET

    summaries = process_parent_folder(input_root=input_root, output_root=output_root, verbose=verbose)

    ok = [s for s in summaries if s.get("status") == "ok"]
    skipped = [s for s in summaries if str(s.get("status", "")).startswith("skipped")]
    errors = [s for s in summaries if s.get("status") == "error"]
    total_rows = sum(int(s.get("total_rows", 0) or 0) for s in ok)

    if verbose:
        print("\n" + "=" * 80)
        print("[DONE] Essential-field remap completed.")
        print(f"Counties completed : {len(ok):,}/{len(summaries):,}")
        print(f"Counties skipped   : {len(skipped):,}")
        print(f"Counties failed    : {len(errors):,}")
        print(f"Rows written       : {total_rows:,}")
        print("=" * 80)


if __name__ == "__main__":
    main()
