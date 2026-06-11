#!/usr/bin/env python
"""
Merge all CSV/Excel files inside each county folder into a single file per county.

- Input:
    parent_input_folder/
        CountyA/
            file_1.xlsx
            file_2.csv
            ...
        CountyB/
            ...
- Output:
    parent_output_folder/
        CountyA/
            CountyA_merged.xlsx or CountyA_merged.csv
        CountyB/
            ...

Rules:
- If all files in a county are CSV → output a single CSV.
- If all are Excel → output a single Excel.
- If mixed → default to Excel (.xlsx) output.

Requirements:
    pip install pandas openpyxl
"""

from pathlib import Path
from typing import List, Optional, Literal
import pandas as pd

FileType = Literal["csv", "excel", "mixed"]

# =============================================================================
# ✅ CONFIG (EDIT THESE VARIABLES — no CLI arguments)
# =============================================================================

# Parent folder containing county subfolders
INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_acres_data_counties\_by_priority\priority_1\step_0_full_raw_data"

# Output root folder:
# - If you set this to None, it will default to "<input_root>_merged"
OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_acres_data_counties\_by_priority\priority_1\step_1_full_raw_data_combined"  # e.g. r"/path/to/output_folder"  or None

# Set to True for minimal output (same as --quiet)
QUIET = False

# =============================================================================
# Core helper functions
# =============================================================================

def detect_file_type(files: List[Path]) -> FileType:
    """Detect whether files are CSV, Excel, or mixed."""
    csv_exts = {".csv"}
    excel_exts = {".xlsx", ".xls"}

    has_csv = any(f.suffix.lower() in csv_exts for f in files)
    has_excel = any(f.suffix.lower() in excel_exts for f in files)

    if has_csv and not has_excel:
        return "csv"
    if has_excel and not has_csv:
        return "excel"
    return "mixed"


def load_file_to_dataframe(file_path: Path) -> pd.DataFrame:
    """Load a CSV/Excel file into a pandas DataFrame."""
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(file_path)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")

    return df


def merge_files_in_county(
    county_folder: Path,
    verbose: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Merge all CSV/Excel files in a county folder into a single DataFrame.

    Returns:
        - Merged DataFrame if there are valid files
        - None if no valid files were found
    """
    valid_exts = {".csv", ".xlsx", ".xls"}
    files = sorted(
        f for f in county_folder.iterdir()
        if f.is_file() and f.suffix.lower() in valid_exts
    )

    if not files:
        if verbose:
            print(f"[SKIP] No CSV/Excel files found in: {county_folder}")
        return None

    if verbose:
        print(f"[INFO] Found {len(files)} file(s) in county: {county_folder.name}")

    dfs = []
    for f in files:
        try:
            df = load_file_to_dataframe(f)
            dfs.append(df)
            if verbose:
                print(f"  - Loaded {f.name} with {len(df)} row(s)")
        except Exception as e:
            print(f"[WARN] Failed to read {f}: {e}")

    if not dfs:
        if verbose:
            print(f"[SKIP] No readable files in: {county_folder}")
        return None

    merged_df = pd.concat(dfs, ignore_index=True)
    if verbose:
        print(f"[INFO] Merged total rows for {county_folder.name}: {len(merged_df)}")

    return merged_df


def save_merged_dataframe(
    df: pd.DataFrame,
    county_name: str,
    output_root: Path,
    file_type: FileType,
    verbose: bool = True,
) -> Path:
    """
    Save the merged DataFrame for a county into the output folder.

    - If file_type == "csv": save as CSV
    - If file_type == "excel" or "mixed": save as Excel (.xlsx)
    """
    county_output_folder = output_root / county_name
    county_output_folder.mkdir(parents=True, exist_ok=True)

    if file_type == "csv":
        output_path = county_output_folder / f"{county_name}_merged.csv"
        df.to_csv(output_path, index=False)
    else:
        output_path = county_output_folder / f"{county_name}_merged.xlsx"
        df.to_excel(output_path, index=False)

    if verbose:
        print(f"[SAVE] {county_name}: {output_path}")

    return output_path


def process_parent_folder(
    input_root: Path,
    output_root: Path,
    verbose: bool = True,
) -> None:
    """
    Iterate all county folders in input_root, merge their files, and
    save merged outputs into output_root.
    """
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(
            f"Input root folder does not exist or is not a directory: {input_root}"
        )

    output_root.mkdir(parents=True, exist_ok=True)

    county_folders = sorted([p for p in input_root.iterdir() if p.is_dir()])

    if verbose:
        print(f"[START] Found {len(county_folders)} county folder(s) in '{input_root}'")
        print(f"[OUTPUT ROOT] {output_root}")
        print("-" * 80)

    processed_counties = 0

    for county_folder in county_folders:
        county_name = county_folder.name
        if verbose:
            print(f"\n[COUNTY] Processing: {county_name}")

        valid_exts = {".csv", ".xlsx", ".xls"}
        files = sorted(
            f for f in county_folder.iterdir()
            if f.is_file() and f.suffix.lower() in valid_exts
        )

        if not files:
            if verbose:
                print(f"[SKIP] No CSV/Excel files in {county_name}, skipping.")
            continue

        file_type = detect_file_type(files)

        merged_df = merge_files_in_county(county_folder, verbose=verbose)
        if merged_df is None:
            continue

        save_merged_dataframe(
            df=merged_df,
            county_name=county_name,
            output_root=output_root,
            file_type=file_type,
            verbose=verbose,
        )

        processed_counties += 1

    if verbose:
        print("\n" + "-" * 80)
        print(f"[DONE] Processed {processed_counties}/{len(county_folders)} county folder(s).")


def main() -> None:
    input_root = Path(INPUT_ROOT).resolve()

    if OUTPUT_ROOT:
        output_root = Path(OUTPUT_ROOT).resolve()
    else:
        output_root = input_root.with_name(input_root.name + "_merged")

    verbose = not QUIET

    process_parent_folder(
        input_root=input_root,
        output_root=output_root,
        verbose=verbose,
    )


if __name__ == "__main__":
    main()
