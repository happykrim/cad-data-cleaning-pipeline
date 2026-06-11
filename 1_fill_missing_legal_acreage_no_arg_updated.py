#!/usr/bin/env python
"""
Pipeline step: fill missing legal_acreage cells after 0_data_merging_no_arg.py.

Recommended script name:
    1_fill_missing_legal_acreage_no_arg.py

What it does:
- Takes the parent folder that contains county subfolders.
- Finds each county's merged CSV/XLSX file.
- Finds the legal acreage column, legal description columns, and surface-area column.
- Fills only missing legal acreage cells.
- Adds Empty_Legal_Acreage to flag rows where legal_acreage was empty BEFORE filling.
- Hard-prioritizes legal description extraction first.
- Uses Shape__Area / 43,560 only as a fallback when no accepted acreage can be
  extracted from the legal description, matching the surface_area_to_acres_audit.py approach.
- Adds trace/debug columns so every filled value has a source, method, and fallback reason.
- Saves clean outputs into a new output root, preserving the county folder structure.

Requirements:
    pip install pandas openpyxl
"""

from __future__ import annotations

import json
import math
import re
import traceback
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =============================================================================
# ✅ CONFIG - EDIT THESE VARIABLES, NO CLI ARGUMENTS NEEDED
# =============================================================================

# This should be the OUTPUT_ROOT from 0_data_merging_no_arg.py.
# In your current pipeline, 0_data_merging_no_arg.py saves merged county files here.
INPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_acres_data_counties\_by_priority\priority_1\step_1_full_raw_data_combined"

# New pipeline output folder. Set to None to create "<INPUT_ROOT>_legal_acreage_filled".
OUTPUT_ROOT = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_acres_data_counties\_by_priority\priority_1\step_2_legal_acreage_filled"

# Only blank/missing legal acreage cells are filled by default.
OVERWRITE_EXISTING_LEGAL_ACREAGE = False

# Keep this False unless you intentionally want 0 acreage to be treated as missing.
TREAT_ZERO_AS_MISSING = False

# Surface area unit. Most CAD GIS Shape__Area exports are square feet.
SURFACE_UNIT = "square_feet"

# Round filled acreage values for cleaner downstream CSV/database usage.
ROUND_ACRES_DECIMALS = 6

# Legal-description fills can be made stricter by changing this to {"high", "medium"}.
ALLOWED_LEGAL_DESC_CONFIDENCES = {"high", "medium"}

# If True, adds helper/debug columns showing exactly how each missing acreage was filled.
ADD_TRACE_COLUMNS = True

# If True, saves a root-level CSV summary with county-level counts.
WRITE_SUMMARY_CSV = True

# Minimal terminal output.
QUIET = False

# Adds a boolean-style field to the final output.
# True means legal_acreage was empty/missing BEFORE this script filled it.
EMPTY_LEGAL_ACREAGE_COLUMN = "Empty_Legal_Acreage"

# Keep this True for the requested behavior: legal description is primary,
# and surface area is used only when legal description cannot produce an accepted value.
SURFACE_AREA_ONLY_WHEN_LEGAL_DESC_NOT_AVAILABLE = True


# =============================================================================
# Column detection settings
# =============================================================================

LEGAL_ACREAGE_COLUMN_CANDIDATES = [
    "legal_acreage",
    "legal acreage",
    "legal_acres",
    "legal acres",
    "legal_acre",
    "acreage",
]

LEGAL_DESC_COLUMN_CANDIDATES = [
    "legal_desc",
    "legal description",
    "legal_description",
    "legaldesc",
    "legal",
    "scraped_legal_description",
    "scraped legal description",
    "property_legal_description",
    "property legal description",
    "legal_description_full",
    "legal description full",
]

EXTRA_LEGAL_DESC_COLUMN_CANDIDATES = [
    "legal_desc2",
    "legal_desc3",
    "legal description 2",
    "legal description 3",
    "legal_description_2",
    "legal_description_3",
    "scraped_legal_description",
    "scraped legal description",
    "property_legal_description",
    "property legal description",
    "legal_description_full",
    "legal description full",
    "description",
]

SURFACE_AREA_COLUMN_CANDIDATES = [
    "Shape__Area",
    "Shape_Area",
    "shape__area",
    "shape_area",
    "surface_area",
    "surface area",
    "Shape.STArea()",
    "st_area",
]

SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xls"}


# =============================================================================
# Legal description parser copied/adapted from legal_desc_to_acres_audit.py
# =============================================================================

# =========================================================
@dataclass
class AcreCandidate:
    value: float
    raw_match: str
    method: str
    start: int
    end: int
    score: float
    in_parentheses: bool
    context: str
    warnings: List[str]


@dataclass
class ParsedAcreResult:
    acres_found: bool
    acres_value: Optional[float]
    confidence: str
    method: str
    chosen_raw_match: str
    candidate_count: int
    all_candidates_json: str
    warnings: str
    notes: str



# =========================================================
def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "null", "n/a", "na", "--", "unknown", "not available"}


def pct(numerator: float, denominator: float) -> float:
    return round((numerator / denominator * 100.0), 2) if denominator else 0.0


def round_or_none(value: Any, digits: int = 4) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        return round(float(value), digits)
    except Exception:
        return None


def format_num(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and math.isnan(value):
            return ""
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return ""


def html_escape(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def parse_number(value: Any) -> Optional[float]:
    """Parse normal numeric values from a column, not from legal description text."""
    if is_missing(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip().replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def parse_legal_desc_number(raw: str, unit: str = "acre") -> Optional[float]:
    """
    Parse a number extracted from legal description text.

    Special cases handled:
    - .50 -> 0.50
    - 093 AC -> 0.093 AC, because some descriptions omit the decimal point
    - 61,63950 AC -> 61.63950 AC, because some descriptions use a comma like a decimal separator
    - 43,560 SQ FT -> 43560 square feet, because square-foot values use comma thousands
    - 296. ACRES -> 296.0 ACRES, because some CAD descriptions keep a trailing decimal point
    - ACRES 44 60 -> 44.60 ACRES, because some exports lose the decimal point as a space
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    text = re.sub(r"\s+", " ", text)

    if unit == "acre" and re.fullmatch(r"\d+\s+\d{1,4}", text):
        whole, decimal = text.split()
        text = whole + "." + decimal

    if unit == "acre" and re.fullmatch(r"0\d+", text):
        try:
            return float("0." + text[1:])
        except Exception:
            return None

    if unit == "acre" and re.fullmatch(r"\d+\.", text):
        text = text + "0"

    if "," in text and "." not in text:
        parts = text.split(",")
        if unit == "acre" and len(parts) == 2 and len(parts[1]) != 3:
            text = parts[0] + "." + parts[1]
        else:
            text = "".join(parts)
    else:
        text = text.replace(",", "")

    if text.startswith("."):
        text = "0" + text

    try:
        return float(text)
    except Exception:
        return None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


# LEGAL DESCRIPTION PARSING
# =========================================================
# Acre number pattern used only inside acre-related patterns.
# It accepts normal decimals, .50 decimals, trailing-dot values like 296.,
# comma-decimal cases such as 61,63950 AC, and OCR/spacing cases such as ACRES 44 60.
ACRE_NUMBER_PATTERN = r"(?P<num>(?:\d+\s+\d{1,4}|\d+(?:,\d+)?(?:\.\d*)?|\.\d+))"
UNLABELED_ACRE_NUMBER_PATTERN = r"(?P<num>(?:\d+\.\d*|\.\d+))"
SQFT_NUMBER_PATTERN = r"(?P<num>(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)"

ACRE_PATTERNS: List[Tuple[str, re.Pattern[str], float]] = [
    (
        "acres_keyword_before_number",
        re.compile(rf"\b(?:ACRES?|ACRS)\s*[:#=]?\s*{ACRE_NUMBER_PATTERN}", re.IGNORECASE),
        105.0,
    ),
    (
        "ac_unit_before_number",
        # Handles descriptions like: LOT 37 AC 15.6440 or LOT 13 AC 0.499.
        # Require a decimal value so we do not accidentally parse "AC 43,560 SQ FT" as acres.
        re.compile(r"\b(?:AC|ACS?)\.?\s+(?P<num>(?:\d+\s+\d{1,4}|\d+\.\d*|\.\d+))", re.IGNORECASE),
        98.0,
    ),
    (
        "number_before_acres_unit",
        re.compile(rf"(?<![-#A-Za-z0-9.]){ACRE_NUMBER_PATTERN}\s+(?:ACRES?|ACS?\.?|AC\.?)\b", re.IGNORECASE),
        95.0,
    ),
    (
        "decimal_tight_before_ac_unit",
        # Handles compact values like 20.829AC but intentionally treats integer-tight values like 565AC as suspicious.
        re.compile(rf"(?<![-#A-Za-z0-9.]){ACRE_NUMBER_PATTERN}(?:ACRES?|ACS?\.?|AC\.?)\b", re.IGNORECASE),
        90.0,
    ),
]

NOISE_CONTEXT_RE = re.compile(
    r"@\s*MKT|\bMKT\b|\bHS\b|HOMESTEAD|\bMH\b|M/H|\bIMPS?\b|IMPROV|EXEMPT|TAXABLE|COMMON INTEREST|UND INT|UNDIVIDED|LIFE ESTATE",
    re.IGNORECASE,
)

SUBDIVISION_NAME_RE = re.compile(
    r"SUB(?:DIVISION|D)?\s*-?\s*$|ADDITION\s*-?\s*$|ESTATES?\s*-?\s*$",
    re.IGNORECASE,
)

SECONDARY_ACRE_CONTEXT_RE = re.compile(
    r"\b(?:INCL|INCLUDES?|INCLUDING|INC|AKA|ALSO|PLUS|EXCEPT|LESS|PART OF|PT OF)\b",
    re.IGNORECASE,
)

TRAILING_NOTE_RE = re.compile(
    r"^\s*(?:,?\s*(?:-|&|$)|,\s*\d+\s+[A-Z][A-Z0-9 ]*|,\s*(?:UNBRD|HOMESITE|M/H|MH|HUD|TITLE|HIGH SCHOOL|PARK AREA|OLD|MULTIPLE|ABST|IMPR|CABIN|GUEST|SHORT TERM|STREET|BUSINESS|CEMET|CHURCH)\b)",
    re.IGNORECASE,
)


def normalize_legal_desc_text(value: Any) -> str:
    if is_missing(value):
        return ""
    text = str(value)
    # Common OCR/typing issue: O.16 AC should be treated as 0.16 AC.
    text = re.sub(r"(?<![A-Z0-9])O\.(\d+)", r"0.\1", text, flags=re.IGNORECASE)
    # Normalize whitespace but preserve original punctuation for context.
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parenthesis_depth_at(text: str, index: int) -> int:
    return text[:index].count("(") - text[:index].count(")")


def get_context(text: str, start: int, end: int, radius: int = 60) -> str:
    return text[max(0, start - radius): min(len(text), end + radius)]


def has_immediate_noise_context(text: str, start: int, end: int) -> bool:
    local = text[max(0, start - 12): min(len(text), end + 24)]
    return bool(NOISE_CONTEXT_RE.search(local))


def infer_county_hint(input_path: Path, explicit_hint: str = "") -> str:
    if explicit_hint.strip():
        return explicit_hint.strip()
    # Try parent folder first, then file stem. Example: .../Hays/Hays_merged.csv -> Hays.
    parent = input_path.parent.name.strip()
    if parent and parent.lower() not in {".", "data", "reports", "category_1_merged", "category_2_merged"}:
        return parent
    stem = input_path.stem
    return re.sub(r"[_\-].*$", "", stem).strip()


def candidate_to_public_dict(candidate: AcreCandidate) -> Dict[str, Any]:
    return {
        "value": candidate.value,
        "raw_match": candidate.raw_match,
        "method": candidate.method,
        "score": candidate.score,
        "in_parentheses": candidate.in_parentheses,
        "warnings": candidate.warnings,
        "context": candidate.context,
    }


def add_acre_candidate(
    candidates: List[AcreCandidate],
    text: str,
    match: re.Match[str],
    method: str,
    base_score: float,
    county_hint: str,
) -> None:
    raw_number = match.group("num")
    value = parse_legal_desc_number(raw_number, unit="acre")
    if value is None:
        return

    start, end = match.span()
    raw_match = match.group(0)
    in_parentheses = parenthesis_depth_at(text, start) > 0
    context = get_context(text, start, end)
    score = base_score
    warnings: List[str] = []

    before = text[max(0, start - 40): start]
    after = text[end: min(len(text), end + 40)]

    # Strongly prefer explicit total/ttl acreage when it appears directly before the acre keyword.
    if re.search(r"\b(?:TTL|TOTAL)\s*$", before, re.IGNORECASE):
        score += 25
        warnings.append("total_or_ttl_acres_context")

    if in_parentheses:
        score -= 10
        warnings.append("inside_parentheses")

    # Do not punish an explicit "ACRES 95.54" just because a later parenthetical says "@ MKT".
    # Noise is strongest when the matched candidate itself is inside parentheses or is a shorter AC/HS/MKT value.
    if has_immediate_noise_context(text, start, end) and (in_parentheses or method in {"ac_unit_before_number", "decimal_tight_before_ac_unit"}):
        score -= 55
        warnings.append("immediate_noise_hs_mkt_exemption_or_interest")

    # If the description contains "AC IN HAYS CO", this is often the in-county portion.
    # The hint is optional and safe: it only applies when the phrase matches the hinted county.
    if county_hint and re.search(rf"\bIN\s+{re.escape(county_hint)}\s+CO(?:UNTY)?\b", after, re.IGNORECASE):
        score += 45
        warnings.append(f"in_{county_hint.lower()}_county_context")

    if value <= 0:
        score -= 50
        warnings.append("zero_or_negative")

    # Cases like "ACRES 9524" may be real, but they deserve review because many county descriptions omit decimals.
    if value > 1000 and float(value).is_integer() and "." not in raw_number and "," not in raw_number:
        score -= 20
        warnings.append("large_integer_without_decimal")

    if method == "number_before_acres_unit":
        # Avoid parsing lot or block numbers as acreage: LOT 37 AC 15.644 should not select 37 AC.
        if re.search(r"(?:LOT|LOTS|LT|BLOCK|BLK)\s*(?:PT\s+OF\s*)?$", before, re.IGNORECASE):
            score -= 70
            warnings.append("likely_lot_or_block_number_before_ac")
        if re.search(r"[+]\s*$", before):
            score -= 45
            warnings.append("plus_expression_before_candidate")
        if SUBDIVISION_NAME_RE.search(before):
            score -= 45
            warnings.append("likely_subdivision_name_acres")
        immediate_before = text[max(0, start - 28): start]
        if re.search(r"\b(?:INCL|INCLUDES?|INCLUDING|INC|AKA|ALSO|PLUS|EXCEPT|LESS|PART OF|PT OF)\s*$", immediate_before, re.IGNORECASE):
            score -= 65
            warnings.append("secondary_or_included_acreage_context")

    if method == "decimal_tight_before_ac_unit":
        if "." not in raw_number and not raw_number.startswith("."):
            score -= 80
            warnings.append("tight_integer_before_ac_suspicious")
        immediate_before = text[max(0, start - 28): start]
        if re.search(r"\b(?:INCL|INCLUDES?|INCLUDING|INC|AKA|ALSO|PLUS|EXCEPT|LESS|PART OF|PT OF)\s*$", immediate_before, re.IGNORECASE):
            score -= 65
            warnings.append("secondary_or_included_acreage_context")

    candidates.append(
        AcreCandidate(
            value=value,
            raw_match=raw_match,
            method=method,
            start=start,
            end=end,
            score=score,
            in_parentheses=in_parentheses,
            context=context,
            warnings=warnings,
        )
    )


def extract_candidates_from_legal_desc(text: str, county_hint: str = "") -> List[AcreCandidate]:
    candidates: List[AcreCandidate] = []
    if not text:
        return candidates

    for method, pattern, base_score in ACRE_PATTERNS:
        for match in pattern.finditer(text):
            add_acre_candidate(candidates, text, match, method, base_score, county_hint=county_hint)

    # Add unlabeled decimals as fallback candidates. These catch patterns like:
    # "AB 224 R HAILEY 13.82 (3.82 AC HS)" where the main acreage is unlabeled.
    # The score is boosted when the decimal appears in the county-specific trailing position:
    #   "DEAN, J&R SUBD LOT 1-PT, 24.42"
    #   "G E CO BLK XI 2., -HOMESITE-"
    #   "G E CO 700 MULTIPLE LOTS LOT GE597, GE598 & GE599, 20.5"
    for match in re.finditer(rf"(?<![#A-Za-z0-9/.]){UNLABELED_ACRE_NUMBER_PATTERN}(?![A-Za-z0-9/.])", text):
        start, end = match.span()
        if any(start >= c.start and end <= c.end for c in candidates):
            continue
        value = parse_legal_desc_number(match.group("num"), unit="acre")
        if value is None or value <= 0 or value > 10000:
            continue
        context = get_context(text, start, end)
        # Avoid dates, GEO identifiers, and explicit exemption percentages.
        if re.search(r"GEO\s*#|EX\s*%|TAXABLE\s*%|\b\d{1,2}/\d{1,2}/\d{2,4}\b", context, re.IGNORECASE):
            continue
        in_parentheses = parenthesis_depth_at(text, start) > 0
        score = 40.0
        warnings: List[str] = []
        if in_parentheses:
            score -= 10
            warnings.append("inside_parentheses")
        # If this unlabeled decimal is immediately followed by a parenthetical AC HS/MKT value, it is probably the primary acreage.
        if re.search(r"^\s*\(", text[end: end + 3]):
            score += 15
            warnings.append("followed_by_parenthetical_candidate")
        if re.search(r"^\s*\+", text[end: end + 3]):
            score += 10
            warnings.append("plus_expression_nearby")
        before = text[max(0, start - 45): start]
        after = text[end: min(len(text), end + 80)]
        if TRAILING_NOTE_RE.search(after) or end == len(text):
            score += 25
            warnings.append("county_tail_numeric_acres_pattern")
        if re.search(r"[,\s]$", before) and not re.search(r"\b(?:SUR|SVY|ABST|A\d{3,}|PID|HUD)\s*$", before, re.IGNORECASE):
            score += 8
            warnings.append("separated_from_identifier_context")
        if SECONDARY_ACRE_CONTEXT_RE.search(before):
            score -= 45
            warnings.append("secondary_or_included_acreage_context")
        if in_parentheses and has_immediate_noise_context(text, start, end):
            score -= 55
            warnings.append("immediate_noise_hs_mkt_exemption_or_interest")
        candidates.append(
            AcreCandidate(
                value=value,
                raw_match=match.group(0),
                method="unlabeled_decimal_fallback",
                start=start,
                end=end,
                score=score,
                in_parentheses=in_parentheses,
                context=context,
                warnings=warnings,
            )
        )

    # Square-foot fallback. Used only as a lower-confidence signal when no strong acre value exists.
    for match in re.finditer(rf"(?<![#A-Za-z0-9]){SQFT_NUMBER_PATTERN}\s*(?:SQ\s*FT|SQUARE\s*FEET|SF)\b", text, re.IGNORECASE):
        start, end = match.span()
        if any(start >= c.start and end <= c.end for c in candidates):
            continue
        sqft = parse_legal_desc_number(match.group("num"), unit="sqft")
        if sqft is None or sqft <= 0:
            continue
        candidates.append(
            AcreCandidate(
                value=sqft / 43560.0,
                raw_match=match.group(0),
                method="square_feet_fallback",
                start=start,
                end=end,
                score=30.0,
                in_parentheses=parenthesis_depth_at(text, start) > 0,
                context=get_context(text, start, end),
                warnings=["converted_from_square_feet"],
            )
        )

    # Dimension fallback: 40 x 79 FT or 12'x150'.
    dimension_pattern = re.compile(
        r"(?<!\d)(?P<w>\d+(?:\.\d+)?)\s*(?:'|FT|FEET)?\s*[xX×]\s*(?P<h>\d+(?:\.\d+)?)\s*(?:'|FT|FEET)\b",
        re.IGNORECASE,
    )
    for match in dimension_pattern.finditer(text):
        w = parse_legal_desc_number(match.group("w"), unit="sqft")
        h = parse_legal_desc_number(match.group("h"), unit="sqft")
        if w is None or h is None or w <= 0 or h <= 0:
            continue
        candidates.append(
            AcreCandidate(
                value=(w * h) / 43560.0,
                raw_match=match.group(0),
                method="dimensions_feet_fallback",
                start=match.start(),
                end=match.end(),
                score=30.0,
                in_parentheses=parenthesis_depth_at(text, match.start()) > 0,
                context=get_context(text, match.start(), match.end()),
                warnings=["converted_from_dimensions_in_feet"],
            )
        )

    # De-duplicate exact span/value/method duplicates.
    unique: List[AcreCandidate] = []
    seen = set()
    for candidate in candidates:
        key = (round(candidate.value, 8), candidate.start, candidate.end, candidate.method)
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return unique


def maybe_additive_acre_sum(text: str, candidates: List[AcreCandidate]) -> Optional[Tuple[float, List[AcreCandidate]]]:
    """
    Conservative additive handling.

    This catches simple descriptions like:
      ACRES 0.113 & NE 1/2 ALLEY 0.06 AC

    It intentionally avoids summing when:
    - the text has a TOTAL/TTL ACRES field, because the total should be selected directly
    - the second acreage is HS/MKT/EXEMPT noise
    - the text says the extra acreage is in another county
    """
    if re.search(r"\b(?:TTL|TOTAL)\s+ACRES?\b", text, re.IGNORECASE):
        return None

    strong = sorted(
        [
            c for c in candidates
            if c.method in {"acres_keyword_before_number", "number_before_acres_unit", "decimal_tight_before_ac_unit"}
            and c.score >= 90
            and not c.in_parentheses
        ],
        key=lambda c: c.start,
    )
    if len(strong) < 2:
        return None

    for idx in range(len(strong) - 1):
        first = strong[idx]
        second = strong[idx + 1]
        between = text[first.end: second.start]
        combined_context = get_context(text, first.start, second.end, radius=20)
        # Do not add candidates across combined legal-description fields. The pipe
        # separator is inserted by build_combined_desc_for_row and often means the
        # same legal description appeared in both CAD and scraped fields.
        if "|" in between:
            continue
        # If the later candidate is an explicit county total like ", ACRES 4.365",
        # do not add earlier component acreage to it. This prevents double counting
        # descriptions that list a component and then provide the official total.
        if second.method == "acres_keyword_before_number":
            continue
        if not re.search(r"(&|\+|\bAND\b)", between, re.IGNORECASE):
            continue
        if NOISE_CONTEXT_RE.search(combined_context):
            continue
        if re.search(r"\bIN\s+[A-Z]+\s+CO(?:UNTY)?\b", combined_context, re.IGNORECASE):
            continue
        if abs(first.value - second.value) <= 0.0001:
            continue
        return first.value + second.value, [first, second]

    return None


def choose_best_acre_candidate(text: str, candidates: List[AcreCandidate]) -> Tuple[Optional[float], List[AcreCandidate], str]:
    if not candidates:
        return None, [], "not_found"

    additive = maybe_additive_acre_sum(text, candidates)
    if additive is not None:
        value, chosen = additive
        return value, chosen, "additive_acres_sum"

    ranked = sorted(candidates, key=lambda c: (c.score, -c.start), reverse=True)
    best = ranked[0]
    return best.value, [best], best.method


def confidence_from_candidates(chosen: List[AcreCandidate], method: str, all_candidates: List[AcreCandidate]) -> str:
    if not chosen:
        return "none"
    if method in {"square_feet_fallback", "dimensions_feet_fallback", "unlabeled_decimal_fallback"}:
        return "medium" if chosen[0].score >= 55 else "low"
    if method == "additive_acres_sum":
        return "medium"
    score = max(c.score for c in chosen)
    if score >= 90:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def parse_legal_description_for_acres(value: Any, county_hint: str = "") -> ParsedAcreResult:
    text = normalize_legal_desc_text(value)
    if not text:
        return ParsedAcreResult(
            acres_found=False,
            acres_value=None,
            confidence="none",
            method="missing_legal_desc",
            chosen_raw_match="",
            candidate_count=0,
            all_candidates_json="[]",
            warnings="missing_legal_desc",
            notes="Legal description is empty or missing.",
        )

    candidates = extract_candidates_from_legal_desc(text, county_hint=county_hint)
    acres_value, chosen, method = choose_best_acre_candidate(text, candidates)

    public_candidates = [candidate_to_public_dict(c) for c in sorted(candidates, key=lambda c: (c.score, -c.start), reverse=True)]
    all_warnings: List[str] = []
    for candidate in candidates:
        all_warnings.extend(candidate.warnings)
    if len(candidates) > 1:
        all_warnings.append("multiple_acre_candidates")
    if candidates and all(c.in_parentheses for c in candidates):
        all_warnings.append("parenthetical_only_candidates")

    if acres_value is None:
        return ParsedAcreResult(
            acres_found=False,
            acres_value=None,
            confidence="none",
            method="not_found",
            chosen_raw_match="",
            candidate_count=len(candidates),
            all_candidates_json=json.dumps(public_candidates, ensure_ascii=False),
            warnings="; ".join(sorted(set(all_warnings))),
            notes="No acreage value could be confidently extracted from the legal description.",
        )

    confidence = confidence_from_candidates(chosen, method, candidates)
    chosen_raw_match = " + ".join(c.raw_match for c in chosen)
    chosen_warnings = []
    for c in chosen:
        chosen_warnings.extend(c.warnings)
    warnings = sorted(set(all_warnings + chosen_warnings))

    notes = "Parsed acreage from legal description."
    if method == "additive_acres_sum":
        notes = "Summed two additive acreage candidates joined by '&', '+', or 'AND'."
    elif method == "square_feet_fallback":
        notes = "No stronger acreage pattern was selected; converted square feet to acres."
    elif method == "dimensions_feet_fallback":
        notes = "No stronger acreage pattern was selected; converted feet dimensions to acres."
    elif method == "unlabeled_decimal_fallback":
        notes = "Used an unlabeled decimal because no stronger acre keyword candidate was available or reliable; county-tail numeric contexts are promoted to medium confidence."

    return ParsedAcreResult(
        acres_found=True,
        acres_value=round_or_none(acres_value, 6),
        confidence=confidence,
        method=method,
        chosen_raw_match=chosen_raw_match,
        candidate_count=len(candidates),
        all_candidates_json=json.dumps(public_candidates, ensure_ascii=False),
        warnings="; ".join(warnings),
        notes=notes,
    )




# =============================================================================
# Surface area conversion helpers copied/adapted from surface_area_to_acres_audit.py
# =============================================================================

def get_acre_conversion_factor(surface_unit: str) -> float:
    """Return multiplier that converts one surface unit into acres."""
    unit = surface_unit.strip().lower()
    factors = {
        "square_feet": 1.0 / 43560.0,
        "sqft": 1.0 / 43560.0,
        "sq_ft": 1.0 / 43560.0,
        "ft2": 1.0 / 43560.0,
        "square_meters": 1.0 / 4046.8564224,
        "sqm": 1.0 / 4046.8564224,
        "m2": 1.0 / 4046.8564224,
        "acres": 1.0,
        "acre": 1.0,
        "hectares": 2.4710538147,
        "hectare": 2.4710538147,
        "square_yards": 1.0 / 4840.0,
        "sq_yards": 1.0 / 4840.0,
        "yd2": 1.0 / 4840.0,
        "square_miles": 640.0,
        "sq_miles": 640.0,
        "mi2": 640.0,
    }
    if unit not in factors:
        valid = ", ".join(sorted(factors))
        raise ValueError(f"Unsupported surface unit: {surface_unit}. Valid options: {valid}")
    return factors[unit]


def convert_surface_area_to_acres(value: Any, surface_unit: str = SURFACE_UNIT) -> Optional[float]:
    surface_area = parse_number(value)
    if surface_area is None:
        return None
    factor = get_acre_conversion_factor(surface_unit)
    return round_or_none(surface_area * factor, ROUND_ACRES_DECIMALS)


# =============================================================================
# Pipeline helpers
# =============================================================================

def normalize_column_name(name: Any) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def find_column(df: pd.DataFrame, candidates: List[str], required: bool = False) -> Optional[str]:
    if df.empty:
        if required:
            raise ValueError("Cannot detect columns from an empty dataframe.")
        return None

    exact_map = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in exact_map:
            return exact_map[key]

    normalized_map = {normalize_column_name(c): c for c in df.columns}
    for candidate in candidates:
        key = normalize_column_name(candidate)
        if key in normalized_map:
            return normalized_map[key]

    if required:
        raise KeyError(
            "Could not find required column. "
            f"Candidates: {candidates}. Available columns: {list(df.columns)}"
        )
    return None


def find_extra_desc_columns(df: pd.DataFrame, main_legal_desc_col: Optional[str]) -> List[str]:
    columns: List[str] = []
    used = {main_legal_desc_col} if main_legal_desc_col else set()

    for candidate in EXTRA_LEGAL_DESC_COLUMN_CANDIDATES:
        col = find_column(df, [candidate], required=False)
        if col and col not in used and col not in columns:
            columns.append(col)

    return columns


def is_missing_legal_acreage(value: Any) -> bool:
    if is_missing(value):
        return True
    parsed = parse_number(value)
    if parsed is None:
        return True
    if TREAT_ZERO_AS_MISSING and parsed == 0:
        return True
    return False


def infer_county_hint_from_folder_or_file(county_folder: Path, input_file: Path) -> str:
    folder_hint = county_folder.name.strip()
    if folder_hint:
        return folder_hint
    stem = input_file.stem
    return re.sub(r"[_\-].*$", "", stem).strip()


def read_county_file(input_path: Path) -> pd.DataFrame:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return clean_dataframe(pd.read_csv(input_path, low_memory=False))
    if suffix in {".xlsx", ".xls"}:
        return clean_dataframe(pd.read_excel(input_path, engine="openpyxl"))
    raise ValueError(f"Unsupported file type: {input_path}")


def save_county_file(df: pd.DataFrame, input_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        return
    if suffix in {".xlsx", ".xls"}:
        # Always save Excel outputs as .xlsx for reliability.
        if output_path.suffix.lower() != ".xlsx":
            output_path = output_path.with_suffix(".xlsx")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="acreage_filled")
        return
    raise ValueError(f"Unsupported file type: {input_path}")


def find_county_merged_file(county_folder: Path) -> Optional[Path]:
    if not county_folder.exists() or not county_folder.is_dir():
        return None

    county_name = county_folder.name
    files = sorted(
        f for f in county_folder.iterdir()
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_SUFFIXES
        and not f.name.startswith("~$")
    )
    if not files:
        return None

    preferred_stems = [
        f"{county_name}_merged",
        f"{county_name.lower()}_merged",
        "merged",
    ]
    for preferred in preferred_stems:
        for f in files:
            if f.stem.lower() == preferred.lower():
                return f

    # 0_data_merging_no_arg.py should create one merged file per county.
    # If there is only one file, use it. If multiple exist, use the first stable sorted candidate.
    return files[0]


def build_combined_desc_for_row(row: pd.Series, main_col: Optional[str], extra_cols: List[str]) -> str:
    parts: List[str] = []
    for col in ([main_col] if main_col else []) + extra_cols:
        if col and col in row.index and not is_missing(row[col]):
            text = normalize_legal_desc_text(row[col])
            if text:
                parts.append(text)
    return " | ".join(parts)


def fill_missing_legal_acreage_for_dataframe(
    df: pd.DataFrame,
    county_hint: str,
    input_file: Path,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if df.empty:
        return df.copy(), {
            "status": "empty_file",
            "input_file": str(input_file),
            "total_rows": 0,
        }

    legal_acreage_col = find_column(df, LEGAL_ACREAGE_COLUMN_CANDIDATES, required=True)
    legal_desc_col = find_column(df, LEGAL_DESC_COLUMN_CANDIDATES, required=False)
    extra_desc_cols = find_extra_desc_columns(df, legal_desc_col)
    surface_area_col = find_column(df, SURFACE_AREA_COLUMN_CANDIDATES, required=False)

    out = df.copy()

    # IMPORTANT: this flag is calculated before any filling happens.
    # It lets later database/import steps count parcels whose CAD legal_acreage
    # value was originally empty, even after this script fills the acreage.
    original_missing_legal_acreage_mask = out[legal_acreage_col].map(is_missing_legal_acreage)
    out[EMPTY_LEGAL_ACREAGE_COLUMN] = original_missing_legal_acreage_mask.astype(bool)

    if ADD_TRACE_COLUMNS:
        out["legal_acreage_original"] = out[legal_acreage_col]
        out["legal_acreage_fill_source"] = "existing"
        out["legal_acreage_fill_value"] = None
        out["legal_acreage_filled_by_script"] = False
        out["legal_acreage_legal_desc_value"] = None
        out["legal_acreage_legal_desc_confidence"] = None
        out["legal_acreage_legal_desc_method"] = None
        out["legal_acreage_legal_desc_raw_match"] = None
        out["legal_acreage_legal_desc_warnings"] = None
        out["legal_acreage_surface_area_value"] = None
        out["legal_acreage_surface_area_fallback_reason"] = None
        out["legal_acreage_surface_unit"] = SURFACE_UNIT
        out["legal_acreage_fill_priority"] = "legal_desc_first_then_surface_area_fallback"
        out["legal_acreage_fill_notes"] = "existing legal acreage preserved"

    total_rows = len(out)
    missing_before_mask = original_missing_legal_acreage_mask
    if OVERWRITE_EXISTING_LEGAL_ACREAGE:
        target_mask = pd.Series([True] * total_rows, index=out.index)
    else:
        target_mask = missing_before_mask

    missing_before = int(missing_before_mask.sum())
    target_rows = int(target_mask.sum())

    filled_from_legal_desc = 0
    filled_from_surface_area = 0
    still_missing = 0
    legal_desc_parse_attempts = 0
    legal_desc_missing_or_empty = 0
    legal_desc_not_found = 0
    legal_desc_found_not_allowed = 0
    surface_area_attempts = 0
    surface_area_fallback_after_missing_desc = 0
    surface_area_fallback_after_no_legal_acres_found = 0
    surface_area_fallback_after_unaccepted_legal_acres = 0
    legal_desc_method_counter: Counter[str] = Counter()
    legal_desc_confidence_counter: Counter[str] = Counter()

    for idx in out.index[target_mask]:
        row = out.loc[idx]
        chosen_value: Optional[float] = None
        fill_source = "still_missing"
        fill_notes = "Could not fill from legal description or surface area."

        # 1) Legal description first.
        # This is the primary source. Surface area is not considered unless this
        # section fails to produce an accepted acreage value.
        legal_result: Optional[ParsedAcreResult] = None
        combined_desc = build_combined_desc_for_row(row, legal_desc_col, extra_desc_cols)
        legal_desc_status = "missing_legal_desc"

        if combined_desc:
            legal_desc_parse_attempts += 1
            legal_result = parse_legal_description_for_acres(combined_desc, county_hint=county_hint)
            if legal_result.acres_found and legal_result.acres_value is not None:
                legal_desc_method_counter[legal_result.method] += 1
                legal_desc_confidence_counter[legal_result.confidence] += 1
            if ADD_TRACE_COLUMNS:
                out.at[idx, "legal_acreage_legal_desc_value"] = legal_result.acres_value
                out.at[idx, "legal_acreage_legal_desc_confidence"] = legal_result.confidence
                out.at[idx, "legal_acreage_legal_desc_method"] = legal_result.method
                out.at[idx, "legal_acreage_legal_desc_raw_match"] = legal_result.chosen_raw_match
                out.at[idx, "legal_acreage_legal_desc_warnings"] = legal_result.warnings

            if (
                legal_result.acres_found
                and legal_result.acres_value is not None
                and legal_result.confidence in ALLOWED_LEGAL_DESC_CONFIDENCES
            ):
                chosen_value = round_or_none(legal_result.acres_value, ROUND_ACRES_DECIMALS)
                fill_source = "legal_desc"
                legal_desc_status = "accepted"
                fill_notes = f"Filled from legal description using method={legal_result.method}, confidence={legal_result.confidence}."
            elif legal_result.acres_found and legal_result.acres_value is not None:
                legal_desc_status = "found_but_not_allowed_by_confidence"
                legal_desc_found_not_allowed += 1
            else:
                legal_desc_status = "not_found"
                legal_desc_not_found += 1
        else:
            legal_desc_missing_or_empty += 1

        # 2) Surface area fallback.
        # Requested priority: use surface area only when legal description did not
        # produce an accepted acreage value.
        fallback_allowed = chosen_value is None
        if SURFACE_AREA_ONLY_WHEN_LEGAL_DESC_NOT_AVAILABLE:
            fallback_allowed = fallback_allowed and legal_desc_status != "accepted"

        if fallback_allowed and surface_area_col:
            surface_area_attempts += 1
            if legal_desc_status == "missing_legal_desc":
                fallback_reason = "legal description missing or empty"
                surface_area_fallback_after_missing_desc += 1
            elif legal_desc_status == "not_found":
                fallback_reason = "no acreage extracted from legal description"
                surface_area_fallback_after_no_legal_acres_found += 1
            elif legal_desc_status == "found_but_not_allowed_by_confidence":
                fallback_reason = "legal description acreage found but not accepted by confidence settings"
                surface_area_fallback_after_unaccepted_legal_acres += 1
            else:
                fallback_reason = f"legal description status={legal_desc_status}"

            converted_acres = convert_surface_area_to_acres(row.get(surface_area_col), surface_unit=SURFACE_UNIT)
            if ADD_TRACE_COLUMNS:
                out.at[idx, "legal_acreage_surface_area_value"] = converted_acres
                out.at[idx, "legal_acreage_surface_area_fallback_reason"] = fallback_reason

            if converted_acres is not None:
                chosen_value = converted_acres
                fill_source = "surface_area"
                fill_notes = f"Filled from {surface_area_col} using {SURFACE_UNIT} conversion only after legal description fallback reason: {fallback_reason}."

        # 3) Save result.
        if chosen_value is not None:
            out.at[idx, legal_acreage_col] = chosen_value
            if ADD_TRACE_COLUMNS:
                out.at[idx, "legal_acreage_fill_source"] = fill_source
                out.at[idx, "legal_acreage_fill_value"] = chosen_value
                out.at[idx, "legal_acreage_filled_by_script"] = True
                out.at[idx, "legal_acreage_fill_notes"] = fill_notes
            if fill_source == "legal_desc":
                filled_from_legal_desc += 1
            elif fill_source == "surface_area":
                filled_from_surface_area += 1
        else:
            still_missing += 1
            if ADD_TRACE_COLUMNS:
                out.at[idx, "legal_acreage_fill_source"] = "still_missing"
                out.at[idx, "legal_acreage_filled_by_script"] = False
                out.at[idx, "legal_acreage_fill_notes"] = fill_notes

    missing_after = int(out[legal_acreage_col].map(is_missing_legal_acreage).sum())

    summary = {
        "status": "ok",
        "input_file": str(input_file),
        "legal_acreage_column": legal_acreage_col,
        "legal_desc_column": legal_desc_col or "",
        "extra_legal_desc_columns": ", ".join(extra_desc_cols),
        "surface_area_column": surface_area_col or "",
        "surface_unit": SURFACE_UNIT,
        "total_rows": total_rows,
        "empty_legal_acreage_flag_column": EMPTY_LEGAL_ACREAGE_COLUMN,
        "empty_legal_acreage_true_rows": missing_before,
        "empty_legal_acreage_false_rows": int(total_rows - missing_before),
        "missing_legal_acreage_before": missing_before,
        "rows_targeted_for_fill": target_rows,
        "filled_from_legal_desc": filled_from_legal_desc,
        "filled_from_surface_area": filled_from_surface_area,
        "filled_total": filled_from_legal_desc + filled_from_surface_area,
        "still_missing_target_rows": still_missing,
        "missing_legal_acreage_after": missing_after,
        "existing_values_preserved": int(total_rows - target_rows) if not OVERWRITE_EXISTING_LEGAL_ACREAGE else 0,
        "fill_priority": "legal_desc_first_then_surface_area_fallback",
        "legal_desc_parse_attempts": legal_desc_parse_attempts,
        "legal_desc_missing_or_empty": legal_desc_missing_or_empty,
        "legal_desc_not_found": legal_desc_not_found,
        "legal_desc_found_not_allowed_by_confidence": legal_desc_found_not_allowed,
        "surface_area_attempts": surface_area_attempts,
        "surface_area_fallback_after_missing_desc": surface_area_fallback_after_missing_desc,
        "surface_area_fallback_after_no_legal_acres_found": surface_area_fallback_after_no_legal_acres_found,
        "surface_area_fallback_after_unaccepted_legal_acres": surface_area_fallback_after_unaccepted_legal_acres,
        "legal_desc_detected_pattern_counts": json.dumps(dict(legal_desc_method_counter.most_common()), ensure_ascii=False),
        "legal_desc_detected_confidence_counts": json.dumps(dict(legal_desc_confidence_counter.most_common()), ensure_ascii=False),
        "legal_desc_dominant_detected_pattern": legal_desc_method_counter.most_common(1)[0][0] if legal_desc_method_counter else "",
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return out, summary


def output_file_path_for(input_file: Path, county_folder: Path, output_root: Path) -> Path:
    county_output_folder = output_root / county_folder.name
    suffix = input_file.suffix.lower()
    if suffix == ".xls":
        suffix = ".xlsx"
    return county_output_folder / f"{input_file.stem}_legal_acreage_filled{suffix}"


def process_parent_folder(input_root: Path, output_root: Path, verbose: bool = True) -> List[Dict[str, Any]]:
    if not input_root.exists() or not input_root.is_dir():
        raise FileNotFoundError(f"Input root folder does not exist or is not a directory: {input_root}")

    output_root.mkdir(parents=True, exist_ok=True)
    county_folders = sorted([p for p in input_root.iterdir() if p.is_dir()])
    summaries: List[Dict[str, Any]] = []

    if verbose:
        print(f"[START] Found {len(county_folders)} county folder(s) in '{input_root}'")
        print(f"[OUTPUT ROOT] {output_root}")
        print("-" * 80)

    for county_folder in county_folders:
        county_name = county_folder.name
        if verbose:
            print(f"\n[COUNTY] Processing: {county_name}")

        input_file = find_county_merged_file(county_folder)
        if input_file is None:
            summary = {
                "status": "skipped_no_file",
                "county": county_name,
                "input_file": "",
                "total_rows": 0,
                "filled_total": 0,
                "error": "No CSV/Excel file found in county folder.",
            }
            summaries.append(summary)
            if verbose:
                print(f"[SKIP] No merged CSV/Excel file found in {county_name}")
            continue

        output_file = output_file_path_for(input_file, county_folder, output_root)
        county_hint = infer_county_hint_from_folder_or_file(county_folder, input_file)

        try:
            df = read_county_file(input_file)
            processed_df, summary = fill_missing_legal_acreage_for_dataframe(
                df=df,
                county_hint=county_hint,
                input_file=input_file,
            )
            summary["county"] = county_name
            summary["output_file"] = str(output_file)
            save_county_file(processed_df, input_path=input_file, output_path=output_file)
            summaries.append(summary)

            if verbose:
                print(f"[FILE] {input_file.name}")
                print(f"[ROWS] {summary.get('total_rows', 0):,}")
                print(f"[MISSING BEFORE] {summary.get('missing_legal_acreage_before', 0):,}")
                print(f"[EMPTY LEGAL ACREAGE FLAG] True={summary.get('empty_legal_acreage_true_rows', 0):,} | "
                      f"False={summary.get('empty_legal_acreage_false_rows', 0):,}")
                print(f"[PRIORITY] legal_desc first -> surface_area only as fallback")
                print(f"[FILLED] {summary.get('filled_total', 0):,} total | "
                      f"legal_desc={summary.get('filled_from_legal_desc', 0):,} | "
                      f"surface_area_fallback={summary.get('filled_from_surface_area', 0):,}")
                if summary.get("legal_desc_dominant_detected_pattern"):
                    print(f"[LEGAL DESC PATTERN] dominant={summary.get('legal_desc_dominant_detected_pattern')} | "
                          f"counts={summary.get('legal_desc_detected_pattern_counts')}")
                print(f"[SURFACE FALLBACK REASONS] missing_desc={summary.get('surface_area_fallback_after_missing_desc', 0):,} | "
                      f"no_legal_acres_found={summary.get('surface_area_fallback_after_no_legal_acres_found', 0):,} | "
                      f"unaccepted_legal_acres={summary.get('surface_area_fallback_after_unaccepted_legal_acres', 0):,}")
                print(f"[MISSING AFTER] {summary.get('missing_legal_acreage_after', 0):,}")
                print(f"[SAVE] {output_file}")

        except Exception as exc:
            error_summary = {
                "status": "error",
                "county": county_name,
                "input_file": str(input_file),
                "output_file": str(output_file),
                "total_rows": 0,
                "filled_total": 0,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=3),
            }
            summaries.append(error_summary)
            print(f"[ERROR] {county_name} failed: {exc}")

    if WRITE_SUMMARY_CSV:
        summary_path = output_root / "fill_missing_legal_acreage_summary.csv"
        pd.DataFrame(summaries).to_csv(summary_path, index=False, encoding="utf-8-sig")
        if verbose:
            print("\n" + "-" * 80)
            print(f"[SUMMARY] {summary_path}")

    return summaries


def main() -> None:
    input_root = Path(INPUT_ROOT).resolve()
    output_root = Path(OUTPUT_ROOT).resolve() if OUTPUT_ROOT else input_root.with_name(input_root.name + "_legal_acreage_filled")
    verbose = not QUIET

    summaries = process_parent_folder(
        input_root=input_root,
        output_root=output_root,
        verbose=verbose,
    )

    ok_summaries = [s for s in summaries if s.get("status") == "ok"]
    total_filled = sum(int(s.get("filled_total", 0) or 0) for s in ok_summaries)
    total_legal = sum(int(s.get("filled_from_legal_desc", 0) or 0) for s in ok_summaries)
    total_surface = sum(int(s.get("filled_from_surface_area", 0) or 0) for s in ok_summaries)
    total_missing_after = sum(int(s.get("missing_legal_acreage_after", 0) or 0) for s in ok_summaries)
    total_empty_flag_true = sum(int(s.get("empty_legal_acreage_true_rows", 0) or 0) for s in ok_summaries)
    total_empty_flag_false = sum(int(s.get("empty_legal_acreage_false_rows", 0) or 0) for s in ok_summaries)
    total_surface_missing_desc = sum(int(s.get("surface_area_fallback_after_missing_desc", 0) or 0) for s in ok_summaries)
    total_surface_no_legal_found = sum(int(s.get("surface_area_fallback_after_no_legal_acres_found", 0) or 0) for s in ok_summaries)
    total_surface_unaccepted = sum(int(s.get("surface_area_fallback_after_unaccepted_legal_acres", 0) or 0) for s in ok_summaries)

    if verbose:
        print("\n" + "=" * 80)
        print("[DONE] Missing legal acreage fill pipeline completed.")
        print("[RULE] legal_desc is primary; surface_area is fallback only.")
        print(f"Counties completed       : {len(ok_summaries):,}/{len(summaries):,}")
        print(f"Empty_Legal_Acreage=True : {total_empty_flag_true:,}")
        print(f"Empty_Legal_Acreage=False: {total_empty_flag_false:,}")
        print(f"Filled total             : {total_filled:,}")
        print(f"From legal desc          : {total_legal:,}")
        print(f"From surface fallback    : {total_surface:,}")
        print(f"  - missing desc         : {total_surface_missing_desc:,}")
        print(f"  - no legal acres found : {total_surface_no_legal_found:,}")
        print(f"  - unaccepted legal     : {total_surface_unaccepted:,}")
        print(f"Missing after            : {total_missing_after:,}")
        print("=" * 80)


if __name__ == "__main__":
    main()
