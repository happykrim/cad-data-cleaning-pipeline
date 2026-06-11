"""
Settings for property_type_enrichment_uc_chrome_no_args.py
=========================================================

Edit this file, then run:

    python property_type_enrichment_uc_chrome_no_args.py

This Chrome/Selenium version is intentionally conservative. It is designed to
use normal browsing behavior, slow request pacing, file-level checkpoints, and
cooldowns when a site blocks or fails. It does not try to bypass access controls.
"""

# =============================================================================
# Main folders
# =============================================================================

INPUT_FOLDER = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\property_type_enrichment"
OUTPUT_FOLDER = r"E:\dev\projects\2025\07\15\Texas Real Estate\all_data_counties_category_1_added_test\property_type_enrichment_output"

# =============================================================================
# Google Sheet tracker
# =============================================================================

GSHEET_URL = "https://docs.google.com/spreadsheets/d/1Y7eL0lfH3B_wAl1s1ARuM3LXpGOwyScBO5cyQcGpn-8/edit?gid=0#gid=0"
CREDENTIALS_JSON_FILE = r"earnest-smoke-451916-b5-c2d40ca80114.json"
WORKSHEET_NAME = None

DRY_RUN_SHEET_UPDATES = False
NO_SHEET = False

SHEET_HEADER_COUNTY = "County"
SHEET_HEADER_APPRAISAL_WEBSITE = "Appraisal Website"
SHEET_HEADER_HAS_LINK = "Has Link"
SHEET_HEADER_CATEGORY = "Category"
SHEET_HEADER_PROPERTY_TYPE_SCRAPED = "Property Type Scraped"
SHEET_HEADER_REAL_MINERAL_OTHER_COUNT = "Real/Mineral/Other Count"

# Fallback columns from the tracker layout shown in your screenshot:
# AG = 33, AH = 34
PROPERTY_TYPE_SCRAPED_FALLBACK_COL = 33
REAL_MINERAL_OTHER_COUNT_FALLBACK_COL = 34

# =============================================================================
# County selection
# =============================================================================

# Leave empty to process every eligible county folder found in INPUT_FOLDER.
# Example: COUNTIES_TO_RUN = ["Brazos", "Hays"]
COUNTIES_TO_RUN = []

# The script only processes counties where the tracker row has:
# - Category in this list
# - Has Link = Yes
# - Property Type Scraped empty, or partial if PROCESS_PARTIAL_STATUSES=True
ALLOWED_CATEGORY_VALUES = {"-1", "1", "category 1", "cat 1", "c1"}
PROCESS_PARTIAL_STATUSES = True

# =============================================================================
# Chrome / browser behavior
# =============================================================================

# Headless=False is usually more reliable for these BIS pages.
HEADLESS = False

# Keep this conservative. More browsers means more load on your machine and the county site.
MAX_COUNTY_WORKERS = 8
MAX_BROWSER_WORKERS_PER_COUNTY = 4

# Browser lifecycle. Rotating too often can look noisier, so default is high.
RESTART_BROWSER_EVERY_N_PAGES = 250

# Optional. Leave None unless you specifically want Chrome to reuse a profile.
# Example: CHROME_USER_DATA_DIR = r"E:\chrome_profiles\property_type_scraper"
CHROME_USER_DATA_DIR = None

# Optional fixed Chrome binary path. Leave None for default discovery.
CHROME_BINARY_LOCATION = None

# Chrome startup settings.
# Keep parallel Chrome startup serialized to avoid undetected_chromedriver patch races
# like: [WinError 183] Cannot create a file when that file already exists.
CHROME_START_ATTEMPTS = 3
CHROME_START_STAGGER_SECONDS = 2.5

# Turn this on only after closing all Chrome windows if undetected_chromedriver's
# cached driver gets corrupted. Leave False for normal runs.
CLEAR_UNDETECTED_CHROMEDRIVER_CACHE_ON_START = False

WINDOW_SIZE = "1366,900"
PAGE_LOAD_TIMEOUT_SECONDS = 45
ELEMENT_WAIT_SECONDS = 15

# =============================================================================
# Respectful pacing / cooldowns
# =============================================================================

# Random delay after each property page load per browser.
MIN_DELAY_SECONDS = 2.5
MAX_DELAY_SECONDS = 6.0

# Extra pause after every N pages in a browser.
LONG_PAUSE_EVERY_N_PAGES = 40
LONG_PAUSE_MIN_SECONDS = 45
LONG_PAUSE_MAX_SECONDS = 120

# When a site returns Property Not Found, captcha, access denied, or repeated errors.
COOLDOWN_AFTER_BLOCK_SECONDS = 180
MAX_CONSECUTIVE_FAILURES_PER_BROWSER = 8

# =============================================================================
# File behavior
# =============================================================================

SUPPORTED_INPUT_EXTENSIONS = {".csv", ".xlsx", ".xls"}
ENRICHED_SUFFIX = "__property_details_enriched"
OVERWRITE_OUTPUT = False

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

# For counties like Hays, the same id may need an R prefix.
# The script tests these in order and keeps the first page with a valid Property Details table.
PROPERTY_ID_URL_FORMATS = ["{id}", "R{id}"]

# Optional tax year. Leave None for the site's current/default year.
# Example: TAX_YEAR = "2026"
TAX_YEAR = None

# =============================================================================
# Output columns
# =============================================================================

SCRAPED_COLUMNS = [
    "scraped_property_url",
    "scraped_property_id_input",
    "scraped_property_id_page",
    "scraped_quick_ref_id_page",
    "scraped_property_type_raw",
    "scraped_property_type_normalized",
    "scraped_geographic_id",
    "scraped_property_use",
    "scraped_zoning",
    "scraped_situs_address",
    "scraped_map_id",
    "scraped_mapsco",
    "scraped_legal_description",
    "scraped_abstract_subdivision",
    "scraped_neighborhood",
    "scraped_owner_id",
    "scraped_owner_name",
    "scraped_agent",
    "scraped_mailing_address",
    "scraped_percent_ownership",
    "scraped_exemptions",
    "scrape_status",
    "scrape_error",
    "scraped_at",
]

# =============================================================================
# Logging
# =============================================================================

LOG_LEVEL = "INFO"
LOG_FILE = "property_type_enrichment_uc_chrome.log"

# =============================================================================
# Live save / resume behavior
# =============================================================================

# Save the enriched output file while rows are being scraped instead of waiting
# until the full file finishes.
LIVE_SAVE_ENABLED = True

# Save every N scraped rows. 1 = safest/resumable after every row, but slower for
# very large XLSX files. For CSV, 1 is usually fine. For XLSX, 5 or 10 can be faster.
LIVE_SAVE_EVERY_N_ROWS = 1

# Update the Google Sheet progress every N scraped rows, plus once at the end.
# Keeping this above 1 avoids hammering the Google Sheets API.
SHEET_UPDATE_EVERY_N_ROWS = 25

# Print terminal progress every N scraped rows.
TERMINAL_PROGRESS_EVERY_N_ROWS = 1

# If an output file already exists but has blank scrape_status rows, continue from
# the blank rows instead of skipping the file.
RESUME_INCOMPLETE_OUTPUT = True

# By default, do not retry rows already marked failed/not found/blocked. Set this
# to True when you intentionally want to retry failed rows.
RESCRAPE_FAILED_ROWS = False

# Write to a temporary output file first, then replace the real output file.
WRITE_OUTPUT_ATOMICALLY = True
