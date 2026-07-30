"""Configuration and constants for VAMA Gemini parser."""

TARGET_COLS = [
    "maker",
    "model_name",
    "vama_classification",
    "seat",
    "monthly_north",
    "monthly_central",
    "monthly_south",
    "monthly_total",
    "monthly_share",
    "ytd_north",
    "ytd_central",
    "ytd_south",
    "ytd_total",
    "ytd_share",
    "source_table_index",
    "source_row_index",
]

DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
DEFAULT_TIMEOUT = 300
DEFAULT_ENV_PATH = "/Users/tuckeyhue@gmail.com/env/.env."
