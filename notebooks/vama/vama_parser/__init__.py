"""VAMA Gemini Parser Package.

A professional package for parsing VAMA sales reports using Google Gemini API.
"""

from .gemini_parser import GeminiParser
from .config import TARGET_COLS, DEFAULT_GEMINI_MODEL
from .utils import parse_int, parse_share, extract_json, normalize_rows

__version__ = "1.0.0"
__all__ = [
    "GeminiParser",
    "TARGET_COLS",
    "DEFAULT_GEMINI_MODEL",
    "parse_int",
    "parse_share",
    "extract_json",
    "normalize_rows",
]
