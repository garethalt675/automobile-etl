"""VAMA Gemini PDF Parser - Main parser class."""

import base64
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import (
    DEFAULT_GEMINI_MODEL,
    DEFAULT_SECRET_KEY,
    DEFAULT_SECRET_SCOPE,
    DEFAULT_TIMEOUT,
    TARGET_COLS,
)
from .utils import (
    extract_json,
    normalize_rows,
    parse_env,
    parse_int,
    parse_share,
    read_workspace_file,
)


class GeminiParser:
    """VAMA PDF parser using Google Gemini API."""

    def __init__(
        self,
        spark: SparkSession,
        dbutils,
        document_id: str,
        document_url: str,
        filename: str,
        report_year: int,
        report_month: str,
        report_start_date: str,
        report_end_date: str,
        output_table: str,
        gemini_model: str = DEFAULT_GEMINI_MODEL,
        env_workspace_path: str = None,
        gemini_api_key: str = None,
        secret_scope: str = DEFAULT_SECRET_SCOPE,
        secret_key: str = DEFAULT_SECRET_KEY,
        verbose: bool = True,
    ):
        """Initialize Gemini parser.

        Args:
            spark: SparkSession instance
            dbutils: Databricks utilities instance
            document_id: Unique document identifier
            document_url: URL to download PDF
            filename: Original filename
            report_year: Report year
            report_month: Report month (format: YYYY-MM)
            report_start_date: Report start date (format: YYYY-MM-DD)
            report_end_date: Report end date (format: YYYY-MM-DD)
            output_table: Target Delta table (catalog.schema.table)
            gemini_model: Gemini model name
            env_workspace_path: Path to .env file in workspace (legacy fallback)
            gemini_api_key: Gemini API key (if not provided, loaded from secret)
            secret_scope: Databricks secret scope holding the key
            secret_key: Databricks secret key name
            verbose: Enable verbose logging
        """
        self.spark = spark
        self.dbutils = dbutils
        self.document_id = document_id
        self.document_url = document_url
        self.filename = filename
        self.report_year = report_year
        self.report_month = report_month
        self.report_start_date = report_start_date
        self.report_end_date = report_end_date
        self.output_table = output_table
        self.gemini_model = gemini_model
        self.env_workspace_path = env_workspace_path
        self.secret_scope = secret_scope
        self.secret_key = secret_key
        self.verbose = verbose

        # Document metadata
        self.doc_meta = {
            "document_id": document_id,
            "document_url": document_url,
            "filename": filename,
            "report_year": report_year,
            "report_month": report_month,
            "report_start_date": report_start_date,
            "report_end_date": report_end_date,
        }

        # Load API key
        self.gemini_api_key = self._load_api_key(gemini_api_key)

        # State
        self.pdf_bytes = None
        self.rows = None
        self.enriched_rows = None
        self.timing = {}

    def _log(self, message: str):
        """Log message if verbose mode is enabled."""
        if self.verbose:
            print(message)

    def _load_api_key(self, api_key: Optional[str] = None) -> str:
        """Load Gemini API key from the Databricks secret, an env file, or the process env.

        The Databricks secret is checked first and is the place to rotate the key.
        The workspace .env. file is only a legacy fallback -- as of 2026-07-30 it
        still held a dead key, and because it used to be read first, rotating the
        secret had no effect on this pipeline.

        Args:
            api_key: Optional API key, wins over every lookup

        Returns:
            API key string

        Raises:
            ValueError: If API key cannot be found
        """
        if api_key:
            return api_key

        # Databricks secret -- authoritative source, rotate here
        if self.secret_scope and self.secret_key:
            try:
                value = self.dbutils.secrets.get(scope=self.secret_scope, key=self.secret_key)
                if value and value.strip():
                    self._log(
                        f"  Gemini key loaded from secret {self.secret_scope}/{self.secret_key} "
                        f"(length {len(value.strip())})"
                    )
                    return value.strip()
            except Exception as e:
                self._log(
                    f"Warning: secret {self.secret_scope}/{self.secret_key} unavailable "
                    f"({type(e).__name__}); falling back to env file"
                )

        # Legacy workspace env file
        if self.env_workspace_path:
            try:
                env = parse_env(read_workspace_file(self.env_workspace_path, self.dbutils))
                if "GEMINI_API_KEY" in env:
                    self._log(f"  Gemini key loaded from legacy env file {self.env_workspace_path}")
                    return env["GEMINI_API_KEY"]
            except Exception as e:
                self._log(f"Warning: Could not load env file {self.env_workspace_path}: {e}")

        # Try environment variable
        if "GEMINI_API_KEY" in os.environ:
            return os.environ["GEMINI_API_KEY"]

        raise ValueError(
            f"GEMINI_API_KEY not found. Provide via api_key parameter, "
            f"secret ({self.secret_scope}/{self.secret_key}), "
            f"env file ({self.env_workspace_path}), or environment variable."
        )

    def download_pdf(self) -> bytes:
        """Download PDF from document URL.

        Returns:
            PDF content as bytes

        Raises:
            requests.HTTPError: If download fails
        """
        start_time = time.time()
        self._log(f"  📥 Downloading PDF from: {self.document_url[:80]}...")
        
        resp = requests.get(self.document_url, timeout=120)
        resp.raise_for_status()
        self.pdf_bytes = resp.content
        
        elapsed = time.time() - start_time
        self.timing["download"] = elapsed
        self._log(f"  ✓ Downloaded {len(self.pdf_bytes):,} bytes in {elapsed:.2f}s")
        
        return self.pdf_bytes

    def call_gemini(self) -> List[dict]:
        """Call Gemini API to extract table data from PDF.

        Returns:
            List of extracted rows

        Raises:
            RuntimeError: If API call fails
            ValueError: If response is invalid
        """
        if not self.pdf_bytes:
            raise ValueError("PDF not downloaded. Call download_pdf() first.")

        start_time = time.time()
        self._log(f"  🔮 Calling Gemini {self.gemini_model}...")

        # Prepare prompt
        prompt = f"""
Extract ALL model-level rows from the attached VAMA sales report PDF.
Return ONLY valid compact JSON:
{{"cols":{json.dumps(TARGET_COLS)},"rows":[[...],[...]]}}

Rules:
- Rows must align exactly with these cols: {', '.join(TARGET_COLS)}.
- Include only actual model rows. Exclude subtotal, total, grand total, segment summary, and blank/non-model rows.
- Read the visual PDF table carefully; do not inherit wrong values across rowspans.
- Integers only for numeric sales fields; no commas. Missing blanks/dashes -> null.
- Shares can be strings like "10.4%" or null.
- source_table_index: visual detail table index starting at 0. source_row_index: visual row order in that table starting at 0.
- For Dec 2024, critical checks if present: CX-3 monthly_total must be 470; BT-50 monthly_total should be 286.
""".strip()

        # Base64 encode PDF
        base64_pdf = base64.b64encode(self.pdf_bytes).decode("ascii")

        # Prepare payload
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": "application/pdf", "data": base64_pdf}},
                    ],
                }
            ],
            "generationConfig": {
                "temperature": 0,
                "response_mime_type": "application/json",
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        # Call API. The key goes in a header, not the query string, so it cannot
        # leak through a logged or raised URL.
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent"
        resp = requests.post(
            url,
            json=payload,
            headers={"x-goog-api-key": self.gemini_api_key},
            timeout=DEFAULT_TIMEOUT,
        )

        if not resp.ok:
            raise RuntimeError(f"Gemini HTTP {resp.status_code}: {resp.text[:1000]}")

        # Parse response
        raw = resp.json()
        text = raw["candidates"][0]["content"]["parts"][0].get("text", "")
        obj = json.loads(extract_json(text))
        self.rows = normalize_rows(obj)

        if not self.rows:
            raise ValueError("Gemini returned no rows")

        elapsed = time.time() - start_time
        self.timing["gemini_api"] = elapsed
        self._log(f"  ✓ Gemini API completed in {elapsed:.2f}s ({len(self.rows)} rows extracted)")
        self._log(f"    - finishReason: {raw.get('candidates', [{}])[0].get('finishReason')}")
        self._log(f"    - usageMetadata: {json.dumps(raw.get('usageMetadata', {}), ensure_ascii=False)}")

        return self.rows

    def transform_rows(self) -> List[dict]:
        """Transform and enrich extracted rows.

        Returns:
            List of enriched rows ready for insertion
        """
        if not self.rows:
            raise ValueError("No rows to transform. Call call_gemini() first.")

        start_time = time.time()
        self._log(f"  🔄 Transforming {len(self.rows)} rows...")

        now = datetime.now(timezone.utc)
        self.enriched_rows = []

        for r in self.rows:
            rr = {**self.doc_meta, **r}

            # Parse integer fields
            for c in [
                "monthly_north",
                "monthly_central",
                "monthly_south",
                "monthly_total",
                "ytd_north",
                "ytd_central",
                "ytd_south",
                "ytd_total",
                "source_table_index",
                "source_row_index",
            ]:
                rr[c] = parse_int(rr.get(c))

            # Keep share fields as strings (changed from Decimal to avoid rescaling errors)
            monthly_share_val = rr.get("monthly_share")
            rr["monthly_share"] = str(monthly_share_val) if monthly_share_val is not None else None

            ytd_share_val = rr.get("ytd_share")
            rr["ytd_share"] = str(ytd_share_val) if ytd_share_val is not None else None

            rr["extracted_timestamp"] = now
            self.enriched_rows.append(rr)

        elapsed = time.time() - start_time
        self.timing["transform"] = elapsed
        self._log(f"  ✓ Rows transformed in {elapsed:.2f}s")

        return self.enriched_rows

    def write_to_delta(self, delete_existing: bool = True) -> int:
        """Write enriched rows to Delta table.

        Args:
            delete_existing: Whether to delete existing rows for this document_id

        Returns:
            Number of rows inserted
        """
        if not self.enriched_rows:
            raise ValueError("No rows to write. Call transform_rows() first.")

        start_time = time.time()
        self._log(f"  💾 Writing to {self.output_table}...")

        # Define schema - CHANGED monthly_share and ytd_share to STRING to avoid decimal rescaling errors
        schema = StructType(
            [
                StructField("document_id", StringType(), True),
                StructField("document_url", StringType(), True),
                StructField("filename", StringType(), True),
                StructField("report_year", IntegerType(), True),
                StructField("report_month", StringType(), True),
                StructField("report_start_date", StringType(), True),
                StructField("report_end_date", StringType(), True),
                StructField("maker", StringType(), True),
                StructField("model_name", StringType(), True),
                StructField("vama_classification", StringType(), True),
                StructField("seat", StringType(), True),
                StructField("monthly_north", IntegerType(), True),
                StructField("monthly_central", IntegerType(), True),
                StructField("monthly_south", IntegerType(), True),
                StructField("monthly_total", IntegerType(), True),
                StructField("monthly_share", StringType(), True),  # Changed from DecimalType(10, 4)
                StructField("ytd_north", IntegerType(), True),
                StructField("ytd_central", IntegerType(), True),
                StructField("ytd_south", IntegerType(), True),
                StructField("ytd_total", IntegerType(), True),
                StructField("ytd_share", StringType(), True),  # Changed from DecimalType(10, 4)
                StructField("source_table_index", IntegerType(), True),
                StructField("source_row_index", IntegerType(), True),
                StructField("extracted_timestamp", TimestampType(), True),
            ]
        )

        schema_cols = [f.name for f in schema.fields]

        # Create DataFrame
        df = self.spark.createDataFrame(
            [{c: r.get(c) for c in schema_cols} for r in self.enriched_rows], schema=schema
        )
        df.createOrReplaceTempView("vama_gemini_rows_to_write")

        # Create table if not exists (schema already matches with STRING columns)
        self.spark.sql(
            f"""
CREATE TABLE IF NOT EXISTS {self.output_table} (
  document_id STRING,
  document_url STRING,
  filename STRING,
  report_year INT,
  report_month STRING,
  report_start_date DATE,
  report_end_date DATE,
  maker STRING,
  model_name STRING,
  vama_classification STRING,
  seat STRING,
  monthly_north INT,
  monthly_central INT,
  monthly_south INT,
  monthly_total INT,
  monthly_share STRING,
  ytd_north INT,
  ytd_central INT,
  ytd_south INT,
  ytd_total INT,
  ytd_share STRING,
  source_table_index INT,
  source_row_index INT,
  extracted_timestamp TIMESTAMP
) USING DELTA
"""
        )

        # Delete existing data for this document if requested
        if delete_existing:
            escaped_doc_id = self.document_id.replace("'", "''")
            self.spark.sql(f"DELETE FROM {self.output_table} WHERE document_id = '{escaped_doc_id}'")

        # Insert new data - share columns stay as STRING (no CAST to DECIMAL)
        self.spark.sql(
            f"""
INSERT INTO {self.output_table}
SELECT
  document_id,
  document_url,
  filename,
  CAST(report_year AS INT),
  report_month,
  CAST(report_start_date AS DATE),
  CAST(report_end_date AS DATE),
  maker,
  model_name,
  vama_classification,
  seat,
  CAST(monthly_north AS INT),
  CAST(monthly_central AS INT),
  CAST(monthly_south AS INT),
  CAST(monthly_total AS INT),
  monthly_share,
  CAST(ytd_north AS INT),
  CAST(ytd_central AS INT),
  CAST(ytd_south AS INT),
  CAST(ytd_total AS INT),
  ytd_share,
  CAST(source_table_index AS INT),
  CAST(source_row_index AS INT),
  CAST(extracted_timestamp AS TIMESTAMP)
FROM vama_gemini_rows_to_write
"""
        )

        elapsed = time.time() - start_time
        self.timing["write"] = elapsed
        self._log(f"  ✓ Wrote {len(self.enriched_rows)} rows in {elapsed:.2f}s")

        return len(self.enriched_rows)

    def parse(self) -> Dict:
        """Execute full parsing pipeline: download -> call Gemini -> transform -> write.

        Returns:
            Dictionary with parsing results and timing info
        """
        total_start = time.time()

        self._log(f"\n{'='*80}")
        self._log(f"🔮 VAMA Gemini Parser")
        self._log(f"{'='*80}")
        self._log(f"Document ID: {self.document_id}")
        self._log(f"Filename: {self.filename}")
        self._log(f"Model: {self.gemini_model}")
        self._log(f"{'='*80}\n")

        try:
            # Step 1: Download PDF
            self._log("⏱️  STEP 1: Download PDF")
            self.download_pdf()

            # Step 2: Call Gemini API
            self._log("\n⏱️  STEP 2: Call Gemini API")
            self.call_gemini()

            # Step 3: Transform rows
            self._log("\n⏱️  STEP 3: Transform Rows")
            self.transform_rows()

            # Step 4: Write to Delta
            self._log("\n⏱️  STEP 4: Write to Delta Table")
            rows_written = self.write_to_delta()

            # Calculate total time
            total_elapsed = time.time() - total_start
            self.timing["total"] = total_elapsed

            # Print summary
            self._log(f"\n{'='*80}")
            self._log("📊 PARSING SUMMARY")
            self._log(f"{'='*80}")
            self._log(f"Download:     {self.timing.get('download', 0):.2f}s")
            self._log(f"Gemini API:   {self.timing.get('gemini_api', 0):.2f}s")
            self._log(f"Transform:    {self.timing.get('transform', 0):.2f}s")
            self._log(f"Write:        {self.timing.get('write', 0):.2f}s")
            self._log(f"Total:        {total_elapsed:.2f}s")
            self._log(f"Rows written: {rows_written}")
            self._log(f"{'='*80}\n")

            return {
                "status": "success",
                "document_id": self.document_id,
                "rows_written": rows_written,
                "timing": self.timing,
            }

        except Exception as e:
            error_msg = str(e)
            self._log(f"\n❌ ERROR: {error_msg}")
            return {
                "status": "failed",
                "document_id": self.document_id,
                "error": error_msg,
                "timing": self.timing,
            }
