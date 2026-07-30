# Databricks notebook source
# DBTITLE 1,VAMA Gemini Parser Package - Usage Guide
# MAGIC %md
# MAGIC # 📦 VAMA Gemini Parser Package - Usage Guide
# MAGIC
# MAGIC This notebook demonstrates how to use the professional `vama_parser` package for extracting VAMA sales data from PDFs using Google Gemini API.
# MAGIC
# MAGIC ## 🎯 Features
# MAGIC
# MAGIC * **Object-oriented design**: Clean `GeminiParser` class with well-defined methods
# MAGIC * **Type hints**: Full type annotations for better IDE support
# MAGIC * **Comprehensive logging**: Detailed timing and progress information
# MAGIC * **Error handling**: Robust error handling with informative messages
# MAGIC * **Configurable**: Flexible configuration options
# MAGIC * **Reusable**: Import and use anywhere in your workspace
# MAGIC
# MAGIC ## 📚 Package Structure
# MAGIC
# MAGIC ```
# MAGIC vama_parser/
# MAGIC ├── __init__.py          # Package initialization
# MAGIC ├── config.py            # Configuration and constants
# MAGIC ├── utils.py             # Helper functions
# MAGIC └── gemini_parser.py     # Main GeminiParser class
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Setup
# Setup: Add package to Python path
import sys

package_path = '/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA'
if package_path not in sys.path:
    sys.path.insert(0, package_path)

print("✓ Package path configured")

# COMMAND ----------

# DBTITLE 1,Import Package
# Import the package
from vama_parser import GeminiParser, TARGET_COLS, DEFAULT_GEMINI_MODEL

print(f"✓ Imported GeminiParser")
print(f"  Default model: {DEFAULT_GEMINI_MODEL}")
print(f"  Target columns: {len(TARGET_COLS)} fields")

# COMMAND ----------

# DBTITLE 1,Basic Usage
# MAGIC %md
# MAGIC ## 🚀 Basic Usage
# MAGIC
# MAGIC ### Simple Example
# MAGIC
# MAGIC The most straightforward way to use the parser:

# COMMAND ----------

# DBTITLE 1,Example 1: Simple Parse
# Example 1: Parse a single document

parser = GeminiParser(
    spark=spark,
    dbutils=dbutils,
    document_id="example_doc_001",
    document_url="http://vama.org.vn/path/to/report.pdf",
    filename="VAMA sales report August 2021.pdf",
    report_year=2021,
    report_month="2021-08",
    report_start_date="2021-08-01",
    report_end_date="2021-08-31",
    output_table="market_data.vama.sales_by_model_region_gemini",
    gemini_model="gemini-3.1-flash-lite",
    env_workspace_path="/Users/tuckeyhue@gmail.com/env/.env.",
    verbose=True  # Enable detailed logging
)

# Execute the full pipeline
result = parser.parse()

print(f"\nResult: {result['status']}")
if result['status'] == 'success':
    print(f"Rows written: {result['rows_written']}")
    print(f"Total time: {result['timing']['total']:.2f}s")

# COMMAND ----------

# DBTITLE 1,Advanced Usage
# MAGIC %md
# MAGIC ## 🔧 Advanced Usage
# MAGIC
# MAGIC ### Step-by-Step Execution
# MAGIC
# MAGIC For more control, you can execute each step individually:

# COMMAND ----------

# DBTITLE 1,Example 2: Step-by-Step
# Example 2: Step-by-step execution for more control

parser = GeminiParser(
    spark=spark,
    dbutils=dbutils,
    document_id="example_doc_002",
    document_url="http://vama.org.vn/path/to/report.pdf",
    filename="VAMA sales report.pdf",
    report_year=2021,
    report_month="2021-08",
    report_start_date="2021-08-01",
    report_end_date="2021-08-31",
    output_table="market_data.vama.sales_by_model_region_gemini",
    verbose=False  # Disable verbose logging for batch processing
)

# Step 1: Download PDF
print("📥 Step 1: Downloading PDF...")
pdf_bytes = parser.download_pdf()
print(f"  Downloaded {len(pdf_bytes):,} bytes")

# Step 2: Call Gemini API
print("\n🔮 Step 2: Calling Gemini API...")
rows = parser.call_gemini()
print(f"  Extracted {len(rows)} rows")

# Step 3: Transform rows
print("\n🔄 Step 3: Transforming rows...")
enriched = parser.transform_rows()
print(f"  Transformed {len(enriched)} rows")

# Step 4: Write to Delta
print("\n💾 Step 4: Writing to Delta...")
rows_written = parser.write_to_delta(delete_existing=True)
print(f"  Wrote {rows_written} rows")

print(f"\n✓ Complete! Timing: {parser.timing}")

# COMMAND ----------

# DBTITLE 1,Batch Processing
# MAGIC %md
# MAGIC ## 📦 Batch Processing
# MAGIC
# MAGIC ### Processing Multiple Documents
# MAGIC
# MAGIC Example of processing multiple documents in a loop:

# COMMAND ----------

# DBTITLE 1,Example 3: Batch Processing
# Example 3: Batch processing multiple documents

# Sample document list (you would query from your processing log)
documents = [
    {
        "document_id": "doc_001",
        "document_url": "http://vama.org.vn/path/to/report1.pdf",
        "filename": "Report Jan 2021.pdf",
        "report_year": 2021,
        "report_month": "2021-01",
        "report_start_date": "2021-01-01",
        "report_end_date": "2021-01-31"
    },
    {
        "document_id": "doc_002",
        "document_url": "http://vama.org.vn/path/to/report2.pdf",
        "filename": "Report Feb 2021.pdf",
        "report_year": 2021,
        "report_month": "2021-02",
        "report_start_date": "2021-02-01",
        "report_end_date": "2021-02-28"
    }
]

results = []

for idx, doc in enumerate(documents, 1):
    print(f"\n[{idx}/{len(documents)}] Processing: {doc['filename']}")
    
    parser = GeminiParser(
        spark=spark,
        dbutils=dbutils,
        document_id=doc['document_id'],
        document_url=doc['document_url'],
        filename=doc['filename'],
        report_year=doc['report_year'],
        report_month=doc['report_month'],
        report_start_date=doc['report_start_date'],
        report_end_date=doc['report_end_date'],
        output_table="market_data.vama.sales_by_model_region_gemini",
        verbose=False  # Reduce noise in batch mode
    )
    
    result = parser.parse()
    results.append(result)
    
    if result['status'] == 'success':
        print(f"  ✓ Success: {result['rows_written']} rows in {result['timing']['total']:.1f}s")
    else:
        print(f"  ✗ Failed: {result.get('error', 'Unknown error')}")

# Summary
print(f"\n{'='*60}")
print("BATCH SUMMARY")
print(f"{'='*60}")
success = sum(1 for r in results if r['status'] == 'success')
failed = len(results) - success
print(f"Total: {len(results)} documents")
print(f"Success: {success}")
print(f"Failed: {failed}")

if success > 0:
    avg_time = sum(r['timing']['total'] for r in results if r['status'] == 'success') / success
    print(f"Average time: {avg_time:.1f}s")

# COMMAND ----------

# DBTITLE 1,Configuration Options
# MAGIC %md
# MAGIC ## ⚙️ Configuration Options
# MAGIC
# MAGIC ### GeminiParser Parameters
# MAGIC
# MAGIC | Parameter | Type | Required | Default | Description |
# MAGIC |-----------|------|----------|---------|-------------|
# MAGIC | `spark` | SparkSession | Yes | - | Active Spark session |
# MAGIC | `dbutils` | DBUtils | Yes | - | Databricks utilities |
# MAGIC | `document_id` | str | Yes | - | Unique document identifier |
# MAGIC | `document_url` | str | Yes | - | URL to download PDF |
# MAGIC | `filename` | str | Yes | - | Original filename |
# MAGIC | `report_year` | int | Yes | - | Report year |
# MAGIC | `report_month` | str | Yes | - | Report month (YYYY-MM) |
# MAGIC | `report_start_date` | str | Yes | - | Start date (YYYY-MM-DD) |
# MAGIC | `report_end_date` | str | Yes | - | End date (YYYY-MM-DD) |
# MAGIC | `output_table` | str | Yes | - | Target Delta table |
# MAGIC | `gemini_model` | str | No | `gemini-3.1-flash-lite` | Gemini model name |
# MAGIC | `env_workspace_path` | str | No | None | Path to .env file |
# MAGIC | `gemini_api_key` | str | No | None | Direct API key (overrides env) |
# MAGIC | `verbose` | bool | No | True | Enable detailed logging |
# MAGIC
# MAGIC ### Available Models
# MAGIC
# MAGIC * `gemini-3.1-flash-lite` (default) - Fast, cost-effective
# MAGIC * `gemini-3.1-flash` - Balanced performance
# MAGIC * `gemini-3.1-pro` - Highest accuracy

# COMMAND ----------

# DBTITLE 1,Error Handling
# MAGIC %md
# MAGIC ## ⚠️ Error Handling
# MAGIC
# MAGIC ### Example with Try-Catch
# MAGIC
# MAGIC The package returns structured results with status and error information:

# COMMAND ----------

# DBTITLE 1,Example 4: Error Handling
# Example 4: Robust error handling

try:
    parser = GeminiParser(
        spark=spark,
        dbutils=dbutils,
        document_id="test_doc",
        document_url="http://invalid-url.com/missing.pdf",
        filename="test.pdf",
        report_year=2021,
        report_month="2021-08",
        report_start_date="2021-08-01",
        report_end_date="2021-08-31",
        output_table="market_data.vama.sales_by_model_region_gemini",
        verbose=True
    )
    
    result = parser.parse()
    
    if result['status'] == 'success':
        print(f"✓ Success: {result['rows_written']} rows written")
        print(f"  Timing breakdown: {result['timing']}")
    else:
        print(f"✗ Failed: {result['error']}")
        print(f"  Document ID: {result['document_id']}")
        print(f"  Time elapsed before failure: {result['timing'].get('total', 0):.1f}s")
        
except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## ✅ Best Practices
# MAGIC
# MAGIC ### 1. API Key Management
# MAGIC
# MAGIC * Store API keys in workspace .env files (never in code)
# MAGIC * Use the `env_workspace_path` parameter
# MAGIC * Alternative: Set `GEMINI_API_KEY` environment variable
# MAGIC
# MAGIC ### 2. Batch Processing
# MAGIC
# MAGIC * Set `verbose=False` for batch jobs to reduce log noise
# MAGIC * Implement retry logic for transient failures
# MAGIC * Monitor API quota usage
# MAGIC
# MAGIC ### 3. Output Tables
# MAGIC
# MAGIC * Use separate staging tables for validation (`_gemini` suffix)
# MAGIC * Merge to production tables after quality checks
# MAGIC * Keep `parsing_method` field to track data source
# MAGIC
# MAGIC ### 4. Performance
# MAGIC
# MAGIC * Use `gemini-3.1-flash-lite` for cost efficiency
# MAGIC * Process documents in parallel when possible
# MAGIC * Monitor timing metrics to identify slow documents
# MAGIC
# MAGIC ### 5. Error Recovery
# MAGIC
# MAGIC * Log all failures with document IDs
# MAGIC * Implement fallback strategies (HTML parsing)
# MAGIC * Review failed documents manually if needed