# Databricks notebook source
# DBTITLE 1,VAMA Step 3 - Extract Readable Tables
# MAGIC %md
# MAGIC # Step 3: Extract readable VAMA tables
# MAGIC
# MAGIC Produces two useful Delta tables:
# MAGIC
# MAGIC - `market_data.vama.extracted_tables_long`: generic readable long format for every parsed table cell/row.
# MAGIC - `market_data.vama.sales_by_model_region`: normalized model-level sales table from `Detail` PDFs.
# MAGIC
# MAGIC **Note:** This notebook extracts data from mainstream VAMA members. **Lexus, BMW, and Mercedes-Benz Vietnam (MBV)** are extracted separately in the `3b_Extract_Other_Makers` notebook and written to `market_data.vama.sales_by_other_makers`.
# MAGIC
# MAGIC The second table focuses on the recurring Detail PDF layout:
# MAGIC `Maker / Model / Classification / Seat / Sales month by region / Share / YTM by region / Share`.

# COMMAND ----------

# DBTITLE 1,Import Libraries and Define Catalog/Schema
import json
import re
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser

from pyspark.sql.types import *

CATALOG = "market_data"
SCHEMA = "vama"

# COMMAND ----------

# DBTITLE 1,Package Setup
# Setup VAMA parser package import path
import sys
package_path = '/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA'
if package_path not in sys.path:
    sys.path.insert(0, package_path)
    print(f"✓ Added package path: {package_path}")

# Verify package is importable
try:
    from vama_parser import GeminiParser
    print("✓ Successfully imported vama_parser package")
except ImportError as e:
    print(f"✗ Failed to import vama_parser: {e}")

# COMMAND ----------

# DBTITLE 1,Create Delta Tables (extracted_tables_long & sales_by_model_region

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.extracted_tables_long (
  document_id STRING NOT NULL,
  document_url STRING,
  title STRING,
  filename STRING,
  report_year INT,
  report_month INT,
  report_month_key STRING,
  report_type STRING,
  table_index INT,
  row_index INT,
  column_index INT,
  column_name STRING,
  cell_value STRING,
  extracted_timestamp TIMESTAMP
) USING DELTA
""")

# sales_by_model_region is created once and then MERGEd into, so a scheduled run
# never drops it. Dropping it here (the previous behaviour) forced a full
# re-extract of every parsed document on every run, re-spent Gemini quota on the
# LLM-fallback documents, and left the table empty for the whole run - and empty
# for good if the run failed part-way, taking curated_vama_sales_unified with it.
# Use the reextract_all widget below to rebuild history deliberately.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.sales_by_model_region (
  document_id STRING NOT NULL,
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
  monthly_share DECIMAL(10,4),
  ytd_north INT,
  ytd_central INT,
  ytd_south INT,
  ytd_total INT,
  ytd_share DECIMAL(10,4),
  source_table_index INT,
  source_row_index INT,
  extracted_timestamp TIMESTAMP,
  parsing_method STRING
) USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,Maker Segregation Note
# MAGIC %md
# MAGIC ### 📊 Extraction logic
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Define HTML Table Parser Class
class TableHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self.current_table = []
        self.current_row = []
        self.current_cell = ''
        self.current_colspan = 1
        self.in_table = False
        self.in_row = False
        self.in_cell = False

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
            self.current_table = []
        elif tag == 'tr' and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ('td', 'th') and self.in_row:
            self.in_cell = True
            self.current_cell = ''
            self.current_colspan = 1
            for k, v in attrs:
                if k == 'colspan':
                    try:
                        self.current_colspan = max(int(v), 1)
                    except Exception:
                        self.current_colspan = 1

    def handle_endtag(self, tag):
        if tag == 'table':
            if self.current_table:
                self.tables.append(self.current_table)
            self.in_table = False
        elif tag == 'tr' and self.in_row:
            if self.current_row:
                self.current_table.append(self.current_row)
            self.in_row = False
        elif tag in ('td', 'th') and self.in_cell:
            value = re.sub(r'\s+', ' ', self.current_cell).strip()
            self.current_row.append(value)
            for _ in range(self.current_colspan - 1):
                self.current_row.append('')
            self.in_cell = False
            self.current_colspan = 1

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

def parse_tables(parsed_json):
    if isinstance(parsed_json, str):
        data = json.loads(parsed_json)
    else:
        data = parsed_json
    elements = data.get('document', {}).get('elements', [])
    tables = []
    for elem in elements:
        if elem.get('type') == 'table' and elem.get('content'):
            parser = TableHTMLParser()
            parser.feed(elem.get('content'))
            tables.extend(parser.tables)
    return tables

def clean_cell(value):
    if value is None:
        return ''
    return re.sub(r'\s+', ' ', str(value)).strip()

def to_int(value):
    s = clean_cell(value)
    if s in ('', '-', '-'):
        return None
    s = s.replace(',', '').replace('.', '').replace(' ', '')
    if not re.fullmatch(r'-?\d+', s):
        return None
    return int(s)

def to_share(value):
    s = clean_cell(value)
    if not s or '%' not in s:
        return None
    m = re.search(r'-?\d+(?:[\.\,]\d+)?', s)
    if not m:
        return None
    try:
        return Decimal(m.group(0).replace(',', '.')) / Decimal('100')
    except InvalidOperation:
        return None

def is_noise_row(row):
    values = [clean_cell(x) for x in row]
    non_empty = [v for v in values if v]
    if not non_empty:
        return True
    if len(non_empty) <= 2 and all(v in ('-', '–') for v in non_empty):
        return True
    joined = ' '.join(non_empty).lower()
    if joined.startswith('maker model name') or joined.startswith('vama monthly'):
        return True
    return False

def is_valid_detail_table(table):
    """Check if table has the expected Detail PDF schema (Maker/Model/Classification)."""
    if not table or len(table) < 2:
        return False
    header = [clean_cell(c).lower() for c in table[0]]
    has_maker = any('maker' in h for h in header)
    has_model = any('model' in h for h in header)
    has_classification = any('classification' in h or 'vama' in h for h in header)
    has_no = any(h in ('no', 'no.') for h in header)
    has_vehicle_type = any('vehicle type' in h for h in header)
    if has_no or has_vehicle_type:
        return False
    return has_maker and has_model and has_classification

def has_non_numeric_makers(table):
    """Check if table has non-numeric maker values (not just category codes)."""
    if not table or len(table) < 3:
        return False
    # Check rows 2-10 for non-numeric makers
    for row_idx in range(2, min(10, len(table))):
        if row_idx >= len(table):
            break
        maker = clean_cell(table[row_idx][0])
        # Skip empty or numeric makers
        if maker and not re.fullmatch(r'\d+', maker) and maker.lower() not in ('maker', 'no'):
            return True
    return False

def find_all_detail_tables(tables, debug_doc_id=None):
    """
    Find ALL tables with valid detail data (non-numeric makers).
    Returns list of table indices.
    """
    detail_table_indices = []
    
    for idx, table in enumerate(tables):
        if not is_valid_detail_table(table):
            if debug_doc_id:
                print(f"  Table {idx}: NOT valid detail table (rows={len(table)})")
            continue
        
        has_real_makers = has_non_numeric_makers(table)
        
        if debug_doc_id:
            print(f"  Table {idx}: Valid detail table ({len(table)} rows), has real makers: {has_real_makers}")
        
        if has_real_makers:
            detail_table_indices.append(idx)
    
    return detail_table_indices

# NEW: Filter for excluding other makers
EXCLUDED_MAKERS = ['Lexus', 'BMW', 'MBV', 'Mercedes-Benz']

def is_excluded_maker(maker_value):
    """Check if maker should be excluded (Lexus, BMW, MBV)."""
    maker_lower = maker_value.lower()
    return any(excluded.lower() in maker_lower for excluded in EXCLUDED_MAKERS)

def detect_column_structure(header, report_year=None, report_month=None):
    """Detect column structure using SIMPLIFIED FIXED OFFSETS."""
    header_clean = [clean_cell(c).lower() for c in header]
    
    maker_idx = None
    model_idx = None
    classification_idx = None
    seat_idx = None
    
    for i, h in enumerate(header_clean):
        if 'maker' in h and maker_idx is None:
            maker_idx = i
        if 'model' in h and model_idx is None:
            model_idx = i
        if ('classification' in h or 'vama' in h) and classification_idx is None:
            classification_idx = i
        if 'seat' in h and seat_idx is None:
            seat_idx = i
    
    if maker_idx is None or model_idx is None or classification_idx is None:
        return None
    
    metadata_end = max(maker_idx, model_idx, classification_idx)
    if seat_idx is not None and seat_idx > metadata_end:
        metadata_end = seat_idx
    data_start = metadata_end + 1
    
    col_map = {
        'maker': maker_idx,
        'model': model_idx,
        'classification': classification_idx,
        'seat': seat_idx,
        'monthly_north': data_start,
        'monthly_central': data_start + 1,
        'monthly_south': data_start + 2,
        'monthly_total': data_start + 3,
        'monthly_share': None,
        'ytd_north': data_start + 4,
        'ytd_central': data_start + 5,
        'ytd_south': data_start + 6,
        'ytd_total': data_start + 7,
        'ytd_share': None,
    }
    
    return col_map

def extract_sales_rows_from_table(doc, table, table_idx, debug=False):
    """Extract sales rows from a single table - EXCLUDING other makers."""
    rows = []
    
    col_map = detect_column_structure(table[0], doc['report_year'], doc['report_month'])
    if col_map is None:
        if debug:
            print(f"  ⚠️ Could not detect column structure for table {table_idx}")
        return rows
    
    now = datetime.utcnow()
    year = doc['report_year']
    month = doc['report_month']
    
    if year and month:
        report_start = date(year, month, 1)
        next_month = report_start + relativedelta(months=1)
        report_end = next_month - relativedelta(days=1)
        report_month_str = f"{year:04d}-{month:02d}"
    else:
        report_start = None
        report_end = None
        report_month_str = None
    
    for row_index, row in enumerate(table):
        if row_index == 0:
            continue
            
        cells = [clean_cell(c) for c in row]
        max_col_needed = max([v for v in col_map.values() if v is not None])
        if len(cells) <= max_col_needed:
            cells += [''] * (max_col_needed - len(cells) + 1)
        
        if is_noise_row(cells):
            continue
        
        maker = cells[col_map['maker']]
        model = cells[col_map['model']]
        classification = cells[col_map['classification']]
        seat = cells[col_map['seat']] if col_map['seat'] is not None else ''
        
        # NEW: Skip excluded makers (Lexus, BMW, MBV)
        if is_excluded_maker(maker):
            continue
        
        if re.fullmatch(r'\d+', maker):
            continue
        if not maker or maker.lower() in ('maker', 'no', 'i', 'ii', 'iii'):
            continue
        if not model or model.lower() in ('model name', 'vehicle type', 'north', 'central', 'south', 'total'):
            continue
        
        model_lower = model.lower()
        if 'subtotal' in model_lower or model_lower == 'sub-total':
            continue
        if 'grand' in model_lower and 'total' in model_lower:
            continue
        
        monthly_total = to_int(cells[col_map['monthly_total']])
        ytd_total = to_int(cells[col_map['ytd_total']])
        if monthly_total is None and ytd_total is None:
            continue
        
        monthly_share = to_share(cells[col_map['monthly_share']]) if col_map['monthly_share'] is not None else None
        ytd_share = to_share(cells[col_map['ytd_share']]) if col_map['ytd_share'] is not None else None
        
        rows.append({
            'document_id': doc['document_id'],
            'document_url': doc['document_url'],
            'filename': doc['filename'],
            'report_year': doc['report_year'],
            'report_month': report_month_str,
            'report_start_date': report_start,
            'report_end_date': report_end,
            'maker': maker,
            'model_name': model,
            'vama_classification': classification,
            'seat': seat,
            'monthly_north': to_int(cells[col_map['monthly_north']]),
            'monthly_central': to_int(cells[col_map['monthly_central']]),
            'monthly_south': to_int(cells[col_map['monthly_south']]),
            'monthly_total': monthly_total,
            'monthly_share': monthly_share,
            'ytd_north': to_int(cells[col_map['ytd_north']]),
            'ytd_central': to_int(cells[col_map['ytd_central']]),
            'ytd_south': to_int(cells[col_map['ytd_south']]),
            'ytd_total': ytd_total,
            'ytd_share': ytd_share,
            'source_table_index': table_idx,
            'source_row_index': row_index,
            'extracted_timestamp': now,
            'parsing_method': 'html'
        })
    return rows

def extract_sales_rows(doc, tables):
    """
    Extract sales data from ALL detail tables with non-numeric makers.
    EXCLUDES: Lexus, BMW, MBV (handled in separate notebook/table).
    """
    all_rows = []
    
    if doc['report_type'] != 'detail':
        return all_rows
    
    
    year = doc['report_year']
    month = doc['report_month']
    report_month_str = f"{year:04d}-{month:02d}" if year and month else None
    
    # DEBUG: Print table info for a few key months
    debug_months = ['2024-11', '2024-10', '2021-12', '2020-12']
    debug_this_doc = report_month_str in debug_months
    
    if debug_this_doc:
        print(f"\nDEBUG {report_month_str} ({doc['document_id']}): {len(tables)} tables")
    
    # Find ALL detail tables with non-numeric makers
    detail_table_indices = find_all_detail_tables(tables, doc['document_id'] if debug_this_doc else None)
    
    if not detail_table_indices:
        if not debug_this_doc:
            print(f"⚠️ No suitable tables found in {doc['document_id']}")
        return all_rows
    
    if debug_this_doc:
        print(f"  ✓ Extracting from {len(detail_table_indices)} detail table(s): {detail_table_indices}")
    
    # Extract from ALL detail tables
    for table_idx in detail_table_indices:
        table = tables[table_idx]
        rows = extract_sales_rows_from_table(doc, table, table_idx, debug=debug_this_doc)
        all_rows.extend(rows)
        if debug_this_doc:
            print(f"    Table {table_idx}: {len(rows)} rows extracted")
    
    if debug_this_doc:
        print(f"  → Total extracted: {len(all_rows)} rows")
    
    return all_rows

# COMMAND ----------

# DBTITLE 1,Select Documents Needing Extraction
# Incremental by default: a document is extracted when it has never been
# extracted, when the last attempt failed, or when it has been re-parsed since it
# was last extracted (which is what ADHOC_Reparse_Detail_PDFs produces).
# Documents already carrying a good extraction are left alone, so their rows -
# including Gemini-corrected parsing_method='llm' rows - survive untouched.
#
# Escape hatches for when the extraction logic itself changes:
#   reextract_all = true       rebuild every parsed document
#   only_months = 2026-05,...  restrict to these report_month_key values
dbutils.widgets.text("reextract_all", "false")
dbutils.widgets.text("only_months", "")

REEXTRACT_ALL = dbutils.widgets.get("reextract_all").strip().lower() == "true"
ONLY_MONTHS = [m.strip() for m in dbutils.widgets.get("only_months").split(",") if m.strip()]

filters = ["log.parse_status = 'success'"]
if not REEXTRACT_ALL:
    filters.append("""(
        log.extraction_status IS NULL
        OR log.extraction_status = 'failed'
        OR log.extraction_timestamp IS NULL
        OR raw.parsed_timestamp > log.extraction_timestamp
    )""")
if ONLY_MONTHS:
    months_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in ONLY_MONTHS)
    filters.append(f"raw.report_month_key IN ({months_sql})")

# Deduplicate parsed documents by selecting latest parse for each document_id
raw_docs = spark.sql(f"""
WITH ranked_docs AS (
  SELECT raw.*,
    ROW_NUMBER() OVER (PARTITION BY raw.document_id ORDER BY raw.parsed_timestamp DESC) as rn
  FROM {CATALOG}.{SCHEMA}.parsed_documents_raw raw
  JOIN {CATALOG}.{SCHEMA}.document_processing_log log
    ON raw.document_id = log.document_id
  WHERE {" AND ".join(filters)}
)
SELECT document_id, document_url, title, filename, report_year, report_month, report_month_key, report_type, parsed_json, parsed_timestamp
FROM ranked_docs
WHERE rn = 1
""").collect()

print(f"reextract_all={REEXTRACT_ALL}  only_months={ONLY_MONTHS or '(all)'}")
print(f"Documents to extract: {len(raw_docs)}")
long_rows = []
sales_rows = []
status_rows = []

for doc_row in raw_docs:
    doc = doc_row.asDict()
    try:
        tables = parse_tables(doc['parsed_json'])
        now = datetime.utcnow()
        for table_index, table in enumerate(tables):
            header = [clean_cell(c) for c in table[0]] if table else []
            for row_index, row in enumerate(table):
                cells = [clean_cell(c) for c in row]
                for col_index, value in enumerate(cells):
                    col_name = header[col_index] if col_index < len(header) and header[col_index] else f"col_{col_index+1}"
                    long_rows.append({
                        'document_id': doc['document_id'],
                        'document_url': doc['document_url'],
                        'title': doc['title'],
                        'filename': doc['filename'],
                        'report_year': doc['report_year'],
                        'report_month': doc['report_month'],
                        'report_month_key': doc['report_month_key'],
                        'report_type': doc['report_type'],
                        'table_index': table_index,
                        'row_index': row_index,
                        'column_index': col_index,
                        'column_name': col_name,
                        'cell_value': value,
                        'extracted_timestamp': now,
                        'parsing_method': 'html'
                    })
        doc_sales_rows = extract_sales_rows(doc, tables)
        sales_rows.extend(doc_sales_rows)
        # A detail document that parses but yields no sales rows is a failure, not a
        # success. Reporting 'success' here is what let 2026-06 lose ~24,000 units
        # silently: the status kept the document out of the incremental re-extract
        # filter and out of the LLM fallback, so nothing ever retried it and nothing
        # flagged it. Only 'detail' documents carry model rows - 'other' report types
        # are handled by 3b Extract Other Makers, so zero rows there is expected.
        if doc['report_type'] == 'detail' and not doc_sales_rows:
            status_rows.append((
                doc['document_id'],
                'failed',
                f"Detail document parsed {len(tables)} table(s) but produced 0 sales rows",
                0,
                now,
            ))
            print(f"FAILED {doc['document_id']}: detail document produced 0 sales rows from {len(tables)} table(s)")
        else:
            status_rows.append((doc['document_id'], 'success', None, len(doc_sales_rows), now))
    except Exception as e:
        status_rows.append((doc['document_id'], 'failed', str(e), 0, datetime.utcnow()))
        print(f"FAILED {doc['document_id']}: {e}")

# COMMAND ----------

# DBTITLE 1,Extract & Insert Long Format Data
if long_rows:
    long_schema = StructType([
        StructField('document_id', StringType(), False),
        StructField('document_url', StringType(), True),
        StructField('title', StringType(), True),
        StructField('filename', StringType(), True),
        StructField('report_year', IntegerType(), True),
        StructField('report_month', IntegerType(), True),
        StructField('report_month_key', StringType(), True),
        StructField('report_type', StringType(), True),
        StructField('table_index', IntegerType(), True),
        StructField('row_index', IntegerType(), True),
        StructField('column_index', IntegerType(), True),
        StructField('column_name', StringType(), True),
        StructField('cell_value', StringType(), True),
        StructField('extracted_timestamp', TimestampType(), True),
    ])
    spark.createDataFrame(long_rows, long_schema).createOrReplaceTempView('vama_extracted_tables_long_new')
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.extracted_tables_long AS target
    USING vama_extracted_tables_long_new AS source
    ON target.document_id = source.document_id
       AND target.table_index = source.table_index
       AND target.row_index = source.row_index
       AND target.column_index = source.column_index
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if sales_rows:
    sales_schema = StructType([
        StructField('document_id', StringType(), False),
        StructField('document_url', StringType(), True),
        StructField('filename', StringType(), True),
        StructField('report_year', IntegerType(), True),
        StructField('report_month', StringType(), True),
        StructField('report_start_date', DateType(), True),
        StructField('report_end_date', DateType(), True),
        StructField('maker', StringType(), True),
        StructField('model_name', StringType(), True),
        StructField('vama_classification', StringType(), True),
        StructField('seat', StringType(), True),
        StructField('monthly_north', IntegerType(), True),
        StructField('monthly_central', IntegerType(), True),
        StructField('monthly_south', IntegerType(), True),
        StructField('monthly_total', IntegerType(), True),
        StructField('monthly_share', DecimalType(10,4), True),
        StructField('ytd_north', IntegerType(), True),
        StructField('ytd_central', IntegerType(), True),
        StructField('ytd_south', IntegerType(), True),
        StructField('ytd_total', IntegerType(), True),
        StructField('ytd_share', DecimalType(10,4), True),
        StructField('source_table_index', IntegerType(), True),
        StructField('source_row_index', IntegerType(), True),
        StructField('extracted_timestamp', TimestampType(), True),
        StructField('parsing_method',  StringType(), True)

    ])
    spark.createDataFrame(sales_rows, sales_schema).createOrReplaceTempView('vama_sales_by_model_region_new')
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.sales_by_model_region AS target
    USING vama_sales_by_model_region_new AS source
    ON target.document_id = source.document_id
       AND target.maker = source.maker
       AND target.model_name = source.model_name
       AND target.source_table_index = source.source_table_index
       AND target.source_row_index = source.source_row_index
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# DBTITLE 1,Update Extraction Status in Processing Log
if status_rows:
    # Deduplicate status_rows by document_id (keep last occurrence)
    status_dict = {}
    for doc_id, status, error_msg, rows_inserted, timestamp in status_rows:
        status_dict[doc_id] = (doc_id, status, error_msg, rows_inserted, timestamp)
    deduplicated_status_rows = list(status_dict.values())
    
    status_schema = StructType([
        StructField('document_id', StringType(), False),
        StructField('extraction_status', StringType(), True),
        StructField('extraction_error_message', StringType(), True),
        StructField('extraction_rows_inserted', IntegerType(), True),
        StructField('extraction_timestamp', TimestampType(), True),
    ])
    spark.createDataFrame(deduplicated_status_rows, status_schema).createOrReplaceTempView('vama_extraction_status')
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.document_processing_log AS target
    USING vama_extraction_status AS source
    ON target.document_id = source.document_id
    WHEN MATCHED THEN UPDATE SET
      extraction_status = source.extraction_status,
      extraction_error_message = source.extraction_error_message,
      extraction_rows_inserted = source.extraction_rows_inserted,
      extraction_timestamp = source.extraction_timestamp,
      updated_at = current_timestamp()
    """)

print(f"Generic long rows prepared: {len(long_rows)}")
print(f"Model-region sales rows prepared: {len(sales_rows)}")

display(spark.sql(f"""
SELECT report_month, COUNT(*) AS rows, SUM(monthly_total) AS monthly_total, SUM(ytd_total) AS ytd_total
FROM {CATALOG}.{SCHEMA}.sales_by_model_region
GROUP BY report_month
ORDER BY report_month DESC
"""))

# COMMAND ----------

# DBTITLE 1,Quick Check: Total Sales by Month
# MAGIC %sql
# MAGIC select report_month, sum(monthly_total)
# MAGIC from market_data.vama.sales_by_model_region
# MAGIC group by all
# MAGIC

# COMMAND ----------

# DBTITLE 1,Sample Data: February 2026 Records
# MAGIC %sql
# MAGIC select *
# MAGIC from market_data.vama.sales_by_model_region
# MAGIC where report_month = '2026-02'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Extraction Validation
# MAGIC %md
# MAGIC ## Extraction Validation
# MAGIC
# MAGIC Validate extracted sales totals against grand-total rows in source PDFs to ensure column mapping is correct.

# COMMAND ----------

# DBTITLE 1,Validation Query
validation_docs_query = f"""
WITH extracted_monthly AS (
  SELECT 
    s.document_id,
    s.report_month,
    SUM(s.monthly_north) as extracted_north,
    SUM(s.monthly_central) as extracted_central,
    SUM(s.monthly_south) as extracted_south,
    SUM(s.monthly_total) as extracted_total
  FROM {CATALOG}.{SCHEMA}.sales_by_model_region s
  WHERE s.report_month >= '2018-01'
  GROUP BY s.document_id, s.report_month
),
grand_total_source AS (
  WITH grand_total_positions AS (
    SELECT DISTINCT
      report_month_key,
      document_id,
      table_index,
      row_index
    FROM {CATALOG}.{SCHEMA}.extracted_tables_long
    WHERE LOWER(cell_value) LIKE '%grand%total%'
      AND report_month_key >= '2018-01'
      AND filename LIKE '%Detail%'
  ),
  grand_total_values AS (
    SELECT 
      etl.report_month_key,
      etl.document_id,
      etl.column_index,
      etl.cell_value,
      CAST(REGEXP_REPLACE(etl.cell_value, '[^0-9]', '') AS BIGINT) as numeric_value
    FROM {CATALOG}.{SCHEMA}.extracted_tables_long etl
    INNER JOIN grand_total_positions gtp
      ON etl.report_month_key = gtp.report_month_key
      AND etl.document_id = gtp.document_id
      AND etl.table_index = gtp.table_index
      AND etl.row_index = gtp.row_index
    WHERE etl.column_index BETWEEN 4 AND 7
      AND etl.cell_value RLIKE '[0-9]'
  )
  SELECT 
    document_id,
    report_month_key,
    MAX(CASE WHEN column_index = 7 THEN numeric_value END) as source_total
  FROM grand_total_values
  GROUP BY document_id, report_month_key
)
SELECT DISTINCT
  e.document_id,
  e.report_month,
  e.extracted_total,
  s.source_total,
  (ABS(e.extracted_total - s.source_total) / s.source_total)*100 as total_error_pct
FROM extracted_monthly e
INNER JOIN grand_total_source s 
  ON e.document_id = s.document_id
  AND e.report_month = s.report_month_key
ORDER BY e.report_month DESC
"""

validation_docs = spark.sql(validation_docs_query)
validation_docs.display()

# COMMAND ----------

# DBTITLE 1,Auto-Fallback: Re-parse Failed Validations with Gemini LLM
# Automatic Gemini Fallback for Failed Validations
# Runs validation → if FAIL → delete incorrect data → re-parse with Gemini → insert with parsing_method='llm'

import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import traceback
import importlib

# Add package directory to Python path
sys.path.insert(0, '/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA')

# Force reload to pick up the updated STRING schema for share columns
if 'vama_parser' in sys.modules:
    print("♻️  Reloading vama_parser module to pick up schema changes...")
    import vama_parser
    importlib.reload(vama_parser)
    if 'vama_parser.gemini_parser' in sys.modules:
        importlib.reload(sys.modules['vama_parser.gemini_parser'])
    from vama_parser import GeminiParser
else:
    from vama_parser import GeminiParser

print("🔍 Running validation to identify failed documents...\n")

# Get failed documents from validation query
failed_docs_query = f"""
WITH extracted_monthly AS (
  SELECT 
    s.document_id,
    s.report_month,
    SUM(s.monthly_north) as extracted_north,
    SUM(s.monthly_central) as extracted_central,
    SUM(s.monthly_south) as extracted_south,
    SUM(s.monthly_total) as extracted_total
  FROM {CATALOG}.{SCHEMA}.sales_by_model_region s
  WHERE s.report_month >= '2018-01'
    AND s.parsing_method = 'html'  -- Only check HTML-parsed documents
  GROUP BY s.document_id, s.report_month
),
grand_total_source AS (
  WITH grand_total_positions AS (
    SELECT DISTINCT
      report_month_key,
      document_id,
      table_index,
      row_index
    FROM {CATALOG}.{SCHEMA}.extracted_tables_long
    WHERE LOWER(cell_value) LIKE '%grand%total%'
      AND report_month_key >= '2018-01'
      AND filename LIKE '%Detail%'
  ),
  grand_total_values AS (
    SELECT 
      etl.report_month_key,
      etl.document_id,
      etl.column_index,
      etl.cell_value,
      CAST(REGEXP_REPLACE(etl.cell_value, '[^0-9]', '') AS BIGINT) as numeric_value
    FROM {CATALOG}.{SCHEMA}.extracted_tables_long etl
    INNER JOIN grand_total_positions gtp
      ON etl.report_month_key = gtp.report_month_key
      AND etl.document_id = gtp.document_id
      AND etl.table_index = gtp.table_index
      AND etl.row_index = gtp.row_index
    WHERE etl.column_index BETWEEN 4 AND 7
      AND etl.cell_value RLIKE '[0-9]'
  )
  SELECT
    document_id,
    report_month_key,
    MAX(CASE WHEN column_index = 7 THEN numeric_value END) as source_total
  FROM grand_total_values
  GROUP BY document_id, report_month_key
  -- Only trust the grand-total row when its own regional cells reconcile to the
  -- total sitting at column 7. Reading column 7 as "the total" is a positional
  -- assumption, and it is wrong whenever the parser misaligns that row: in
  -- 2026-06 the row was shifted three columns, so column 7 held a year-to-date
  -- figure (30,611) and the document was scored a 23% error against a number that
  -- was never its monthly total. That false failure is what sent an otherwise
  -- good month into the destructive Gemini fallback below. A row that does not
  -- add up is unusable evidence - leave the document unjudged rather than
  -- declaring it broken. 2026-05 reconciles (10,145 + 4,871 + 9,120 = 24,136)
  -- and is unaffected.
  HAVING MAX(CASE WHEN column_index = 4 THEN numeric_value END)
       + MAX(CASE WHEN column_index = 5 THEN numeric_value END)
       + MAX(CASE WHEN column_index = 6 THEN numeric_value END)
       = MAX(CASE WHEN column_index = 7 THEN numeric_value END)
)
SELECT DISTINCT
  e.document_id,
  e.report_month,
  e.extracted_total,
  s.source_total,
  ABS(e.extracted_total - s.source_total) / s.source_total as total_error_pct
FROM extracted_monthly e
INNER JOIN grand_total_source s 
  ON e.document_id = s.document_id
  AND e.report_month = s.report_month_key
WHERE ABS(e.extracted_total - s.source_total) / s.source_total >= 0.05
ORDER BY e.report_month DESC
"""

failed_docs = spark.sql(failed_docs_query).collect()

if not failed_docs:
    print("✓ No failed documents found. All validations passed!")
else:
    print(f"✗ Found {len(failed_docs)} failed document(s) requiring Gemini re-parse:\n")
    for doc in failed_docs:
        print(f"  - {doc.document_id} ({doc.report_month}): {doc.total_error_pct*100:.1f}% error")
    
    print(f"\n🤖 Starting Gemini fallback processing...\n")
    print("⚠️  Using 10-minute timeout per document (some PDFs are complex/large)\n")
    
    # Get document metadata for failed documents
    doc_ids_str = "','".join([d.document_id for d in failed_docs])
    doc_metadata_query = f"""
    SELECT 
      log.document_id,
      log.document_url,
      log.filename,
      log.report_year,
      log.report_month_key
    FROM {CATALOG}.{SCHEMA}.document_processing_log log
    WHERE log.document_id IN ('{doc_ids_str}')
    """
    
    doc_metadata = {row.document_id: row.asDict() for row in spark.sql(doc_metadata_query).collect()}
    
    fallback_results = []
    
    for idx, failed_doc in enumerate(failed_docs, 1):
        doc_id = failed_doc.document_id
        metadata = doc_metadata.get(doc_id)
        
        if not metadata:
            print(f"  ⚠️ Skipping {doc_id}: metadata not found")
            continue
        
        # Derive report_start_date and report_end_date from report_month_key (format: YYYY-MM)
        try:
            report_month_key = metadata['report_month_key']
            report_date = datetime.strptime(report_month_key, '%Y-%m')
            report_start_date = report_date.strftime('%Y-%m-01')
            # Last day of the month
            next_month = report_date + relativedelta(months=1)
            last_day = (next_month - relativedelta(days=1)).strftime('%Y-%m-%d')
            report_end_date = last_day
        except Exception as e:
            print(f"  ⚠️ Skipping {doc_id}: failed to parse report_month_key '{report_month_key}': {e}")
            continue
        
        print(f"\n[{idx}/{len(failed_docs)}] 📄 Processing: {doc_id} ({metadata['filename']})")
        print(f"    URL: {metadata['document_url'][:80]}...")
        
        # Step 1: Call Gemini parser using the professional package.
        #
        # The existing HTML rows are deliberately NOT deleted here. Deleting first and
        # re-inserting only on success is what destroyed 2026-06: the HTML extract had
        # written 87 good rows, validation flagged the document, this cell deleted them,
        # the Gemini re-parse then produced nothing, and the month was left empty while
        # document_processing_log still said 'success'. The delete now happens further
        # down, only once replacement rows actually exist in the _gemini table.
        print(f"  🔮 Calling Gemini parser (timeout: 10 minutes)...")
        start_time = datetime.now()
        
        try:
            # Initialize parser
            parser = GeminiParser(
                spark=spark,
                dbutils=dbutils,
                document_id=doc_id,
                document_url=metadata['document_url'],
                filename=metadata['filename'],
                report_year=metadata['report_year'],
                report_month=metadata['report_month_key'],
                report_start_date=report_start_date,
                report_end_date=report_end_date,
                output_table=f"{CATALOG}.{SCHEMA}.sales_by_model_region_gemini",
                gemini_model="gemini-3.5-flash",
                env_workspace_path="/Users/tuckeyhue@gmail.com/env/.env.",
                verbose=False  # Reduce verbosity for batch processing
            )
            
            # Execute parsing pipeline
            result = parser.parse()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            if result['status'] == 'success':
                print(f"    Gemini parsing completed in {elapsed_time:.1f} seconds")
                print(f"    Extracted {result['rows_written']} rows")

                # Step 2: Materialise the replacement before removing anything. A
                # 'success' status from the parser is not sufficient evidence that rows
                # landed - check the table itself.
                gemini_rows = spark.sql(f"""
                SELECT COUNT(*) AS cnt
                FROM {CATALOG}.{SCHEMA}.sales_by_model_region_gemini
                WHERE document_id = '{doc_id}'
                """).collect()[0].cnt

                if gemini_rows == 0:
                    raise Exception(
                        "Gemini reported success but wrote 0 rows to "
                        f"sales_by_model_region_gemini for {doc_id}; keeping existing rows"
                    )

                print(f"    Gemini produced {gemini_rows} replacement rows")

                # Step 3: Now that a replacement exists, swap it in.
                print(f"  ⌫ Removing superseded rows for {doc_id}...")
                spark.sql(f"""
                DELETE FROM {CATALOG}.{SCHEMA}.sales_by_model_region
                WHERE document_id = '{doc_id}'
                """)

                print(f"  📥 Copying Gemini results to main table...")
                spark.sql(f"""
                INSERT INTO {CATALOG}.{SCHEMA}.sales_by_model_region
                SELECT 
                  document_id,
                  document_url,
                  filename,
                  report_year,
                  report_month,
                  report_start_date,
                  report_end_date,
                  maker,
                  model_name,
                  vama_classification,
                  seat,
                  monthly_north,
                  monthly_central,
                  monthly_south,
                  monthly_total,
                  NULL as monthly_share,
                  ytd_north,
                  ytd_central,
                  ytd_south,
                  ytd_total,
                  NULL as ytd_share,
                  source_table_index,
                  source_row_index,
                  extracted_timestamp,
                  'llm' as parsing_method
                FROM {CATALOG}.{SCHEMA}.sales_by_model_region_gemini
                WHERE document_id = '{doc_id}'
                """)
                
                rows_inserted = spark.sql(f"""
                SELECT COUNT(*) as cnt
                FROM {CATALOG}.{SCHEMA}.sales_by_model_region
                WHERE document_id = '{doc_id}' AND parsing_method = 'llm'
                """).collect()[0].cnt
                
                print(f"    Inserted {rows_inserted} rows with parsing_method='llm'")
                
                # Step 4: Update document_processing_log
                print(f"  📝 Updating processing log...")
                spark.sql(f"""
                UPDATE {CATALOG}.{SCHEMA}.document_processing_log
                SET 
                  extraction_status = 'success_llm_fallback',
                  extraction_rows_inserted = {rows_inserted},
                  extraction_timestamp = current_timestamp(),
                  updated_at = current_timestamp()
                WHERE document_id = '{doc_id}'
                """)
                
                fallback_results.append({
                    'document_id': doc_id,
                    'status': 'success',
                    'rows_inserted': rows_inserted,
                    'elapsed_seconds': elapsed_time
                })
                
                print(f"  ✓ Successfully re-parsed with Gemini\n")
            else:
                raise Exception(result.get('error', 'Unknown error'))
            
        except Exception as e:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc(),
                'elapsed_seconds': elapsed_time
            }
            print(f"  ✗ Gemini parsing failed after {elapsed_time:.1f} seconds:")
            print(f"    Error type: {error_details['error_type']}")
            print(f"    Message: {error_details['error_message'][:200]}")

            # Record the failure. Without this the document keeps the 'success' the
            # HTML extract gave it earlier in the run, so a month that lost its rows
            # here looks healthy in document_processing_log and is never retried -
            # which is precisely how 2026-06 stayed invisible.
            safe_error = error_details['error_message'].replace("'", "''")[:500]
            spark.sql(f"""
            UPDATE {CATALOG}.{SCHEMA}.document_processing_log
            SET
              extraction_status = 'failed_llm_fallback',
              extraction_error_message = '{safe_error}',
              extraction_timestamp = current_timestamp(),
              updated_at = current_timestamp()
            WHERE document_id = '{doc_id}'
            """)

            # Only print full traceback for non-timeout errors
            if 'timeout' not in str(e).lower():
                print(f"\nFull traceback:\n{error_details['traceback']}\n")
            else:
                print(f"    ⚠️ Document exceeded 10-minute timeout - may need manual review\n")
            
            fallback_results.append({
                'document_id': doc_id,
                'status': 'failed',
                'error': error_details
            })
    
    # Summary
    print("\n" + "="*80)
    print("🎯 GEMINI FALLBACK SUMMARY")
    print("="*80)
    success_count = sum(1 for r in fallback_results if r['status'] == 'success')
    failed_count = len(fallback_results) - success_count
    
    # Calculate timing stats for successful runs
    if success_count > 0:
        success_times = [r['elapsed_seconds'] for r in fallback_results if r['status'] == 'success']
        avg_time = sum(success_times) / len(success_times)
        max_time = max(success_times)
        min_time = min(success_times)
        
        print(f"\n⌛ Timing Statistics:")
        print(f"  Average: {avg_time:.1f}s")
        print(f"  Range: {min_time:.1f}s - {max_time:.1f}s")
    
    print(f"\n📊 Results:")
    print(f"  Total documents processed: {len(fallback_results)}")
    print(f"  Successfully re-parsed: {success_count}")
    print(f"  Failed: {failed_count}")
    
    if failed_count > 0:
        timeout_count = sum(1 for r in fallback_results if r['status'] == 'failed' and 'timeout' in str(r.get('error', {}).get('error_message', '')).lower())
        if timeout_count > 0:
            print(f"    - Timeouts: {timeout_count} (exceeded 10-minute limit)")
            print(f"    - Other failures: {failed_count - timeout_count}")
    
    if success_count > 0:
        print(f"\n✓ Re-run the Validation Query (Cell 14) to verify improvements!")
    
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} document(s) still need attention. Consider:")
        print(f"  - Manually inspecting PDFs that timed out")
        print(f"  - Increasing timeout further for complex documents")
        print(f"  - Checking Gemini API rate limits/quotas")

# COMMAND ----------

# DBTITLE 1,Data Completeness Check - Missing Months
# Data Completeness Check: Identify Missing Months
# A complete year should have 12 months of data

print("📅 Checking for missing months in the dataset...\n")

# Query to find missing months
missing_months_query = f"""
WITH all_years AS (
  SELECT DISTINCT 
    CAST(SUBSTRING(report_month, 1, 4) AS INT) as report_year
  FROM {CATALOG}.{SCHEMA}.sales_by_model_region
),
expected_months AS (
  SELECT 
    y.report_year,
    LPAD(CAST(m.month_num AS STRING), 2, '0') as month_str,
    CONCAT(CAST(y.report_year AS STRING), '-', LPAD(CAST(m.month_num AS STRING), 2, '0')) as expected_month
  FROM all_years y
  CROSS JOIN (SELECT EXPLODE(SEQUENCE(1, 12)) as month_num) m
),
actual_months AS (
  SELECT DISTINCT
    CAST(SUBSTRING(report_month, 1, 4) AS INT) as report_year,
    report_month,
    COUNT(DISTINCT document_id) as doc_count
  FROM {CATALOG}.{SCHEMA}.sales_by_model_region
  GROUP BY report_year, report_month
)
SELECT 
  e.report_year,
  COUNT(DISTINCT e.expected_month) as expected_count,
  COUNT(DISTINCT a.report_month) as actual_count,
  (12 - COUNT(DISTINCT a.report_month)) as missing_count,
  COLLECT_LIST(
    CASE 
      WHEN a.report_month IS NULL THEN e.expected_month 
      ELSE NULL 
    END
  ) as missing_months
FROM expected_months e
LEFT JOIN actual_months a 
  ON e.expected_month = a.report_month
GROUP BY e.report_year
HAVING COUNT(DISTINCT a.report_month) < 12
ORDER BY e.report_year DESC
"""

missing_results = spark.sql(missing_months_query)
missing_count = missing_results.count()

if missing_count == 0:
    print("✅ COMPLETE DATASET: All years have 12 months of data!\n")
else:
    print(f"⚠️  INCOMPLETE DATASET: Found {missing_count} year(s) with missing months\n")
    print("="*80)
    
    for row in missing_results.collect():
        missing_list = [m for m in row.missing_months if m is not None]
        print(f"\n📊 Year {row.report_year}:")
        print(f"   Expected: 12 months")
        print(f"   Actual:   {row.actual_count} months")
        print(f"   Missing:  {row.missing_count} month(s)")
        if missing_list:
            print(f"   ❌ Missing months: {', '.join(sorted(missing_list))}")
    
    print("\n" + "="*80)

# Display detailed month-by-month coverage
print("\n📋 Detailed Month-by-Month Coverage:\n")

month_coverage_query = f"""
WITH all_years AS (
  SELECT DISTINCT 
    CAST(SUBSTRING(report_month, 1, 4) AS INT) as report_year
  FROM {CATALOG}.{SCHEMA}.sales_by_model_region
),
expected_months AS (
  SELECT 
    y.report_year,
    CONCAT(CAST(y.report_year AS STRING), '-', LPAD(CAST(m.month_num AS STRING), 2, '0')) as expected_month
  FROM all_years y
  CROSS JOIN (SELECT EXPLODE(SEQUENCE(1, 12)) as month_num) m
),
actual_months AS (
  SELECT DISTINCT
    report_month,
    COUNT(DISTINCT document_id) as doc_count,
    SUM(monthly_total) as total_sales
  FROM {CATALOG}.{SCHEMA}.sales_by_model_region
  GROUP BY report_month
)
SELECT 
  e.report_year,
  e.expected_month,
  CASE 
    WHEN a.report_month IS NOT NULL THEN '✓' 
    ELSE '✗' 
  END as status,
  COALESCE(a.doc_count, 0) as document_count,
  COALESCE(a.total_sales, 0) as monthly_sales
FROM expected_months e
LEFT JOIN actual_months a 
  ON e.expected_month = a.report_month
ORDER BY e.report_year DESC, e.expected_month DESC
"""

display(spark.sql(month_coverage_query))

# COMMAND ----------

# DBTITLE 1,Re-extract Failed Documents with LLM
# Auto-Extract Unprocessed Documents (LLM Fallback)
# 
# Purpose: Automatically identifies and processes documents that have not been extracted yet
# by querying the document_processing_log table to find:
#   1. Documents with parse_status='success' but extraction_status IS NULL or 'failed'
#   2. Documents where HTML parsing succeeded but no rows exist in sales_by_model_region
#
# This replaces the manual hardcoded list approach with dynamic query-based discovery

from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
import importlib
import traceback

# Import parser package with FORCE RELOAD to pick up schema changes
sys.path.insert(0, '/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA')

# Force reload to pick up the updated STRING schema for share columns
if 'vama_parser' in sys.modules:
    print("♻️  Reloading vama_parser module to pick up schema changes...")
    import vama_parser
    importlib.reload(vama_parser)
    # Reload submodules
    if 'vama_parser.gemini_parser' in sys.modules:
        importlib.reload(sys.modules['vama_parser.gemini_parser'])
    from vama_parser import GeminiParser
else:
    from vama_parser import GeminiParser

print("🔍 Scanning document_processing_log for unprocessed documents...\n")

# Query to find documents that need extraction using the log table
# Focus on Detail PDFs (mainstream VAMA members - excludes Lexus, BMW, MBV)
unprocessed_query = f"""
WITH log_status AS (
  SELECT 
    log.document_id,
    log.document_url,
    log.filename,
    log.report_year,
    log.report_month_key,
    log.parse_status,
    log.extraction_status
  FROM {CATALOG}.{SCHEMA}.document_processing_log log
  WHERE log.parse_status = 'success'
    AND log.filename LIKE '%Detail%'
    AND (log.extraction_status IS NULL OR log.extraction_status = 'failed')
),
extracted_docs AS (
  SELECT DISTINCT document_id
  FROM {CATALOG}.{SCHEMA}.sales_by_model_region
)
SELECT 
  l.document_id,
  l.document_url,
  l.filename,
  l.report_year,
  l.report_month_key
FROM log_status l
LEFT JOIN extracted_docs e ON l.document_id = e.document_id
WHERE e.document_id IS NULL  -- No rows in sales table yet
ORDER BY l.report_month_key DESC
"""

unprocessed_docs = spark.sql(unprocessed_query).collect()

if not unprocessed_docs:
    print("✅ No unprocessed documents found. All Detail PDFs have been extracted!\n")
else:
    print(f"📊 Found {len(unprocessed_docs)} unprocessed document(s) requiring LLM extraction\n")
    print("="*80)
    for doc in unprocessed_docs:
        print(f"  • {doc.document_id} - {doc.filename} ({doc.report_month_key})")
    print("="*80 + "\n")

# Convert to same format as before for processing
failed_extractions = [(doc.document_id, doc.report_month_key) for doc in unprocessed_docs]

# Store metadata for easy lookup
metadata_query = f"""
SELECT 
    document_id,
    document_url,
    filename,
    report_year,
    report_month_key
FROM {CATALOG}.{SCHEMA}.document_processing_log
WHERE document_id IN ('{"','".join([d.document_id for d in unprocessed_docs])}')
""" if unprocessed_docs else "SELECT NULL as document_id WHERE 1=0"

doc_metadata = {row.document_id: row.asDict() for row in spark.sql(metadata_query).collect()} if unprocessed_docs else {}

# Exit early if no documents to process
if not failed_extractions:
    print("✅ All documents have been extracted. Nothing to do!\n")
else:
    results = []
    print(f"\n🤖 Starting LLM extraction for {len(failed_extractions)} document(s)...\n")
    print("⚠️  Using 10-minute timeout per document (complex PDFs may take time)\n")

    for idx, (doc_id, report_month) in enumerate(failed_extractions, 1):
        metadata = doc_metadata.get(doc_id)
        
        if not metadata:
            print(f"  ⚠️ Skipping {doc_id}: metadata not found")
            continue
        
        # Derive report dates from report_month_key (format: YYYY-MM)
        try:
            report_date = datetime.strptime(metadata['report_month_key'], '%Y-%m')
            report_start_date = report_date.strftime('%Y-%m-01')
            next_month = report_date + relativedelta(months=1)
            last_day = (next_month - relativedelta(days=1)).strftime('%Y-%m-%d')
            report_end_date = last_day
        except Exception as e:
            print(f"  ⚠️ Skipping {doc_id}: date parsing error: {e}")
            continue
        
        print(f"\n[{idx}/{len(failed_extractions)}] 📄 {metadata['filename']} ({report_month})")
        print(f"    URL: {metadata['document_url'][:80]}...")
        
        start_time = datetime.now()
        
        try:
            # Delete existing (empty) extraction data
            print(f"  ⌫ Clearing any existing data for {doc_id}...")
            spark.sql(f"""
            DELETE FROM {CATALOG}.{SCHEMA}.sales_by_model_region
            WHERE document_id = '{doc_id}'
            """)
            
            # Initialize LLM parser
            print(f"  🔮 Initializing Gemini parser (model: gemini-3.1-flash-lite)...")
            parser = GeminiParser(
                spark=spark,
                dbutils=dbutils,
                document_id=doc_id,
                document_url=metadata['document_url'],
                filename=metadata['filename'],
                report_year=metadata['report_year'],
                report_month=metadata['report_month_key'],
                report_start_date=report_start_date,
                report_end_date=report_end_date,
                output_table=f"{CATALOG}.{SCHEMA}.sales_by_model_region_gemini",
                gemini_model="gemini-3.1-flash-lite",
                env_workspace_path="/Users/tuckeyhue@gmail.com/env/.env.",
                verbose=False  # Reduce verbosity for batch processing
            )
            
            # Execute extraction
            print(f"  ⏳ Parsing document (timeout: 10 minutes)...")
            result = parser.parse()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            if result['status'] == 'success':
                print(f"  ✅ Gemini parsing completed in {elapsed_time:.1f} seconds")
                print(f"    Extracted {result['rows_written']} rows")
                
                # Copy to main table with parsing_method='llm' - NULL out market share columns
                print(f"  📥 Copying results to main table...")
                spark.sql(f"""
                INSERT INTO {CATALOG}.{SCHEMA}.sales_by_model_region
                SELECT 
                    document_id, document_url, filename, report_year, report_month,
                    report_start_date, report_end_date, maker, model_name,
                    vama_classification, seat, monthly_north, monthly_central,
                    monthly_south, monthly_total, 
                    NULL as monthly_share,
                    ytd_north, ytd_central, ytd_south, ytd_total,
                    NULL as ytd_share,
                    source_table_index, source_row_index, extracted_timestamp,
                    'llm' as parsing_method
                FROM {CATALOG}.{SCHEMA}.sales_by_model_region_gemini
                WHERE document_id = '{doc_id}'
                """)
                
                # Verify insertion
                rows_inserted = spark.sql(f"""
                SELECT COUNT(*) as cnt
                FROM {CATALOG}.{SCHEMA}.sales_by_model_region
                WHERE document_id = '{doc_id}' AND parsing_method = 'llm'
                """).collect()[0].cnt
                
                print(f"    Inserted {rows_inserted} rows with parsing_method='llm'")
                
                # Update document_processing_log
                print(f"  📝 Updating processing log...")
                spark.sql(f"""
                UPDATE {CATALOG}.{SCHEMA}.document_processing_log
                SET 
                  extraction_status = 'success_llm',
                  extraction_rows_inserted = {rows_inserted},
                  extraction_timestamp = current_timestamp(),
                  updated_at = current_timestamp()
                WHERE document_id = '{doc_id}'
                """)
                
                print(f"  ✓ Successfully extracted with Gemini\n")
                
                results.append({
                    'document_id': doc_id,
                    'report_month': report_month,
                    'status': 'success',
                    'rows_inserted': rows_inserted,
                    'elapsed_seconds': elapsed_time
                })
            else:
                raise Exception(result.get('error', 'Unknown error'))
                
        except Exception as e:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            error_msg = str(e)
            print(f"  ✗ Extraction failed after {elapsed_time:.1f} seconds:")
            print(f"    Error: {error_msg[:200]}")
            
            # Update log with failure
            spark.sql(f"""
            UPDATE {CATALOG}.{SCHEMA}.document_processing_log
            SET 
              extraction_status = 'failed_llm',
              extraction_error = '{error_msg.replace("'", "''")}',
              extraction_timestamp = current_timestamp(),
              updated_at = current_timestamp()
            WHERE document_id = '{doc_id}'
            """)
            
            results.append({
                'document_id': doc_id,
                'report_month': report_month,
                'status': 'failed',
                'error': error_msg,
                'elapsed_seconds': elapsed_time
            })
    
    # Summary Report
    print("\n" + "="*80)
    print("🎯 EXTRACTION SUMMARY")
    print("="*80)
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    failed_count = len(results) - success_count
    
    print(f"\n📊 Results:")
    print(f"  Total documents processed: {len(results)}")
    print(f"  Successfully extracted: {success_count}")
    print(f"  Failed: {failed_count}")
    
    if success_count > 0:
        total_rows = sum(r['rows_inserted'] for r in results if r['status'] == 'success')
        success_times = [r['elapsed_seconds'] for r in results if r['status'] == 'success']
        avg_time = sum(success_times) / len(success_times)
        max_time = max(success_times)
        min_time = min(success_times)
        
        print(f"\n⌛ Timing Statistics:")
        print(f"  Average: {avg_time:.1f}s")
        print(f"  Range: {min_time:.1f}s - {max_time:.1f}s")
        
        print(f"\n📈 Data Extracted:")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Average per doc: {total_rows/success_count:.0f} rows")
        
        print(f"\n✅ Success! Re-run the 'Data Completeness Check' cell to verify!")
    
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} document(s) failed extraction:")
        for r in [r for r in results if r['status'] == 'failed']:
            print(f"  - {r['document_id']} ({r['report_month']}): {str(r.get('error', 'Unknown'))[:80]}...")
    
    print("\n" + "="*80)