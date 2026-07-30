# Databricks notebook source
# DBTITLE 1,Other Makers Extraction - BMW, Lexus, MBV
# MAGIC %md
# MAGIC # Step 3b: Extract BMW, Lexus, and MBV Tables
# MAGIC
# MAGIC This notebook extracts sales data from **non-mainstream VAMA members**:
# MAGIC - **BMW**
# MAGIC - **Lexus**
# MAGIC - **Mercedes-Benz Vietnam (MBV)**
# MAGIC
# MAGIC **Output Table**: `market_data.vama.sales_by_other_makers`
# MAGIC
# MAGIC **Schema**: Same as `sales_by_model_region` (maker, model, classification, seat, regional sales columns)
# MAGIC
# MAGIC **Note**: Mainstream VAMA members are extracted separately in the main `3_Extract_Tables` notebook.

# COMMAND ----------

# DBTITLE 1,Reload Scope
# This notebook used to begin with an unconditional
#     DELETE FROM market_data.vama.sales_by_other_makers
# which only worked because it was run by hand immediately before a full
# re-extract. On a schedule it emptied the table: notebook 3 marks every document
# it touches - BMW/Lexus/MBV included - as extraction_status='success', so the
# document selection further down matched nothing and the deleted rows were never
# rewritten. Deletes are now scoped to the documents actually being re-extracted,
# just before the append.

# COMMAND ----------

# DBTITLE 1,Import Dependencies
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

# DBTITLE 1,Create Target Table
# Create sales_by_other_makers table with same schema as sales_by_model_region
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.sales_by_other_makers (
  document_id STRING NOT NULL,
  report_month STRING NOT NULL,
  report_year INT,
  report_type STRING,
  maker STRING,
  model STRING,
  classification STRING,
  seat STRING,
  north INT,
  south INT,
  monthly_total INT,
  market_share_month DECIMAL(5,2),
  ytm_north INT,
  ytm_south INT,
  ytm_total INT,
  market_share_ytm DECIMAL(5,2),
  extracted_timestamp TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,HTML Table Parser
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
        elif tag in ['td', 'th'] and self.in_row:
            self.in_cell = True
            self.current_cell = ''
            self.current_colspan = 1
            for attr, value in attrs:
                if attr == 'colspan':
                    self.current_colspan = int(value)

    def handle_endtag(self, tag):
        if tag == 'table' and self.in_table:
            self.in_table = False
            if self.current_table:
                self.tables.append(self.current_table)
        elif tag == 'tr' and self.in_row:
            self.in_row = False
            if self.current_row:
                self.current_table.append(self.current_row)
        elif tag in ['td', 'th'] and self.in_cell:
            self.in_cell = False
            for _ in range(self.current_colspan):
                self.current_row.append(self.current_cell.strip())

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

def parse_tables(parsed_json):
    """Extract HTML tables from parsed JSON. Supports both legacy and Gemini formats."""
    try:
        data = json.loads(parsed_json)
        
        # Format 1: Direct tables array
        if isinstance(data, dict) and 'tables' in data:
            return data['tables']
        
        # Format 2: Direct list
        elif isinstance(data, list):
            return data
        
        # Format 3: Gemini PDF format with document.elements
        elif isinstance(data, dict) and 'document' in data:
            doc = data['document']
            if isinstance(doc, dict) and 'elements' in doc:
                # Extract HTML from table elements
                table_elements = [e for e in doc['elements'] if e.get('type') == 'table']
                tables = []
                for elem in table_elements:
                    html_content = elem.get('content', '')
                    if html_content:
                        parser = TableHTMLParser()
                        parser.feed(html_content)
                        tables.extend(parser.tables)
                return tables
        
        # Format 4: Generic HTML
        else:
            html = data.get('html', '') if isinstance(data, dict) else str(data)
            parser = TableHTMLParser()
            parser.feed(html)
            return parser.tables
            
    except Exception as e:
        print(f"Parse error: {e}")
        return []

def clean_cell(cell):
    """Clean cell text."""
    if not cell:
        return ''
    return re.sub(r'\s+', ' ', str(cell)).strip()

# COMMAND ----------

# DBTITLE 1,Maker Filtering Strategy
# MAGIC %md
# MAGIC ### 🎯 Document Filtering
# MAGIC
# MAGIC This notebook **includes ONLY** documents with titles containing:
# MAGIC - **BMW**
# MAGIC - **Lexus**
# MAGIC - **MBV** or **Mercedes-Benz**
# MAGIC
# MAGIC All other makers are processed in the main extraction notebook.

# COMMAND ----------

# DBTITLE 1,Extract Sales Data from Other Makers
def extract_sales_rows(doc, tables):
    """Extract sales rows from BMW/Lexus/MBV tables - supports both 2-row and 3-row headers."""
    sales_rows = []
    now = datetime.utcnow()
    
    for table_idx, table in enumerate(tables):
        if len(table) < 3:  # Need at least 2 header rows + 1 data row
            continue
        
        # Detect header format by checking if row 1 contains regional keywords
        row1 = [clean_cell(c).upper() for c in table[1]]
        has_regions_row1 = any(region in h for h in row1 for region in ['NORTH', 'CENTRAL', 'SOUTH'])
        
        if has_regions_row1:
            # 2-row header format (newer documents)
            header = row1
            data_start_row = 2
        else:
            # 3-row header format (older documents) - use row 2 as header
            if len(table) < 4:
                continue
            header = [clean_cell(c).upper() for c in table[2]]
            has_regions = any(region in h for h in header for region in ['NORTH', 'CENTRAL', 'SOUTH'])
            if not has_regions:
                continue
            data_start_row = 3
        
        # Map column indices
        col_map = {}
        for i, h in enumerate(header):
            if 'COMPANY' in h:
                col_map['company'] = i
            elif 'BRAND' in h:
                col_map['brand'] = i
            elif 'SEGMENT' in h or 'CLASS' in h:
                col_map['segment'] = i
            elif 'NORTH' in h and 'YTM' not in header[max(0, i-2):i+3]:
                if 'north' not in col_map:
                    col_map['north'] = i
            elif 'CENTRAL' in h and 'YTM' not in header[max(0, i-2):i+3]:
                if 'central' not in col_map:
                    col_map['central'] = i
            elif 'SOUTH' in h and 'YTM' not in header[max(0, i-2):i+3]:
                if 'south' not in col_map:
                    col_map['south'] = i
            elif 'TOTAL' in h and 'YTM' not in header[max(0, i-2):i+3]:
                if 'total' not in col_map:
                    col_map['total'] = i
        
        # Extract data rows
        for row_idx, row in enumerate(table[data_start_row:], start=data_start_row):
            if not row or len(row) < 3:
                continue
            
            company = clean_cell(row[col_map.get('company', 0)]).upper()
            brand = clean_cell(row[col_map.get('brand', 1)]).upper()
            segment = clean_cell(row[col_map.get('segment', 2)]).upper()
            
            # Skip all TOTAL/SUBTOTAL rows - check all text fields
            total_keywords = ['TOTAL', 'SUBTOTAL', 'TỔNG', 'CỘNG', 'SUM', 'AGGREGATE']
            is_total_row = any(
                keyword in field 
                for field in [company, brand, segment]
                for keyword in total_keywords
            )
            
            # Also skip rows with empty company AND brand (likely subtotals)
            if is_total_row or (not company and not brand):
                continue
            
            try:
                def parse_num(val):
                    if not val:
                        return None
                    try:
                        return int(re.sub(r'[^0-9]', '', str(val)))
                    except:
                        return None
                
                monthly_north = parse_num(row[col_map['north']]) if 'north' in col_map and col_map['north'] < len(row) else None
                monthly_central = parse_num(row[col_map['central']]) if 'central' in col_map and col_map['central'] < len(row) else None
                monthly_south = parse_num(row[col_map['south']]) if 'south' in col_map and col_map['south'] < len(row) else None
                monthly_total = parse_num(row[col_map['total']]) if 'total' in col_map and col_map['total'] < len(row) else None
                
                if not monthly_total:
                    monthly_total = (monthly_north or 0) + (monthly_central or 0) + (monthly_south or 0)
                
                maker = brand if brand else company
                
                sales_rows.append({
                    'document_id': doc['document_id'],
                    'document_url': doc.get('document_url'),
                    'filename': doc.get('filename'),
                    'report_year': doc.get('report_year'),
                    'report_month': doc.get('report_month_key'),
                    'report_start_date': None,
                    'report_end_date': None,
                    'maker': maker,
                    'model_name': segment,
                    'vama_classification': segment,
                    'seat': '',
                    'monthly_north': monthly_north,
                    'monthly_central': monthly_central,
                    'monthly_south': monthly_south,
                    'monthly_total': monthly_total,
                    'monthly_share': None,
                    'ytd_north': None,
                    'ytd_central': None,
                    'ytd_south': None,
                    'ytd_total': None,
                    'ytd_share': None,
                    'source_table_index': table_idx,
                    'source_row_index': row_idx,
                    'extracted_timestamp': now,
                    'parsing_method': 'gemini_html'
                })
            except Exception as e:
                print(f"Row extraction error: {e}")
                continue
    
    return sales_rows

# Query documents - filter by title, then by what this notebook has yet to do.
#
# Progress is tracked against sales_by_other_makers itself rather than
# document_processing_log.extraction_status: that column is owned by notebook 3,
# which sets 'success' on every document it processes, so keying off it made this
# selection return zero rows. A document is in scope when it has no rows here yet,
# or when it has been re-parsed since its rows were written.
dbutils.widgets.text("reextract_all", "false")
dbutils.widgets.text("only_months", "")

REEXTRACT_ALL = dbutils.widgets.get("reextract_all").strip().lower() == "true"
ONLY_MONTHS = [m.strip() for m in dbutils.widgets.get("only_months").split(",") if m.strip()]

filters = [
    "log.parse_status = 'success'",
    """(raw.title LIKE '%BMW%'
         OR raw.title LIKE '%Lexus%'
         OR raw.title LIKE '%MBV%'
         OR raw.title LIKE '%Mercedes-Benz%')""",
]
if not REEXTRACT_ALL:
    filters.append("""(
        done.document_id IS NULL
        OR raw.parsed_timestamp > done.last_extracted
    )""")
if ONLY_MONTHS:
    months_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in ONLY_MONTHS)
    filters.append(f"raw.report_month_key IN ({months_sql})")

raw_docs = spark.sql(f"""
WITH already_extracted AS (
  SELECT document_id, MAX(extracted_timestamp) AS last_extracted
  FROM {CATALOG}.{SCHEMA}.sales_by_other_makers
  GROUP BY document_id
),
ranked_docs AS (
  SELECT raw.*,
    ROW_NUMBER() OVER (PARTITION BY raw.document_id ORDER BY raw.parsed_timestamp DESC) as rn
  FROM {CATALOG}.{SCHEMA}.parsed_documents_raw raw
  JOIN {CATALOG}.{SCHEMA}.document_processing_log log
    ON raw.document_id = log.document_id
  LEFT JOIN already_extracted done
    ON raw.document_id = done.document_id
  WHERE {" AND ".join(filters)}
)
SELECT document_id, document_url, title, filename, report_year, report_month, report_month_key, report_type, parsed_json, parsed_timestamp
FROM ranked_docs
WHERE rn = 1
""").collect()

print(f"reextract_all={REEXTRACT_ALL}  only_months={ONLY_MONTHS or '(all)'}")
print(f"Documents to extract (BMW/Lexus/MBV): {len(raw_docs)}")
sales_rows = []

for doc_row in raw_docs:
    doc = doc_row.asDict()
    try:
        tables = parse_tables(doc['parsed_json'])
        extracted = extract_sales_rows(doc, tables)
        sales_rows.extend(extracted)
        if len(extracted) > 0:
            print(f"✓ {doc['title'][:50]}: {len(extracted)} rows")
    except Exception as e:
        print(f"FAILED {doc['document_id']}: {e}")

print(f"\nTotal sales rows extracted: {len(sales_rows)}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from market_data.vama.document_processing_log

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
# Write extracted sales data to Delta table
if sales_rows:
    # Filter out rows with missing required document_id
    valid_rows = []
    for row in sales_rows:
        if row.get('document_id'):
            valid_rows.append(row)
        else:
            print(f"⚠️ Skipping row missing document_id: {row.get('maker', 'unknown')}")
    
    print(f"\nValid rows to write: {len(valid_rows)} of {len(sales_rows)}")
    
    if valid_rows:
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
            StructField('monthly_share', DecimalType(10, 4), True),
            StructField('ytd_north', IntegerType(), True),
            StructField('ytd_central', IntegerType(), True),
            StructField('ytd_south', IntegerType(), True),
            StructField('ytd_total', IntegerType(), True),
            StructField('ytd_share', DecimalType(10, 4), True),
            StructField('source_table_index', IntegerType(), True),
            StructField('source_row_index', IntegerType(), True),
            StructField('extracted_timestamp', TimestampType(), True),
            StructField('parsing_method', StringType(), True)
        ])
        
        sales_df = spark.createDataFrame(valid_rows, schema=sales_schema)

        # The write below is append-mode, so clear this batch's documents first or
        # a re-extract would double their rows. Scoped to the batch, unlike the
        # whole-table DELETE this notebook used to open with.
        batch_doc_ids = sorted({row['document_id'] for row in valid_rows})
        ids_sql = ", ".join("'" + d.replace("'", "''") + "'" for d in batch_doc_ids)
        spark.sql(f"""
        DELETE FROM {CATALOG}.{SCHEMA}.sales_by_other_makers
        WHERE document_id IN ({ids_sql})
        """)
        print(f"Cleared prior rows for {len(batch_doc_ids)} document(s) before append")

        sales_df.write.mode('append').saveAsTable(f'{CATALOG}.{SCHEMA}.sales_by_other_makers')
        print(f"✓ Written {len(valid_rows)} rows to {CATALOG}.{SCHEMA}.sales_by_other_makers")
    else:
        print("⚠️ No valid rows to write")
else:
    print("⚠️ No sales rows to write")

# COMMAND ----------

# DBTITLE 1,Update Extraction Status in Log
# Update document_processing_log for successfully extracted documents
if sales_rows:
    # Get unique document_ids that were successfully extracted
    extracted_doc_ids = list(set([row['document_id'] for row in sales_rows if row.get('document_id')]))
    
    if extracted_doc_ids:
        # Create temp view with successfully extracted document IDs
        from pyspark.sql.functions import current_timestamp
        
        extracted_docs_df = spark.createDataFrame(
            [(doc_id,) for doc_id in extracted_doc_ids],
            ['document_id']
        )
        extracted_docs_df.createOrReplaceTempView('extracted_docs')
        
        # Update extraction_status to 'success' for these documents
        spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.document_processing_log AS target
        USING extracted_docs AS source
        ON target.document_id = source.document_id
        WHEN MATCHED THEN UPDATE SET
            extraction_status = 'success',
            extraction_timestamp = current_timestamp(),
            extraction_error_message = NULL
        """)
        
        print(f"✓ Updated extraction_status to 'success' for {len(extracted_doc_ids)} documents")
        
        # Show updated documents
        updated = spark.sql(f"""
        SELECT document_id, title, extraction_status, extraction_timestamp
        FROM {CATALOG}.{SCHEMA}.document_processing_log
        WHERE document_id IN (SELECT document_id FROM extracted_docs)
        ORDER BY extraction_timestamp DESC
        """)
        display(updated)
    else:
        print("⚠️ No document IDs to update in log")
else:
    print("⚠️ No sales rows extracted - log table not updated")

# COMMAND ----------

# DBTITLE 1,Verify Extraction Results
# MAGIC %sql
# MAGIC SELECT maker, report_month, SUM(monthly_total) as total_sales
# MAGIC FROM market_data.vama.sales_by_other_makers
# MAGIC GROUP BY maker, report_month
# MAGIC ORDER BY report_month DESC, maker
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Sample Extracted Data
# MAGIC %sql
# MAGIC SELECT report_month,sum(monthly_total)
# MAGIC FROM market_data.vama.sales_by_other_makers
# MAGIC group by all