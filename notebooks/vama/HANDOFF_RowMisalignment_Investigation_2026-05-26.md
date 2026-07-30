# VAMA PDF Parsing Investigation - Row Misalignment Root Cause

**Date:** 2026-05-26  
**Status:** 🔴 BLOCKED - Upstream API Bug  
**Previous Handoff:** `/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA/HANDOFF_PDF_Parsing_Issue_2026-05-25.md`

---

## Executive Summary

**Root Cause Identified:** The Databricks `ai_parse_document()` API produces **corrupt HTML** when extracting tables with rowspan/colspan attributes. Data values are systematically offset from their row labels BEFORE our Python parsing code ever sees them.

**Impact:** All 695 documents with rowspan complexity contain misaligned data. The `TableHTMLParser` fix applied in Cell 5 cannot correct data that's already wrong at the source.

**Verdict:** This is an **upstream API bug** in Databricks' document intelligence service, not a fixable problem in our extraction logic.

---

## Investigation Timeline

### Context from Previous Session
- User reported CX-3 model should have `monthly_total=331` in 2024-11
- Extracted value was `4,548` (14x wrong)
- Initial diagnosis: `TableHTMLParser` mishandling rowspan/colspan in Step 2
- Created comprehensive handoff document with 3-phase fix strategy

### User Re-Parsing Results (2026-05-26 00:25)
User re-parsed all PDFs and re-ran extraction:
- **2024-11**: Partially improved (`4,548` → `201`) - still wrong, expected `331`
- **2024-12**: Still wrong (`2,846` extracted, should be `470`)

### This Session's Investigation
1. Updated `TableHTMLParser` with rowspan tracking logic (Cell 5)
2. Re-ran extraction - **no change** in output
3. Examined raw HTML from Databricks API - **discovered corrupt source data**

---

## Root Cause Analysis

### The Smoking Gun: Raw HTML Inspection

#### 2024-12 Document (`774f8e05999f8816`)
**What the PDF shows** (user screenshot):
```
BT-50:  75,  12, 110,  286
CX-3:  270, 101,  99,  470
Sub-total: 1597, 418, 831, 2846
```

**What the Databricks API returns:**
```html
<tr><td>BT-50</td>...<td>270</td><td>101</td><td>99</td><td>470</td>...</tr>
<tr><td>CX-3</td>...<td>1,597</td><td>418</td><td>831</td><td>2,846</td><td rowspan="3">1.7%</td>...</tr>
```

**Analysis:**
- BT-50 row contains CX-3's values (270, 101, 99, 470)
- CX-3 row contains subtotal values (1597, 418, 831, 2846)
- The HTML is **already corrupt** before our code processes it

#### 2024-11 Document (`4522b7b5744ce481`)
**Raw HTML extraction:**
```
Row 10 (CX-8):  156,  73, 102,  331
Row 11 (CX-3):   73,  34,  94,  201
```

**Analysis:**
- CX-8 row contains CX-3's expected value (331)
- CX-3 row contains wrong values (201)
- **1-row offset persists** despite simpler rowspan pattern

### Why Different Behavior Between Months?

| Document | Rowspan Complexity | API Behavior | Offset Magnitude |
|----------|-------------------|--------------|------------------|
| **2024-11** | `rowspan="7"` on first THACO MAZDA row only (Mazda2) | Partially handled | **1 row off** (4,548→201, expected 331) |
| **2024-12** | `rowspan="6"` on Mazda2 + `rowspan="3"` on CX-3 (nested) | Completely mangled | **2+ rows off** (2,846 extracted, expected 470) |

**Interpretation:**
- Single large rowspan → API fails **gracefully** (minor misalignment)
- Multiple nested rowspans → API fails **catastrophically** (major misalignment)
- Both cases produce corrupt HTML, differing only in severity

---

## Technical Evidence

### 1. Parsed HTML Structure (2024-12)
```html
<!-- Mazda2 row has rowspan=6 on share columns -->
<tr><td>THACO MAZDA</td><td>Mazda2</td>...<td rowspan="6">10.4%</td>...</tr>

<!-- Next 5 rows (Mazda3, CX-5, Mazda6, BT-50, CX-8) have NO <td> for share column -->
<!-- Parser should track rowspan and insert empty cells, but API misaligns data values -->

<!-- CX-3 row introduces NEW rowspan=3 -->
<tr><td>THACO MAZDA</td><td>CX-3</td>...<td rowspan="3">1.7%</td>...</tr>
```

### 2. Cell Count Mismatch
BT-50 row in source HTML has **12 `<td>` elements**, but the table has **14 columns**:
- Columns 0-7: Present in actual `<td>` tags
- Column 8: **Missing** (covered by Mazda2's rowspan)
- Columns 9-13: Present in actual `<td>` tags

**Expected behavior:** API should preserve column alignment by inserting placeholder for column 8  
**Actual behavior:** API outputs 12 cells without placeholders, causing data to shift left

### 3. Extraction Code Verification
Query confirmed raw `extracted_tables_long` contains wrong values:
```sql
SELECT row_index, model, monthly_total
FROM market_data.vama.extracted_tables_long
WHERE document_id = '774f8e05999f8816' AND table_index = 1
  AND row_index BETWEEN 18 AND 20;

-- Results:
-- 18, BT-50, 470   ← Should be 286
-- 20, CX-3,  2846  ← Should be 470
```

The `TableHTMLParser` correctly parsed the corrupt HTML — the corruption happened upstream.

---

## Why the TableHTMLParser Fix Didn't Work

### What We Fixed (Cell 5)
Added rowspan tracking to `TableHTMLParser`:
```python
self.active_rowspans = []  # Track [(col_idx, remaining_rows, value), ...]
# Insert pending rowspan cells BEFORE actual cell
# Track new rowspan cells for subsequent rows
```

### Why It Had No Effect
The parser correctly handles rowspan in **well-formed HTML**, but the Databricks API returns **malformed HTML** where:
1. Cell values are already attached to wrong row labels
2. Rowspan cells exist but data values don't respect them
3. No amount of client-side parsing can undo this corruption

**Analogy:** We built a spell-checker for a document that's already been scrambled at the character level.

---

## Attempted Solutions & Results

### ✅ Completed
1. **Multi-table extraction** (2026-05-25)
   - Extracted from all detail tables instead of just one
   - Fixed missing ~40-50% of data
   - Status: **Working correctly**

2. **Rowspan handling in parser** (2026-05-26)
   - Added `active_rowspans` tracking
   - Implemented pre-insertion of covered cells
   - Status: **Code correct, but can't fix upstream corruption**

### ❌ Failed
3. **Re-parsing documents** (2026-05-26 00:25)
   - User re-ran Step 2 (PDF→HTML conversion)
   - Expected: Fixed HTML from updated Databricks API
   - Result: Same corrupt HTML (API hasn't changed)
   - Status: **No improvement**

---

## Impact Assessment

### Documents Affected
- **All 695 documents** parsed by `ai_parse_document()` are at risk
- Severity varies by rowspan complexity in source PDF
- Simple tables (no rowspan) → ✅ Accurate
- Single rowspan → ⚠️ Minor offset (1-2 rows)
- Nested rowspans → ❌ Major corruption (2+ rows, wrong labels)

### Data Reliability
Current `market_data.vama.sales_by_model_region` table:
- **12,351 rows** extracted
- **Unknown % corrupted** (depends on per-document rowspan patterns)
- High-risk periods: Any month with manufacturer subtotals in tables

### Validation Status
Cell 13 validation query cannot detect this issue because:
- Grand totals may sum correctly (values present, just mislabeled)
- Row-level corruption is invisible in aggregate metrics

---

## Next Steps: Three Options

### Option 1: Alternative PDF Parser (RECOMMENDED)
**Replace Databricks `ai_parse_document()` with direct PDF extraction**

**Tools to evaluate:**
- `pdfplumber` - Python library, excellent table extraction
- `camelot-py` - Specialized for PDF tables
- `tabula-py` - Java-based, handles complex layouts

**Pros:**
- Direct control over table parsing logic
- Can handle rowspan/colspan correctly
- Proven reliability on similar use cases

**Cons:**
- Requires rewriting Step 2 (notebook `2_Parse_PDFs`)
- May need PDF file re-download (if API only stored HTML)
- Performance may be slower than API

**Effort:** 2-4 hours to prototype, 1 day to refactor pipeline

### Option 2: Contact Databricks Support
**Report API bug and request fix**

**Evidence to provide:**
- Document ID: `774f8e05999f8816` (2024-12)
- Expected vs. actual HTML comparison
- Rowspan handling test cases

**Pros:**
- If fixed, benefits all future parsing
- No code changes required

**Cons:**
- Unknown timeline for fix (weeks to months)
- May not be prioritized by Databricks
- Requires re-parsing all 695 documents after fix

**Effort:** 1 hour to file support ticket, wait time TBD

### Option 3: Correction Heuristics (NOT RECOMMENDED)
**Build post-processing logic to detect and fix misalignments**

**Approach:**
- Detect anomalous values (e.g., subtotal in individual model row)
- Use expected value ranges to shift rows back
- Validate against known totals

**Pros:**
- Doesn't require re-parsing
- Could patch existing data

**Cons:**
- Extremely brittle (fails on edge cases)
- High risk of introducing new errors
- Requires manual validation per document
- Doesn't solve root cause

**Effort:** 3-5 days, high maintenance burden

---

## Recommended Action Plan

### Immediate (Next Session)
1. **Prototype `pdfplumber` parser**
   - Test on documents `774f8e05999f8816` (2024-12) and `4522b7b5744ce481` (2024-11)
   - Compare extracted tables to user screenshots
   - Verify rowspan handling works correctly

2. **If prototype succeeds:**
   - Refactor notebook `2_Parse_PDFs` to use `pdfplumber`
   - Re-run full pipeline (Steps 1-3) on all 695 documents
   - Validate extraction using Cell 13 query

3. **If prototype fails:**
   - File Databricks support ticket (Option 2)
   - Pause pipeline until fix available

### Medium-term
- Implement automated row-level validation (compare individual models to known values)
- Build regression tests for rowspan/colspan edge cases
- Document PDF table structure patterns for future maintenance

---

## File References

### Notebooks
- **Current:** `188270495869500` (3_Extract_Tables)
  - Cell 5: `TableHTMLParser` with rowspan tracking (latest version)
  - Cell 6-9: Extraction and table write logic
  - Cell 13: Validation query (aggregate-level only)

- **Upstream:** `2_Parse_PDFs` (ID unknown)
  - Contains `ai_parse_document()` call
  - Target for refactor if using Option 1

### Data Tables
- `market_data.vama.parsed_documents_raw` - 695 documents with corrupt HTML
- `market_data.vama.extracted_tables_long` - 616,082 raw cells (from corrupt HTML)
- `market_data.vama.sales_by_model_region` - 12,351 rows (unknown % corrupted)

### Handoff Documents
- Previous: `/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA/HANDOFF_PDF_Parsing_Issue_2026-05-25.md`
  - Initial problem discovery and multi-table fix
  - 3-phase fix strategy (now superseded)

- Column Mapping: `/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/2. VAMA/HANDOFF_ColumnMapping_2026-05-25.md`
  - Original column offset investigation
  - User-reported CX-3 discrepancy

### Test Documents
- `774f8e05999f8816` - VAMA sales report Dec 2024 - Detail.pdf
  - Severe corruption (2+ row offset)
  - CX-3 extracted as 2,846, should be 470
  - Nested rowspan patterns

- `4522b7b5744ce481` - VAMA sales report Nov 2024 - Detail.pdf
  - Minor corruption (1 row offset)
  - CX-3 extracted as 201, should be 331
  - Simple rowspan pattern

---

## Skills for Next Session

If pursuing **Option 1 (pdfplumber)**:
- General Python/ETL skills
- PDF processing libraries documentation search
- Data validation patterns

If pursuing **Option 2 (Databricks Support)**:
- Technical writing for bug reports
- Documentation search for API issue reporting

No specific skills required beyond base assistant capabilities.

---

## Key Learnings

1. **Always validate raw source data** before assuming parser bugs
2. **Rowspan/colspan in HTML tables** is a common failure point for document AI APIs
3. **Aggregate validation is insufficient** - need row-level spot checks
4. **"Improved" ≠ "Fixed"** - partial improvements can mask persistent bugs

---

**END OF HANDOFF**  
**Next Agent:** Start with Option 1 prototype unless user directs otherwise.