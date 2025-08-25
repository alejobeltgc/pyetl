# Copilot Instructions for the pyetl Project

This document provides guidance for AI coding agents to be productive in this codebase.

## Project Overview

The pyetl project is a production-ready ETL pipeline that processes Excel files containing financial rates and fees for various business lines (accounts, credit, etc.). The pipeline extracts data from multi-sheet Excel files, transforms it according to business rules, validates the output, and produces structured JSON ready for DynamoDB and API consumption.

**Architecture Flow:**
1. **Excel Parser** → Extracts multiple tables from Excel sheets
2. **Business Rules** → Transforms raw data using configurable rules
3. **Validator** → Validates output against business requirements
4. **Output** → Structured JSON with metadata and validation reports

**Current Focus:** Production-ready pipeline with comprehensive validation, error handling, and extensible architecture for multiple business lines.

## Key Technologies and Libraries

- **Python 3.8+**: Core language
- **Pandas**: Excel processing and data manipulation
- **Openpyxl**: Excel file reading
- **Pytest**: Testing framework (future)
- **JSON**: Output format for DynamoDB compatibility

## Project Structure

```
pyetl/
├── main.py                    # 🚀 Main ETL orchestrator - START HERE
├── config.py                  # ⚙️ Global configuration
├── data/                      # 📁 Input Excel files
├── output/                    # 📁 Generated outputs (raw, transformed, validation)
└── src/                       # 🔧 Core components
    ├── excel_parser.py        # Multi-table Excel parsing
    ├── business_rules/        # Business logic per line
    │   └── accounts_rules.py  # Accounts-specific transformations
    └── config/                # Business line configurations
        └── accounts_config.py # Accounts field mappings & rules
```

## Developer Workflow

### 1. Running the Pipeline
```bash
# Process default file
python main.py

# Process specific file  
python main.py --file data/your-file.xlsx

# Different business line
python main.py --business-line credit
```

### 2. Key Development Points

**Excel Parser (`src/excel_parser.py`)**:
- Detects multiple tables within single sheets
- Handles variable column counts per table
- Automatic header detection and column naming
- Robust boundary detection for table separation

**Business Rules (`src/business_rules/accounts_rules.py`)**:
- Table type classification (mobile_plans, transfers, withdrawals, traditional_services)
- Rate parsing including conditional rates ("3 included free, $X additional")
- Field normalization (frequencies, tax flags, currencies)
- Service ID generation from descriptions

**Main Pipeline (`main.py`)**:
- Orchestrates complete ETL process
- Comprehensive validation with detailed reporting
- Structured output with metadata tracking
- Error handling and exit codes

## Business Rules Architecture

The system uses configurable business rules per line:

**Classification Patterns** (`src/config/accounts_config.py`):
```python
"table_classification": {
    "mobile_plans": {
        "patterns": ["planes", "app", "movil"],
        "required_columns": ["g_zero", "puls", "premier"]
    }
}
```

**Rate Parsing**:
- Fixed rates: `{"type": "fixed", "value": 8990}`
- Conditional: `{"type": "conditional", "included_free": 3, "additional_cost": 7510}`
- Unlimited: `{"type": "unlimited", "value": 0}`

## Output Format

Produces DynamoDB-ready JSON:
```json
{
  "business_line": "accounts",
  "document_version": "v1", 
  "last_updated": "2025-08-25T11:26:59Z",
  "tables": {
    "mobile_plans": {
      "services": [
        {
          "service_id": "app_opening",
          "description": "Planes abiertos desde app Vivibanco",
          "rates": {
            "g_zero": {"type": "fixed", "value": 0},
            "puls": {"type": "fixed", "value": 8990}
          },
          "applies_tax": true,
          "frequency": "monthly"
        }
      ]
    }
  }
}
```

## Validation Framework

**Built-in Validation** (`config.py`):
- Required fields per service
- Valid frequency values
- Rate structure validation  
- Description length limits
- Cross-table consistency checks

**Validation Reports**:
- Errors (pipeline fails)
- Warnings (pipeline continues)
- Statistics and processing metrics

## Adding New Business Lines

1. **Create config**: `src/config/credit_config.py`
2. **Create rules**: `src/business_rules/credit_rules.py` 
3. **Update main**: Add to `ETLPipeline.__init__()` business line mapping
4. **Test**: Run with `--business-line credit`

## Testing Strategy

- Place sample Excel files in `data/`
- Run pipeline and check `output/03_validation_report.json`
- Verify table classification and service extraction
- Test edge cases: empty tables, malformed data, missing headers

## Common Patterns

**Table Detection**: Headers repeat to indicate new table starts
**Rate Handling**: Complex text parsing for conditional rates  
**Service IDs**: Generated from description keywords using patterns
**Extensibility**: New business lines follow same config + rules pattern

## Key Files to Understand

1. **`main.py`** - Overall pipeline orchestration
2. **`src/excel_parser.py`** - Table detection and extraction logic  
3. **`src/business_rules/accounts_rules.py`** - Business transformation examples
4. **`config.py`** - Global settings and validation rules

The codebase prioritizes robustness, configurability, and clear separation of concerns for handling complex financial data transformations.
