# PyETL - Financial Rates & Fees ETL Pipeline

A robust ETL pipeline for processing Excel files containing financial rates and fees data, transforming them into structured JSON format ready for DynamoDB and API consumption.

## 🏗️ Project Structure

```
pyetl/
├── main.py                    # 🚀 Main ETL pipeline orchestrator
├── config.py                  # ⚙️ Global configuration
├── requirements.txt           # 📦 Python dependencies
├── data/                      # 📁 Input Excel files
│   └── tasas-y-tarifas.xlsx  # Sample data file
├── output/                    # 📁 Generated output files
│   ├── 01_raw_extracted.json # Raw extracted tables
│   ├── 02_transformed.json   # Business-transformed data
│   └── 03_validation_report.json # Validation results
├── src/                       # 🔧 Core components
│   ├── excel_parser.py       # Excel parsing logic
│   ├── transformer.py        # Data transformation (deprecated)
│   ├── business_rules/       # Business logic
│   │   ├── __init__.py
│   │   └── accounts_rules.py # Accounts-specific rules
│   ├── config/               # Configuration modules
│   │   ├── __init__.py
│   │   └── accounts_config.py # Accounts configuration
│   └── utils/                # Utility functions
└── tests/                     # 🧪 Test files (future)
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Process Excel File

```bash
# Process default file (data/tasas-y-tarifas.xlsx)
python main.py

# Process specific file
python main.py --file data/your-file.xlsx

# Process for different business line
python main.py --business-line credit

# Skip intermediate files
python main.py --no-intermediate
```

### 3. Check Results

Output files are saved in the `output/` directory:
- `01_raw_extracted.json` - Raw tables extracted from Excel
- `02_transformed.json` - Business-ready structured data
- `03_validation_report.json` - Validation results and quality checks

## 📊 Supported Data Types

### Accounts Business Line
- **Mobile Plans**: G-Zero, Puls, Premier plan rates
- **Transfers**: ACH, Transfiya, keys-based transfers
- **Withdrawals**: ATM, branch, correspondent banking
- **Traditional Services**: Account maintenance, statements, certificates

### Features
- ✅ **Automatic Table Detection**: Identifies multiple tables within Excel sheets
- ✅ **Business Rules**: Configurable transformation rules per business line
- ✅ **Data Validation**: Comprehensive validation with error/warning reports
- ✅ **Rate Parsing**: Handles complex rate structures (fixed, conditional, unlimited)
- ✅ **Normalization**: Standardizes frequencies, tax flags, currencies
- ✅ **Service ID Generation**: Automatic meaningful ID creation

## 🔧 Configuration

Edit `config.py` to customize:
- File paths and naming conventions
- Processing parameters
- Validation rules
- Business line settings

Business-specific rules are in `src/config/accounts_config.py`:
- Plan type mappings
- Field normalizations
- Service ID patterns

## 📈 Output Format

The pipeline produces structured JSON ready for DynamoDB:

```json
{
  "business_line": "accounts",
  "document_type": "rates_and_fees",
  "document_version": "v1",
  "last_updated": "2025-08-25T11:26:59.536136+00:00",
  "tables": {
    "mobile_plans": {
      "services": [
        {
          "service_id": "app_opening",
          "description": "Planes abiertos desde app Vivibanco",
          "rates": {
            "g_zero": {"type": "fixed", "value": 0},
            "puls": {"type": "fixed", "value": 8990},
            "premier": {"type": "conditional", "included_free": 3, "additional_cost": 7510}
          },
          "applies_tax": true,
          "frequency": "monthly"
        }
      ]
    }
  }
}
```

## 🧪 Validation

The pipeline includes comprehensive validation:
- **Structure validation**: Required fields, data types
- **Business rules**: Valid frequencies, rate formats
- **Data quality**: Description length, missing values
- **Cross-checks**: Consistency across plans

Exit codes:
- `0`: Success
- `1`: Validation failed (errors)
- `2`: Success with warnings

## 🔮 Future Enhancements

- **Credit Business Line**: Support for credit products
- **AWS Integration**: Lambda, S3, DynamoDB connectors
- **API Layer**: REST API for data consumption
- **Advanced Validation**: Business logic validation
- **Monitoring**: Processing metrics and alerts
- **Testing**: Comprehensive test suite

## 📝 Development

The codebase is structured for easy extension:

1. **New Business Lines**: Add config in `src/config/` and rules in `src/business_rules/`
2. **New Table Types**: Extend classification in business rules
3. **Custom Validations**: Add rules to `config.py` validation section
4. **New Output Formats**: Extend the pipeline with additional transformers

## 🏷️ Version

Current version: **v1.0** (Production Ready)
- ✅ Complete Excel parsing
- ✅ Business transformation
- ✅ Validation framework
- ✅ Error handling
- ✅ Configurable pipeline
