# PyETL - Serverless Financial Rates ETL Pipeline

A cloud-native, serverless ETL pipeline for processing Excel files containing financial rates and fees data. Files are automatically processed when uploaded to S3, transformed using business rules, and stored in DynamoDB with API access.

## 🏗️ Architecture

```
📁 S3 Bucket          🔥 Lambda Functions         � DynamoDB
   (Excel files)   →    (ETL Processing)     →     (Structured data)
       ↓                       ↓                         ↓
   Auto-trigger          Business Rules           🌐 REST API
                         Validation               (Query interface)
```

### AWS Services Used
- **S3**: Excel file storage and triggers
- **Lambda**: Serverless ETL processing 
- **DynamoDB**: NoSQL database for structured data
- **API Gateway**: REST endpoints for data access
- **CloudWatch**: Logging and monitoring

## 🚀 Deployment Options

### Option 1: Serverless (Recommended)
Deploy to AWS with automatic scaling and pay-per-use pricing.

### Option 2: Local Development
Run the ETL pipeline locally for development and testing.

---

## ☁️ Serverless Deployment

### Prerequisites
- AWS CLI configured with appropriate permissions
- Node.js (for Serverless Framework)
- Python 3.9+

### Quick Deploy

```bash
# Install dependencies
npm install
pip install -r requirements.txt

# Deploy to AWS
./deploy.sh dev

# Deploy to production
./deploy.sh prod
```

### Manual Setup

```bash
# Install Serverless Framework
npm install -g serverless

# Install plugins
npm install

# Deploy
serverless deploy --stage dev
```

### AWS Resources Created
- **S3 Bucket**: `pyetl-serverless-{stage}-files`
- **DynamoDB Table**: `pyetl-serverless-{stage}-rates`
- **Lambda Functions**: ETL processor + API handlers
- **API Gateway**: REST endpoints
- **IAM Roles**: Least-privilege access

## 📤 Using the Serverless Pipeline

### 1. Upload Excel File
```bash
# Get the S3 bucket name from deployment output
aws s3 cp data/tasas-y-tarifas.xlsx s3://pyetl-serverless-dev-files/

# Or use the upload script
python scripts/upload_test_file.py pyetl-serverless-dev-files data/tasas-y-tarifas.xlsx
```

### 2. Monitor Processing
```bash
# Watch Lambda logs
serverless logs --function processRatesFile --tail

# Check processing status
curl https://your-api-gateway-url/health
```

### 3. Query Results via API

**List all documents:**
```bash
curl https://your-api-gateway-url/api/documents
```

**Get specific document:**
```bash
curl https://your-api-gateway-url/api/documents/{document_id}
```

**Query services by business line:**
```bash
curl https://your-api-gateway-url/api/services/accounts?service_id=app_opening
```

---

## �️ Local Development

### 1. Setup Environment
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Process Excel File Locally
```bash
# Process default file (data/tasas-y-tarifas.xlsx)
python main.py

# Process specific file
python main.py --file data/your-file.xlsx

# Process for different business line
python main.py --business-line credit
```

### 3. Check Results
Output files are saved in the `output/` directory:
- `01_raw_extracted.json` - Raw tables extracted from Excel
- `02_transformed.json` - Business-ready structured data
- `03_validation_report.json` - Validation results and quality checks

---

## 🏗️ Project Structure

```
pyetl/
├── serverless.yml             # 🚀 AWS infrastructure definition
├── package.json               # 📦 Node.js dependencies
├── deploy.sh                  # 🔧 Deployment script
├── main.py                    # 💻 Local ETL orchestrator
├── config.py                  # ⚙️ Global configuration
├── requirements.txt           # 🐍 Python dependencies
├── data/                      # 📁 Sample Excel files
├── output/                    # 📁 Local output files
├── scripts/                   # 🛠️ Utility scripts
│   ├── test_local.py         # Local testing
│   └── upload_test_file.py   # S3 upload helper
└── src/                       # 🔧 Core components
    ├── lambda_handler.py      # ☁️ AWS Lambda handlers
    ├── api_handler.py         # 🌐 API Gateway handlers
    ├── excel_parser.py        # 📊 Excel processing
    ├── transformer.py         # 🔄 Data transformation
    ├── validator.py           # ✅ Data validation
    ├── cloud/                 # ☁️ AWS integrations
    │   └── dynamo_writer.py  # 💾 DynamoDB operations
    ├── business_rules/        # 💼 Business logic
    │   └── accounts_rules.py # Accounts-specific rules
    └── config/               # ⚙️ Configuration modules
        └── accounts_config.py # Accounts configuration
```

## 📊 Supported Data Types

### Accounts Business Line
- **Mobile Plans**: G-Zero, Puls, Premier plan rates
- **Transfers**: ACH, Transfiya, keys-based transfers  
- **Withdrawals**: ATM, branch, correspondent banking
- **Traditional Services**: Account maintenance, statements, certificates

### Features
- ✅ **Serverless Architecture**: Auto-scaling, pay-per-use
- ✅ **Event-Driven Processing**: S3 upload triggers ETL
- ✅ **Automatic Table Detection**: Multiple tables per Excel sheet
- ✅ **Business Rules Engine**: Configurable transformation rules
- ✅ **Data Validation**: Comprehensive quality checks
- ✅ **REST API**: Query processed data programmatically
- ✅ **Rate Parsing**: Complex rate structures (fixed, conditional, unlimited)
- ✅ **Colombian Number Format**: Handles $8.990 = 8990 correctly

## 📈 DynamoDB Data Model

**Partition Key (pk)**: `document#{document_id}`
**Sort Key (sk)**: `metadata | validation | service#{service_id}#{table_type}`

**GSI**: `business_line` + `service_id` for efficient querying

```json
{
  "pk": "document#123e4567-e89b-12d3-a456-426614174000",
  "sk": "service#app_opening#mobile_plans",
  "business_line": "accounts",
  "service_id": "app_opening",
  "description": "Planes abiertos desde app Davivienda",
  "rates": {
    "g_zero": {"type": "fixed", "value": 0},
    "puls": {"type": "fixed", "value": 8990},
    "premier": {"type": "fixed", "value": 15000}
  },
  "applies_tax": true,
  "frequency": "monthly"
}
```

## 🌐 API Endpoints

### Health Check
```
GET /health
```

### Documents
```
GET /api/documents                    # List all documents
GET /api/documents/{document_id}      # Get specific document
```

### Services
```
GET /api/services/{business_line}     # Get services by business line
?service_id=app_opening              # Filter by service ID
?table_type=mobile_plans             # Filter by table type
```

## 🔧 Configuration

**Global Settings** (`config.py`):
- File paths and processing parameters
- Validation rules and thresholds
- DynamoDB TTL settings

**Business Rules** (`src/config/accounts_config.py`):
- Plan type mappings
- Field normalizations  
- Service ID patterns
- Frequency mappings

## 🧪 Testing

### Local Testing
```bash
# Test ETL components locally
python scripts/test_local.py

# Run with pytest (future)
python -m pytest tests/
```

### Cloud Testing
```bash
# Upload test file
python scripts/upload_test_file.py bucket-name data/test.xlsx

# Monitor logs
serverless logs --function processRatesFile --tail

# Test APIs
curl https://your-api-url/health
```

## 📋 Monitoring & Operations

### CloudWatch Logs
- Lambda execution logs
- ETL processing metrics  
- Error tracking and alerts

### Metrics
- Processing duration
- Success/failure rates
- Data volume processed
- API response times

### Cost Optimization
- Pay-per-request DynamoDB
- Lambda pay-per-invocation
- S3 lifecycle policies
- CloudWatch log retention

## 🔮 Future Enhancements

- **Multi-region deployment**
- **Dead letter queues** for failed processing
- **Step Functions** for complex workflows
- **EventBridge** for event routing
- **Additional business lines** (credit, loans)
- **Real-time streaming** with Kinesis
- **Machine learning** for data quality
- **GraphQL API** with AppSync

## 🏷️ Version

**Current**: v2.0 (Serverless Architecture)
- ✅ AWS Lambda processing
- ✅ S3 event triggers  
- ✅ DynamoDB storage
- ✅ REST API access
- ✅ Infrastructure as Code
- ✅ Monitoring & logging

**Previous**: v1.0 (Local ETL)
- ✅ Local file processing
- ✅ JSON output format
- ✅ Validation framework
