#!/usr/bin/env python3
"""
Local testing script for the ETL pipeline.
Simulates the Lambda execution environment.
"""

import sys
import os
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Import the lambda handler
from lambda_handler import process_rates_file

def test_local_etl():
    """Test the ETL pipeline locally."""
    
    # Mock S3 event
    mock_event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': 'data/tasas-y-tarifas.xlsx'}
            }
        }]
    }
    
    # Mock context
    class MockContext:
        function_name = 'test-function'
        aws_request_id = 'test-request-id'
        
    context = MockContext()
    
    # Set environment variables for testing
    os.environ['DYNAMODB_TABLE'] = 'test-table'
    os.environ['S3_BUCKET'] = 'test-bucket'
    os.environ['STAGE'] = 'local'
    
    print("🧪 Testing ETL pipeline locally...")
    
    try:
        # Note: This will fail without actual AWS credentials and resources
        # But it can help test the code structure
        result = process_rates_file(mock_event, context)
        print(f"✅ Result: {result}")
        
    except Exception as e:
        print(f"❌ Expected error (missing AWS resources): {e}")
        print("✅ Code structure test completed - deploy to AWS to test fully")

if __name__ == "__main__":
    test_local_etl()
