#!/usr/bin/env python3
"""
Upload test file to S3 bucket to trigger the ETL pipeline.
"""

import boto3
import sys
from pathlib import Path

def upload_test_file(bucket_name: str, file_path: str):
    """Upload a test Excel file to S3 to trigger the ETL."""
    
    s3_client = boto3.client('s3')
    
    file_path = Path(file_path)
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)
    
    key = f"test-uploads/{file_path.name}"
    
    print(f"📤 Uploading {file_path} to s3://{bucket_name}/{key}")
    
    try:
        s3_client.upload_file(str(file_path), bucket_name, key)
        print(f"✅ Upload successful!")
        print(f"🔗 S3 URI: s3://{bucket_name}/{key}")
        print("⏳ Check CloudWatch logs for ETL processing status")
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python upload_test_file.py <bucket_name> <file_path>")
        print("Example: python upload_test_file.py pyetl-serverless-dev-files ../data/tasas-y-tarifas.xlsx")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    file_path = sys.argv[2]
    
    upload_test_file(bucket_name, file_path)
