#!/usr/bin/env python3
"""
Simple script to test file upload to S3 bucket.
"""

import boto3
import sys
import os


def upload_test_file():
    """Upload a test Excel file to trigger processing."""
    
    bucket_name = "pyetl-rates-input-demo-2025"
    local_file = "data/tasas-y-tarifas.xlsx"
    s3_key = "test-upload.xlsx"
    
    if not os.path.exists(local_file):
        print(f"❌ File not found: {local_file}")
        sys.exit(1)
    
    print(f"📤 Uploading {local_file} to s3://{bucket_name}/{s3_key}")
    
    try:
        s3_client = boto3.client('s3')
        
        s3_client.upload_file(local_file, bucket_name, s3_key)
        
        print(f"✅ File uploaded successfully!")
        print(f"🔗 S3 URL: s3://{bucket_name}/{s3_key}")
        print(f"⚡ This should trigger Lambda processing automatically")
        
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    upload_test_file()
