"""S3 adapter for file operations."""

import boto3
from botocore.exceptions import ClientError
from typing import BinaryIO, Optional
import io

from ...domain.repositories import FileRepository


class S3FileAdapter(FileRepository):
    """
    S3 implementation of the FileRepository.
    
    Handles file operations using Amazon S3.
    """
    
    def __init__(self, region_name: str = 'us-east-1'):
        self.s3_client = boto3.client('s3', region_name=region_name)
    
    async def read_file(self, bucket: str, key: str) -> BinaryIO:
        """Read file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            return io.BytesIO(response['Body'].read())
        except ClientError as e:
            raise FileRepositoryError(f"Failed to read file s3://{bucket}/{key}: {str(e)}") from e
    
    async def write_file(self, bucket: str, key: str, content: bytes) -> None:
        """Write file to S3."""
        try:
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=content)
        except ClientError as e:
            raise FileRepositoryError(f"Failed to write file s3://{bucket}/{key}: {str(e)}") from e
    
    async def file_exists(self, bucket: str, key: str) -> bool:
        """Check if file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise FileRepositoryError(f"Failed to check file s3://{bucket}/{key}: {str(e)}") from e
    
    async def delete_file(self, bucket: str, key: str) -> bool:
        """Delete file from S3."""
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise FileRepositoryError(f"Failed to delete file s3://{bucket}/{key}: {str(e)}") from e
    
    async def get_file_metadata(self, bucket: str, key: str) -> Optional[dict]:
        """Get file metadata from S3."""
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'size': response.get('ContentLength'),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {})
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            raise FileRepositoryError(f"Failed to get metadata for s3://{bucket}/{key}: {str(e)}") from e


class FileRepositoryError(Exception):
    """Raised when file repository operations fail."""
    pass
