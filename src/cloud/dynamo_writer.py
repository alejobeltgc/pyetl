"""
DynamoDB writer for storing transformed data and validation reports.
"""

import boto3
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from decimal import Decimal


class DynamoDBWriter:
    """
    Handles writing ETL results to DynamoDB with proper data modeling.
    
    Data Model:
    - pk: document#{document_id}
    - sk: metadata | service#{service_id} | validation
    - business_line: for GSI queries
    - service_id: for GSI queries
    """
    
    def __init__(self, table_name: str):
        """
        Initialize DynamoDB writer.
        
        Args:
            table_name (str): DynamoDB table name
        """
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
    
    def store_document(self, transformed_data: Dict[str, Any], 
                      validation_report: Dict[str, Any],
                      file_metadata: Dict[str, Any]) -> str:
        """
        Store complete ETL results in DynamoDB.
        
        Args:
            transformed_data: The transformed business data
            validation_report: Validation results
            file_metadata: File processing metadata
            
        Returns:
            str: Document ID for the stored data
        """
        document_id = str(uuid.uuid4())
        
        # Store document metadata
        self._store_metadata(document_id, transformed_data, file_metadata)
        
        # Store validation report
        self._store_validation_report(document_id, validation_report)
        
        # Store services data
        self._store_services(document_id, transformed_data)
        
        return document_id
    
    def _store_metadata(self, document_id: str, transformed_data: Dict[str, Any], 
                       file_metadata: Dict[str, Any]) -> None:
        """Store document metadata."""
        metadata_item = {
            'pk': f"document#{document_id}",
            'sk': 'metadata',
            'document_id': document_id,
            'business_line': transformed_data.get('business_line') or 'UNKNOWN',
            'document_type': transformed_data.get('document_type'),
            'document_version': transformed_data.get('document_version'),
            'last_updated': file_metadata.get('processed_at') or datetime.now(timezone.utc).isoformat(),
            'source_sheets': transformed_data.get('source_sheets', []),
            'source_file': file_metadata.get('source_file'),
            'bucket': file_metadata.get('bucket'),
            'processed_at': file_metadata.get('processed_at'),
            'stage': file_metadata.get('stage'),
            'validation_status': file_metadata.get('validation_status'),
            'services_count': file_metadata.get('services_count'),
            'tables_count': file_metadata.get('tables_count'),
            'ttl': self._calculate_ttl(),  # Optional: expire old documents
            'created_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Convert floats to Decimal for DynamoDB
        metadata_item = self._convert_floats_to_decimal(metadata_item)
        
        self.table.put_item(Item=metadata_item)
    
    def _store_validation_report(self, document_id: str, 
                                validation_report: Dict[str, Any]) -> None:
        """Store validation report."""
        validation_item = {
            'pk': f"document#{document_id}",
            'sk': 'validation',
            'document_id': document_id,
            'validation_status': validation_report.get('status'),
            'total_errors': validation_report.get('summary', {}).get('total_errors', 0),
            'total_warnings': validation_report.get('summary', {}).get('total_warnings', 0),
            'errors': validation_report.get('errors', []),
            'warnings': validation_report.get('warnings', []),
            'created_at': datetime.now(timezone.utc).isoformat()
        }
        
        validation_item = self._convert_floats_to_decimal(validation_item)
        
        self.table.put_item(Item=validation_item)
    
    def _store_services(self, document_id: str, transformed_data: Dict[str, Any]) -> None:
        """Store individual services from all tables."""
        business_line = transformed_data.get('business_line')
        
        for table_type, table_data in transformed_data.get('tables', {}).items():
            for service in table_data.get('services', []):
                service_item = {
                    'pk': f"document#{document_id}",
                    'sk': f"service#{service.get('service_id')}#{table_type}",
                    'document_id': document_id,
                    'business_line': business_line,
                    'service_id': service.get('service_id'),
                    'table_type': table_type,
                    'description': service.get('description'),
                    'applies_tax': service.get('applies_tax'),
                    'frequency': service.get('frequency'),
                    'category': service.get('category'),
                    'rates': service.get('rates'),
                    'rate': service.get('rate'),
                    'disclaimer': service.get('disclaimer'),
                    'source_position': table_data.get('source_position', {}),
                    'created_at': datetime.now(timezone.utc).isoformat()
                }
                
                # Remove None values
                service_item = {k: v for k, v in service_item.items() if v is not None}
                
                # Convert floats to Decimal
                service_item = self._convert_floats_to_decimal(service_item)
                
                self.table.put_item(Item=service_item)
    
    def _convert_floats_to_decimal(self, obj: Any) -> Any:
        """
        Recursively convert float values to Decimal for DynamoDB compatibility.
        """
        if isinstance(obj, dict):
            return {k: self._convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_floats_to_decimal(item) for item in obj]
        elif isinstance(obj, float):
            return Decimal(str(obj))
        else:
            return obj
    
    def _calculate_ttl(self, days: int = 365) -> int:
        """
        Calculate TTL (Time To Live) for automatic cleanup.
        
        Args:
            days: Number of days until expiration
            
        Returns:
            int: Unix timestamp for TTL
        """
        from datetime import timedelta
        expiry_date = datetime.now(timezone.utc) + timedelta(days=days)
        return int(expiry_date.timestamp())
    
    def get_document(self, document_id: str) -> Dict[str, Any]:
        """
        Retrieve a complete document by ID.
        
        Args:
            document_id: Document identifier
            
        Returns:
            Dict containing metadata, validation, and services
        """
        response = self.table.query(
            KeyConditionExpression='pk = :pk',
            ExpressionAttributeValues={
                ':pk': f"document#{document_id}"
            }
        )
        
        items = response.get('Items', [])
        
        result = {
            'metadata': None,
            'validation': None,
            'services': []
        }
        
        for item in items:
            sk = item.get('sk', '')
            if sk == 'metadata':
                result['metadata'] = item
            elif sk == 'validation':
                result['validation'] = item
            elif sk.startswith('service#'):
                result['services'].append(item)
        
        return result
    
    def list_documents(self, business_line: Optional[str] = None, 
                      limit: int = 10) -> Dict[str, Any]:
        """
        List documents with optional filtering by business line.
        
        Args:
            business_line: Optional filter by business line
            limit: Maximum number of documents to return
            
        Returns:
            Dict containing documents list and pagination info
        """
        if business_line:
            # Use GSI to filter by business line
            response = self.table.query(
                IndexName='business-line-index',
                KeyConditionExpression='business_line = :bl',
                FilterExpression='sk = :sk',
                ExpressionAttributeValues={
                    ':bl': business_line,
                    ':sk': 'metadata'
                },
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
        else:
            # Scan for all metadata items
            response = self.table.scan(
                FilterExpression='sk = :sk',
                ExpressionAttributeValues={
                    ':sk': 'metadata'
                },
                Limit=limit
            )
        
        return {
            'documents': response.get('Items', []),
            'count': len(response.get('Items', [])),
            'last_evaluated_key': response.get('LastEvaluatedKey')
        }
