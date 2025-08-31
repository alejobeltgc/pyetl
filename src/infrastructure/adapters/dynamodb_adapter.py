"""DynamoDB adapter for document persistence."""

import boto3
from boto3.dynamodb.conditions import Key, Attr
from typing import List, Optional, Dict, Any
from decimal import Decimal
import json
from datetime import datetime

from ...domain.repositories import DocumentRepository
from ...domain.entities import Document, FinancialService


class DynamoDBDocumentAdapter(DocumentRepository):
    """
    DynamoDB implementation of the DocumentRepository.
    
    Handles persistence of documents and services using DynamoDB
    with a single-table design pattern.
    """
    
    def __init__(self, table_name: str, region_name: str = 'us-east-1'):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)
    
    async def save_document(self, document: Document) -> None:
        """Save document and all its services to DynamoDB."""
        # Prepare items for batch write
        items_to_write = []
        
        # Document metadata item
        document_item = {
            'pk': f'document#{document.document_id}',
            'sk': 'metadata',
            'document_id': document.document_id,
            'business_line': document.business_line,
            'filename': document.filename,
            'document_version': document.version,
            'created_at': document.created_at.isoformat() if document.created_at else None,
            'last_updated': document.last_updated.isoformat() if document.last_updated else None,
            'processing_metadata': self._convert_to_dynamo_format(document.processing_metadata),
            'total_services': document.get_service_count(),
            'services_by_type': self._convert_to_dynamo_format(document.get_service_count_by_table_type())
        }
        items_to_write.append(document_item)
        
        # Service items
        for service in document.services:
            service_item = self._service_to_dynamo_item(service)
            items_to_write.append(service_item)
        
        # Batch write items
        await self._batch_write_items(items_to_write)
    
    async def get_document(self, document_id: str) -> Optional[Document]:
        """Retrieve document with all its services."""
        try:
            # Query all items for this document
            response = self.table.query(
                KeyConditionExpression=Key('pk').eq(f'document#{document_id}')
            )
            
            if not response['Items']:
                return None
            
            # Separate metadata and services
            metadata_item = None
            service_items = []
            
            for item in response['Items']:
                if item['sk'] == 'metadata':
                    metadata_item = item
                else:
                    service_items.append(item)
            
            if not metadata_item:
                return None
            
            # Reconstruct document
            document = Document(
                document_id=metadata_item['document_id'],
                business_line=metadata_item['business_line'],
                filename=metadata_item['filename'],
                processing_metadata=self._convert_from_dynamo_format(
                    metadata_item.get('processing_metadata', {})
                ),
                version=metadata_item.get('document_version', 'v1')
            )
            
            if metadata_item.get('created_at'):
                document.created_at = datetime.fromisoformat(metadata_item['created_at'])
            if metadata_item.get('last_updated'):
                document.last_updated = datetime.fromisoformat(metadata_item['last_updated'])
            
            # Add services
            for service_item in service_items:
                service = self._dynamo_item_to_service(service_item)
                document.services.append(service)
            
            return document
            
        except Exception as e:
            raise RepositoryError(f"Failed to get document {document_id}: {str(e)}") from e
    
    async def list_documents(self, business_line: Optional[str] = None) -> List[Document]:
        """List all documents, optionally filtered by business line."""
        try:
            # Scan for metadata items
            filter_expression = Attr('sk').eq('metadata')
            if business_line:
                filter_expression = filter_expression & Attr('business_line').eq(business_line)
            
            response = self.table.scan(FilterExpression=filter_expression)
            
            documents = []
            for item in response['Items']:
                # Create document without services (for listing)
                document = Document(
                    document_id=item['document_id'],
                    business_line=item['business_line'],
                    filename=item['filename'],
                    processing_metadata=self._convert_from_dynamo_format(
                        item.get('processing_metadata', {})
                    ),
                    version=item.get('document_version', 'v1')
                )
                
                if item.get('created_at'):
                    document.created_at = datetime.fromisoformat(item['created_at'])
                if item.get('last_updated'):
                    document.last_updated = datetime.fromisoformat(item['last_updated'])
                
                documents.append(document)
            
            return documents
            
        except Exception as e:
            raise RepositoryError(f"Failed to list documents: {str(e)}") from e
    
    async def get_services_by_business_line(self, business_line: str) -> List[FinancialService]:
        """Get all services for a specific business line using GSI."""
        try:
            response = self.table.query(
                IndexName='business-line-index',
                KeyConditionExpression=Key('business_line').eq(business_line)
            )
            
            services = []
            for item in response['Items']:
                if item['sk'].startswith('service#'):
                    service = self._dynamo_item_to_service(item)
                    services.append(service)
            
            return services
            
        except Exception as e:
            raise RepositoryError(f"Failed to get services for business line {business_line}: {str(e)}") from e
    
    async def delete_document(self, document_id: str) -> bool:
        """Delete document and all its services."""
        try:
            # Get all items for this document
            response = self.table.query(
                KeyConditionExpression=Key('pk').eq(f'document#{document_id}')
            )
            
            if not response['Items']:
                return False
            
            # Delete all items
            with self.table.batch_writer() as batch:
                for item in response['Items']:
                    batch.delete_item(
                        Key={'pk': item['pk'], 'sk': item['sk']}
                    )
            
            return True
            
        except Exception as e:
            raise RepositoryError(f"Failed to delete document {document_id}: {str(e)}") from e
    
    def _service_to_dynamo_item(self, service: FinancialService) -> Dict[str, Any]:
        """Convert service to DynamoDB item format."""
        return {
            'pk': f'document#{service.document_id}',
            'sk': f'service#{service.service_id}#{service.table_type}',
            'business_line': service.business_line,
            'last_updated': service.created_at.isoformat() if service.created_at else None,
            'service_id': service.service_id,
            'description': service.description,
            'table_type': service.table_type,
            'document_id': service.document_id,
            'rates': self._convert_to_dynamo_format({
                plan: rate.to_dict() for plan, rate in service.rates.items()
            }),
            'source_position': self._convert_to_dynamo_format(service.source_position),
            'created_at': service.created_at.isoformat() if service.created_at else None
        }
    
    def _dynamo_item_to_service(self, item: Dict[str, Any]) -> FinancialService:
        """Convert DynamoDB item to service entity."""
        service = FinancialService(
            service_id=item['service_id'],
            description=item['description'],
            business_line=item['business_line'],
            table_type=item['table_type'],
            document_id=item['document_id'],
            source_position=self._convert_from_dynamo_format(item.get('source_position', {}))
        )
        
        if item.get('created_at'):
            service.created_at = datetime.fromisoformat(item['created_at'])
        
        # Reconstruct rates
        rates_data = self._convert_from_dynamo_format(item.get('rates', {}))
        for plan, rate_data in rates_data.items():
            from ...domain.entities import Rate, RateType
            rate_type = RateType(rate_data['type'])
            
            if rate_type == RateType.CONDITIONAL:
                rate = Rate.conditional(
                    rate_data['included_free'],
                    Decimal(str(rate_data['additional_cost'])),
                    rate_data.get('currency')
                )
            elif rate_type == RateType.PERCENTAGE:
                rate = Rate.percentage(Decimal(str(rate_data['value'])))
            elif rate_type == RateType.UNLIMITED:
                rate = Rate.unlimited()
            else:  # FIXED
                rate = Rate.fixed(
                    Decimal(str(rate_data['value'])), 
                    rate_data.get('currency')
                )
            
            service.add_rate(plan, rate)
        
        return service
    
    def _convert_to_dynamo_format(self, data: Any) -> Any:
        """Convert data to DynamoDB-compatible format."""
        if isinstance(data, dict):
            return {k: self._convert_to_dynamo_format(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_to_dynamo_format(item) for item in data]
        elif isinstance(data, Decimal):
            return float(data)
        elif isinstance(data, datetime):
            return data.isoformat()
        else:
            return data
    
    def _convert_from_dynamo_format(self, data: Any) -> Any:
        """Convert data from DynamoDB format."""
        if isinstance(data, dict):
            return {k: self._convert_from_dynamo_format(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_from_dynamo_format(item) for item in data]
        else:
            return data
    
    async def _batch_write_items(self, items: List[Dict[str, Any]]) -> None:
        """Write items in batches to DynamoDB."""
        # DynamoDB batch write limit is 25 items
        batch_size = 25
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            with self.table.batch_writer() as writer:
                for item in batch:
                    writer.put_item(Item=item)


class RepositoryError(Exception):
    """Raised when repository operations fail."""
    pass
