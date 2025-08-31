"""AWS Lambda handlers using hexagonal architecture."""

import json
import os
import logging
from typing import Dict, Any
from urllib.parse import unquote_plus

from ..application import ProcessDocumentUseCase, QueryDocumentsUseCase
from ..infrastructure.adapters import DynamoDBDocumentAdapter, S3FileAdapter, OpenpyxlExcelAdapter
from ..domain.services import ExcelProcessorService, DataValidatorService

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
S3_BUCKET = os.environ.get('S3_BUCKET')
STAGE = os.environ.get('STAGE', 'dev')


def _get_dependencies():
    """Initialize dependencies for dependency injection."""
    # Infrastructure adapters
    document_repository = DynamoDBDocumentAdapter(DYNAMODB_TABLE)
    file_repository = S3FileAdapter()
    
    # Domain services
    excel_processor = ExcelProcessorService()
    data_validator = DataValidatorService()
    
    # Use cases
    process_document_use_case = ProcessDocumentUseCase(
        document_repository=document_repository,
        file_repository=file_repository,
        excel_processor=excel_processor,
        data_validator=data_validator
    )
    
    query_documents_use_case = QueryDocumentsUseCase(
        document_repository=document_repository
    )
    
    return process_document_use_case, query_documents_use_case


async def process_rates_file(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for processing Excel files from S3.
    
    Uses clean architecture with dependency injection.
    """
    try:
        logger.info(f"Processing S3 event: {json.dumps(event, default=str)}")
        
        # Initialize dependencies
        process_document_use_case, _ = _get_dependencies()
        
        # Process each S3 record
        results = []
        
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            
            logger.info(f"Processing file: s3://{bucket}/{key}")
            
            try:
                # Execute use case
                document = await process_document_use_case.execute(bucket, key)
                
                result = {
                    'status': 'success',
                    'document_id': document.document_id,
                    'filename': document.filename,
                    'services_count': document.get_service_count(),
                    'services_by_type': document.get_service_count_by_table_type()
                }
                
                logger.info(f"✅ Successfully processed {key}: {document.get_service_count()} services")
                
            except Exception as e:
                logger.error(f"❌ Failed to process {key}: {str(e)}")
                result = {
                    'status': 'error',
                    'filename': key,
                    'error': str(e)
                }
            
            results.append(result)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Processed {len(results)} files',
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Handler error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


async def get_documents(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """API handler to list documents."""
    try:
        _, query_use_case = _get_dependencies()
        
        # Get query parameters
        query_params = event.get('queryStringParameters') or {}
        business_line = query_params.get('business_line')
        
        documents = await query_use_case.list_documents(business_line)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'documents': [
                    {
                        'document_id': doc.document_id,
                        'filename': doc.filename,
                        'business_line': doc.business_line,
                        'created_at': doc.created_at.isoformat() if doc.created_at else None,
                        'services_count': doc.get_service_count()
                    }
                    for doc in documents
                ]
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error getting documents: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }


async def get_document_by_id(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """API handler to get a specific document."""
    try:
        _, query_use_case = _get_dependencies()
        
        document_id = event['pathParameters']['id']
        document = await query_use_case.get_document(document_id)
        
        if not document:
            return {
                'statusCode': 404,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Document not found'})
            }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(document.to_dict(), default=str)
        }
        
    except Exception as e:
        logger.error(f"Error getting document: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }


async def get_services_by_business_line(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """API handler to get services by business line."""
    try:
        _, query_use_case = _get_dependencies()
        
        business_line = event['pathParameters']['business_line']
        services = await query_use_case.get_services_by_business_line(business_line)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'business_line': business_line,
                'services': [service.to_dict() for service in services]
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error getting services: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
