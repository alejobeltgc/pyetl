"""
API handlers for querying processed data from DynamoDB.
"""

import json
import os
import sys
import logging
from typing import Dict, Any
from decimal import Decimal

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cloud.dynamo_writer import DynamoDBWriter

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')

# Constants
CONTENT_TYPE_JSON = 'application/json'
INTERNAL_SERVER_ERROR = 'Internal server error'


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder for Decimal objects."""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def get_document(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Get a specific document by ID.
    
    Path: GET /documents/{document_id}
    """
    try:
        document_id = event['pathParameters']['document_id']
        
        dynamo_writer = DynamoDBWriter(DYNAMODB_TABLE)
        document = dynamo_writer.get_document(document_id)
        
        if not document.get('metadata'):
            return {
                'statusCode': 404,
                'headers': {'Content-Type': CONTENT_TYPE_JSON},
                'body': json.dumps({'error': 'Document not found'})
            }
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': CONTENT_TYPE_JSON},
            'body': json.dumps(document, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving document: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': CONTENT_TYPE_JSON},
            'body': json.dumps({'error': INTERNAL_SERVER_ERROR})
        }


def list_documents(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    List documents with optional filtering.
    
    Path: GET /documents
    Query parameters:
    - business_line: Filter by business line
    - limit: Maximum number of results (default: 10)
    """
    try:
        query_params = event.get('queryStringParameters') or {}
        business_line = query_params.get('business_line')
        limit = int(query_params.get('limit', 10))
        
        dynamo_writer = DynamoDBWriter(DYNAMODB_TABLE)
        result = dynamo_writer.list_documents(business_line, limit)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': CONTENT_TYPE_JSON},
            'body': json.dumps(result, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Error listing documents: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': CONTENT_TYPE_JSON},
            'body': json.dumps({'error': INTERNAL_SERVER_ERROR})
        }


def get_services_by_business_line(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Get services filtered by business line and service type.
    
    Path: GET /services/{business_line}
    Query parameters:
    - service_id: Filter by service ID
    - table_type: Filter by table type (mobile_plans, traditional_services, etc.)
    """
    try:
        business_line = event['pathParameters']['business_line']
        query_params = event.get('queryStringParameters') or {}
        
        dynamo_writer = DynamoDBWriter(DYNAMODB_TABLE)
        
        # Query using GSI for business line
        response = dynamo_writer.table.query(
            IndexName='business-line-index',
            KeyConditionExpression='business_line = :bl',
            FilterExpression='begins_with(sk, :prefix)',
            ExpressionAttributeValues={
                ':bl': business_line,
                ':prefix': 'service#'
            }
        )
        
        services = response.get('Items', [])
        
        # Apply additional filters
        service_id_filter = query_params.get('service_id')
        table_type_filter = query_params.get('table_type')
        
        if service_id_filter:
            services = [s for s in services if s.get('service_id') == service_id_filter]
        
        if table_type_filter:
            services = [s for s in services if s.get('table_type') == table_type_filter]
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': CONTENT_TYPE_JSON},
            'body': json.dumps({
                'business_line': business_line,
                'services': services,
                'count': len(services)
            }, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving services: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': CONTENT_TYPE_JSON},
            'body': json.dumps({'error': INTERNAL_SERVER_ERROR})
        }
