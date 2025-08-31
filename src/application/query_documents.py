"""Use case for querying documents and services."""

from typing import List, Optional
from ..domain.entities import Document, FinancialService
from ..domain.repositories import DocumentRepository


class QueryDocumentsUseCase:
    """
    Use case for querying documents and services.
    
    Handles all read operations for documents and financial services,
    including filtering and searching capabilities.
    """
    
    def __init__(self, document_repository: DocumentRepository):
        self.document_repository = document_repository
    
    async def get_document(self, document_id: str) -> Optional[Document]:
        """
        Get a specific document by ID.
        
        Args:
            document_id: Document identifier
            
        Returns:
            Document if found, None otherwise
        """
        return await self.document_repository.get_document(document_id)
    
    async def list_documents(self, business_line: Optional[str] = None) -> List[Document]:
        """
        List all documents, optionally filtered by business line.
        
        Args:
            business_line: Optional business line filter
            
        Returns:
            List of documents
        """
        return await self.document_repository.list_documents(business_line)
    
    async def get_services_by_business_line(self, business_line: str) -> List[FinancialService]:
        """
        Get all services for a specific business line.
        
        Args:
            business_line: Business line to filter by
            
        Returns:
            List of financial services
        """
        return await self.document_repository.get_services_by_business_line(business_line)
    
    async def get_document_summary(self, document_id: str) -> Optional[dict]:
        """
        Get document summary with metadata and statistics.
        
        Args:
            document_id: Document identifier
            
        Returns:
            Document summary dictionary or None if not found
        """
        document = await self.document_repository.get_document(document_id)
        if not document:
            return None
        
        return {
            "document_id": document.document_id,
            "filename": document.filename,
            "business_line": document.business_line,
            "created_at": document.created_at.isoformat() if document.created_at else None,
            "last_updated": document.last_updated.isoformat() if document.last_updated else None,
            "total_services": document.get_service_count(),
            "services_by_type": document.get_service_count_by_table_type(),
            "processing_metadata": document.processing_metadata
        }
    
    async def search_services(self, query: str, business_line: Optional[str] = None) -> List[FinancialService]:
        """
        Search services by description or other criteria.
        
        Args:
            query: Search query string
            business_line: Optional business line filter
            
        Returns:
            List of matching services
        """
        # Get all services (filtered by business line if specified)
        if business_line:
            services = await self.document_repository.get_services_by_business_line(business_line)
        else:
            documents = await self.document_repository.list_documents()
            services = []
            for document in documents:
                services.extend(document.services)
        
        # Filter by query
        query_lower = query.lower()
        matching_services = []
        
        for service in services:
            if (query_lower in service.description.lower() or 
                query_lower in service.service_id.lower() or
                query_lower in service.table_type.lower()):
                matching_services.append(service)
        
        return matching_services
