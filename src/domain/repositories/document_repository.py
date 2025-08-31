"""Document repository interface."""

from abc import ABC, abstractmethod
from typing import List, Optional
from ..entities import Document, FinancialService


class DocumentRepository(ABC):
    """
    Repository interface for document persistence.
    
    Defines the contract for storing and retrieving documents
    and their associated financial services.
    """
    
    @abstractmethod
    async def save_document(self, document: Document) -> None:
        """
        Save a document with all its services.
        
        Args:
            document: The document to save
        """
        pass
    
    @abstractmethod
    async def get_document(self, document_id: str) -> Optional[Document]:
        """
        Retrieve a document by its ID.
        
        Args:
            document_id: The document identifier
            
        Returns:
            The document if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def list_documents(self, business_line: Optional[str] = None) -> List[Document]:
        """
        List all documents, optionally filtered by business line.
        
        Args:
            business_line: Optional business line filter
            
        Returns:
            List of documents
        """
        pass
    
    @abstractmethod
    async def get_services_by_business_line(self, business_line: str) -> List[FinancialService]:
        """
        Get all services for a specific business line.
        
        Args:
            business_line: The business line to filter by
            
        Returns:
            List of financial services
        """
        pass
    
    @abstractmethod
    async def delete_document(self, document_id: str) -> bool:
        """
        Delete a document and all its services.
        
        Args:
            document_id: The document identifier
            
        Returns:
            True if document was deleted, False if not found
        """
        pass
