"""Use case for processing documents."""

import uuid
from typing import BinaryIO
from ..domain.entities import Document
from ..domain.repositories import DocumentRepository, FileRepository
from ..domain.services import ExcelProcessorService, DataValidatorService
from ..infrastructure.adapters import OpenpyxlExcelAdapter


class ProcessDocumentUseCase:
    """
    Use case for processing Excel documents.
    
    Orchestrates the complete document processing workflow:
    1. Read file from storage
    2. Process Excel content
    3. Validate extracted data
    4. Store document and services
    """
    
    def __init__(
        self,
        document_repository: DocumentRepository,
        file_repository: FileRepository,
        excel_processor: ExcelProcessorService,
        data_validator: DataValidatorService
    ):
        self.document_repository = document_repository
        self.file_repository = file_repository
        self.excel_processor = excel_processor
        self.data_validator = data_validator
        self.excel_adapter = OpenpyxlExcelAdapter()
    
    async def execute(self, bucket: str, file_key: str) -> Document:
        """
        Execute document processing workflow.
        
        Args:
            bucket: Storage bucket name
            file_key: File key/path in storage
            
        Returns:
            Processed document
            
        Raises:
            ProcessingError: If processing fails
        """
        try:
            # Generate unique document ID
            document_id = str(uuid.uuid4())
            
            # Read file from storage
            file_content = await self.file_repository.read_file(bucket, file_key)
            
            # Extract filename from key
            filename = file_key.split('/')[-1]
            
            # Process Excel content using adapter
            document = await self.excel_adapter.process_excel_file(
                file_content, filename, document_id
            )
            
            # Validate processed data
            validation_report = self.data_validator.validate_document(document)
            
            # Store validation metadata
            document.processing_metadata['validation_report'] = validation_report.to_dict()
            
            # Check for critical errors
            if validation_report.has_errors():
                critical_issues = self.data_validator.get_critical_issues(validation_report)
                if critical_issues:
                    raise ProcessingError(f"Critical validation errors: {critical_issues}")
            
            # Store document
            await self.document_repository.save_document(document)
            
            return document
            
        except Exception as e:
            raise ProcessingError(f"Failed to process document: {str(e)}") from e


class ProcessingError(Exception):
    """Raised when document processing fails."""
    pass
