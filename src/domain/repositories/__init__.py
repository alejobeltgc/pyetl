"""Repository interfaces for the domain layer."""

from .document_repository import DocumentRepository
from .file_repository import FileRepository

__all__ = [
    'DocumentRepository',
    'FileRepository'
]
