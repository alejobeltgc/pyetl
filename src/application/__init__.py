"""Application use cases."""

from .process_document import ProcessDocumentUseCase
from .query_documents import QueryDocumentsUseCase

__all__ = [
    'ProcessDocumentUseCase',
    'QueryDocumentsUseCase'
]
