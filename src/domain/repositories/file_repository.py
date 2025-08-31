"""File repository interface."""

from abc import ABC, abstractmethod
from typing import BinaryIO, Optional


class FileRepository(ABC):
    """
    Repository interface for file operations.
    
    Defines the contract for reading and writing files
    from various storage systems (S3, local filesystem, etc.).
    """
    
    @abstractmethod
    async def read_file(self, bucket: str, key: str) -> BinaryIO:
        """
        Read a file from storage.
        
        Args:
            bucket: The storage bucket/container
            key: The file key/path
            
        Returns:
            File content as binary stream
        """
        pass
    
    @abstractmethod
    async def write_file(self, bucket: str, key: str, content: bytes) -> None:
        """
        Write a file to storage.
        
        Args:
            bucket: The storage bucket/container
            key: The file key/path
            content: File content as bytes
        """
        pass
    
    @abstractmethod
    async def file_exists(self, bucket: str, key: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            bucket: The storage bucket/container
            key: The file key/path
            
        Returns:
            True if file exists, False otherwise
        """
        pass
    
    @abstractmethod
    async def delete_file(self, bucket: str, key: str) -> bool:
        """
        Delete a file from storage.
        
        Args:
            bucket: The storage bucket/container
            key: The file key/path
            
        Returns:
            True if file was deleted, False if not found
        """
        pass
    
    @abstractmethod
    async def get_file_metadata(self, bucket: str, key: str) -> Optional[dict]:
        """
        Get file metadata.
        
        Args:
            bucket: The storage bucket/container
            key: The file key/path
            
        Returns:
            File metadata dictionary or None if not found
        """
        pass
