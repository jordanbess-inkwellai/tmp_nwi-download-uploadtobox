"""
Box.com upload module with metadata support.
"""
import os
import shutil
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
import tempfile
import json

import boxsdk
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException

from config import BoxConfig


logger = logging.getLogger(__name__)


class BoxUploader:
    """Handles file uploads to Box.com with metadata support."""
    
    def __init__(self, config: BoxConfig):
        self.config = config
        self.client: Optional[Client] = None
        
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # No explicit disconnect needed for Box client
        pass
        
    def connect(self):
        """Initialize Box client."""
        try:
            oauth2 = OAuth2(
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                access_token=self.config.access_token,
            )
            self.client = Client(oauth2)
            
            # Test connection by getting user info
            user = self.client.user().get()
            logger.info("Connected to Box as user: %s", user.name)
            
        except Exception as e:
            logger.error("Failed to connect to Box: %s", e)
            raise
            
    def upload_file(self, file_path: str, folder_id: Optional[str] = None,
                   file_name: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Upload a file to Box."""
        if not self.client:
            raise RuntimeError("No active Box connection")
            
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.error("File not found: %s", file_path)
            return None
            
        folder_id = folder_id or self.config.folder_id
        file_name = file_name or file_path_obj.name
        
        try:
            # Get the folder
            folder = self.client.folder(folder_id)
            
            # Check if file already exists
            existing_file = self._find_existing_file(folder, file_name)
            
            if existing_file:
                # Update existing file
                logger.info("Updating existing file: %s", file_name)
                updated_file = existing_file.update_contents(file_path)
                box_file = updated_file
            else:
                # Upload new file
                logger.info("Uploading new file: %s", file_name)
                box_file = folder.upload(file_path, file_name)
                
            logger.info("File uploaded successfully: %s (ID: %s)", file_name, box_file.id)
            
            # Apply metadata if provided
            if metadata and self.config.metadata_template_key:
                self._apply_metadata(box_file, metadata)
                
            return box_file.id
            
        except BoxAPIException as e:
            logger.error("Box API error during upload: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error during upload: %s", e)
            return None
            
    def _find_existing_file(self, folder, file_name: str):
        """Check if file already exists in folder."""
        try:
            items = folder.get_items()
            for item in items:
                if item.name == file_name and item.type == 'file':
                    return item
        except Exception as e:
            logger.warning("Error checking for existing file: %s", e)
        return None
        
    def _apply_metadata(self, box_file, metadata: Dict[str, Any]):
        """Apply metadata to uploaded file."""
        try:
            # Check if metadata already exists
            existing_metadata = None
            try:
                existing_metadata = box_file.metadata(
                    scope='enterprise',
                    template=self.config.metadata_template_key
                ).get()
            except:
                pass
                
            if existing_metadata:
                # Update existing metadata
                box_file.metadata(
                    scope='enterprise',
                    template=self.config.metadata_template_key
                ).update(metadata)
                logger.info("Updated metadata for file: %s", box_file.name)
            else:
                # Create new metadata
                box_file.metadata(
                    scope='enterprise',
                    template=self.config.metadata_template_key
                ).create(metadata)
                logger.info("Created metadata for file: %s", box_file.name)
                
        except Exception as e:
            logger.error("Failed to apply metadata: %s", e)
            
    def upload_directory_as_zip(self, dir_path: str, folder_id: Optional[str] = None,
                              zip_name: Optional[str] = None,
                              metadata: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Compress directory and upload as zip file."""
        dir_path_obj = Path(dir_path)
        if not dir_path_obj.exists() or not dir_path_obj.is_dir():
            logger.error("Directory not found: %s", dir_path)
            return None
            
        zip_name = zip_name or f"{dir_path_obj.name}.zip"
        
        # Create temporary zip file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_zip_path = Path(temp_dir) / zip_name
            
            try:
                # Create zip archive
                logger.info("Creating zip archive: %s", zip_name)
                shutil.make_archive(
                    str(temp_zip_path.with_suffix('')),
                    'zip',
                    str(dir_path_obj)
                )
                
                # Upload the zip file
                return self.upload_file(
                    str(temp_zip_path),
                    folder_id=folder_id,
                    file_name=zip_name,
                    metadata=metadata
                )
                
            except Exception as e:
                logger.error("Failed to create/upload zip: %s", e)
                return None
                
    def upload_multiple(self, file_paths: Dict[str, str], 
                       metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[str]]:
        """Upload multiple files."""
        results = {}
        
        for format_type, file_path in file_paths.items():
            if file_path and Path(file_path).exists():
                # Handle FileGDB as directory
                if format_type == 'filegdb' and Path(file_path).is_dir():
                    logger.info("Uploading FileGDB as zip: %s", file_path)
                    file_id = self.upload_directory_as_zip(
                        file_path,
                        metadata=metadata
                    )
                else:
                    file_id = self.upload_file(
                        file_path,
                        metadata=metadata
                    )
                results[format_type] = file_id
            else:
                results[format_type] = None
                
        return results
        
    def create_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> Optional[str]:
        """Create a new folder in Box."""
        if not self.client:
            raise RuntimeError("No active Box connection")
            
        parent_folder_id = parent_folder_id or self.config.folder_id
        
        try:
            parent_folder = self.client.folder(parent_folder_id)
            
            # Check if folder already exists
            existing_folder = self._find_existing_folder(parent_folder, folder_name)
            if existing_folder:
                logger.info("Folder already exists: %s (ID: %s)", folder_name, existing_folder.id)
                return existing_folder.id
                
            # Create new folder
            new_folder = parent_folder.create_subfolder(folder_name)
            logger.info("Created folder: %s (ID: %s)", folder_name, new_folder.id)
            return new_folder.id
            
        except Exception as e:
            logger.error("Failed to create folder: %s", e)
            return None
            
    def _find_existing_folder(self, parent_folder, folder_name: str):
        """Check if folder already exists."""
        try:
            items = parent_folder.get_items()
            for item in items:
                if item.name == folder_name and item.type == 'folder':
                    return item
        except Exception as e:
            logger.warning("Error checking for existing folder: %s", e)
        return None