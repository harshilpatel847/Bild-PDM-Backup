#!/usr/bin/env python3
"""
Bild PDM API Complete Backup Tool

This script connects to the Bild PDM system via their API to:
1. Get all projects the user has access to
2. Iterate through each project to get all files
3. Download all files preserving the original folder structure
4. Create a complete backup of the Bild account

Requirements: pip install requests
"""

import requests
import json
import sys
import os
from pathlib import Path
from typing import List, Dict, Any
import time
from urllib.parse import urlparse
import re

# Script version information
__version__ = "1.0.0"
__author__ = "Harshil Patel, Scrub Daddy, Inc."
__description__ = "Complete backup solution for Bild PDM accounts"

class BildAPIClient:
    def __init__(self, api_token: str, backup_location: str, base_url: str = "https://sandbox-api.getbild.com"):
        """
        Initialize the Bild API client
        
        Args:
            api_token: Your Bild API token (Bearer token)
            backup_location: Local directory path where files will be downloaded
            base_url: Base URL for the API (default: sandbox)
        """
        self.base_url = base_url.rstrip('/')
        self.backup_location = Path(backup_location).resolve()
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Create backup directory if it doesn't exist
        self.backup_location.mkdir(parents=True, exist_ok=True)
        
        # Statistics tracking
        self.stats = {
            'projects_processed': 0,
            'files_found': 0,
            'files_downloaded': 0,
            'files_skipped': 0,
            'download_errors': 0,
            'total_bytes_downloaded': 0
        }
    
    def get_projects(self) -> List[Dict[str, Any]]:
        """
        Get all projects the user has access to
        
        Returns:
            List of project dictionaries
        """
        url = f"{self.base_url}/projects"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            print(f"✓ Successfully retrieved projects (Status: {response.status_code})")
            projects_data = response.json()
            
            # Handle different response structures
            if isinstance(projects_data, list):
                return projects_data
            elif isinstance(projects_data, dict) and 'data' in projects_data:
                return projects_data['data']
            else:
                return projects_data
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Error getting projects: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response: {e.response.text}")
            return []
    
    def get_project_files(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Get all files from a project's default branch
        
        Args:
            project_id: The project ID
            
        Returns:
            List of file dictionaries
        """
        url = f"{self.base_url}/projects/{project_id}/files"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            files_data = response.json()
            
            # Handle different response structures
            if isinstance(files_data, list):
                return files_data
            elif isinstance(files_data, dict) and 'data' in files_data:
                return files_data['data']
            else:
                return files_data
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Error getting files for project {project_id}: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response: {e.response.text}")
            return []
    
    def get_file_details(self, project_id: str, branch_id: str, file_id: str) -> Dict[str, Any]:
        """
        Get details of the latest released file including download URLs
        
        Args:
            project_id: The project ID
            branch_id: The branch ID
            file_id: The file ID
            
        Returns:
            File details dictionary with download URLs
        """
        url = f"{self.base_url}/projects/{project_id}/branches/{branch_id}/files/{file_id}/released"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            file_data = response.json()
            
            # Handle different response structures
            if isinstance(file_data, dict) and 'data' in file_data:
                return file_data['data']
            else:
                return file_data
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Error getting file details for file {file_id}: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response: {e.response.text}")
            return {}
    
    def sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename to be safe for filesystem
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        # Replace invalid characters with underscores
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove any trailing dots or spaces
        sanitized = sanitized.strip('. ')
        # Limit length to prevent filesystem issues
        if len(sanitized) > 255:
            name, ext = os.path.splitext(sanitized)
            sanitized = name[:255-len(ext)] + ext
        return sanitized if sanitized else 'unnamed_file'
    
    def create_project_directory(self, project_name: str) -> Path:
        """
        Create and return the project directory path
        
        Args:
            project_name: Name of the project
            
        Returns:
            Path object for the project directory
        """
        sanitized_name = self.sanitize_filename(project_name)
        project_dir = self.backup_location / sanitized_name
        project_dir.mkdir(parents=True, exist_ok=True)
        return project_dir
    
    def download_file(self, download_url: str, file_path: Path, file_name: str) -> bool:
        """
        Download a file from the given URL to the specified path
        
        Args:
            download_url: URL to download the file from
            file_path: Full path where the file should be saved
            file_name: Original file name for logging
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            # Create directory structure if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if file already exists and has content
            if file_path.exists() and file_path.stat().st_size > 0:
                print(f"      ✓ File already exists: {file_name}")
                self.stats['files_skipped'] += 1
                return True
            
            # Download the file
            print(f"      Downloading: {file_name}...")
            
            # Use a separate session for downloads to avoid auth headers if not needed
            download_response = requests.get(download_url, stream=True, timeout=300)
            download_response.raise_for_status()
            
            # Write file in chunks to handle large files
            total_size = 0
            with open(file_path, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
            
            self.stats['files_downloaded'] += 1
            self.stats['total_bytes_downloaded'] += total_size
            
            # Format file size for logging
            size_mb = total_size / (1024 * 1024)
            print(f"      ✓ Downloaded: {file_name} ({size_mb:.2f} MB)")
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"      ✗ Download failed for {file_name}: {e}")
            self.stats['download_errors'] += 1
            
            # Clean up partial download
            if file_path.exists():
                try:
                    file_path.unlink()
                except:
                    pass
            
            return False
        except Exception as e:
            print(f"      ✗ Unexpected error downloading {file_name}: {e}")
            self.stats['download_errors'] += 1
            return False
        """
        Flatten a nested file structure (if it's a tree) into a flat list
        
        Args:
            files_data: List of files (may be nested)
            
        Returns:
            Flat list of files
        """
        flat_files = []
        
        def process_item(item):
            if item.get('type') == 'file':
                flat_files.append(item)
            elif item.get('type') == 'folder' and 'children' in item:
                for child in item['children']:
                    process_item(child)
            # Handle other possible structures
            elif 'files' in item:
                for file_item in item['files']:
                    process_item(file_item)
        
        for item in files_data:
            process_item(item)
        
        return flat_files
    
    def backup_all_projects(self) -> Dict[str, Any]:
        """
        Download and backup all files from all accessible projects
        
        Returns:
            Dictionary containing backup summary and file details
        """
        backup_summary = {
            'backup_location': str(self.backup_location),
            'backup_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'projects': [],
            'statistics': {}
        }
        
        print("Starting complete Bild PDM backup...")
        print("=" * 60)
        print(f"Backup location: {self.backup_location}")
        
        # Step 1: Get all projects
        print("\nStep 1: Getting all projects...")
        projects = self.get_projects()
        
        if not projects:
            print("No projects found or unable to retrieve projects.")
            return backup_summary
        
        print(f"Found {len(projects)} projects to backup")
        
        # Step 2: Process each project
        for i, project in enumerate(projects, 1):
            project_id = project.get('id', '')
            project_name = project.get('name', 'Unknown Project')
            default_branch = project.get('defaultBranch', {})
            branch_id = default_branch.get('id', '') if isinstance(default_branch, dict) else ''
            
            print(f"\n{'='*20} PROJECT {i}/{len(projects)} {'='*20}")
            print(f"Processing: '{project_name}' (ID: {project_id})")
            
            project_info = {
                'id': project_id,
                'name': project_name,
                'branch_id': branch_id,
                'files': [],
                'status': 'skipped'
            }
            
            if not project_id:
                print("  ✗ Skipping project - no ID found")
                project_info['status'] = 'error - no ID'
                backup_summary['projects'].append(project_info)
                continue
            
            # Create project directory
            try:
                project_dir = self.create_project_directory(project_name)
                print(f"  📁 Project directory: {project_dir}")
            except Exception as e:
                print(f"  ✗ Error creating project directory: {e}")
                project_info['status'] = f'error - directory creation failed: {e}'
                backup_summary['projects'].append(project_info)
                continue
            
            # Get files for this project
            project_files = self.get_project_files(project_id)
            
            if not project_files:
                print("  ✗ No files found in this project")
                project_info['status'] = 'no files'
                backup_summary['projects'].append(project_info)
                continue
            
            print(f"  📂 Processing files and folders...")
            
            # Process files while preserving structure
            try:
                processed_files = self.process_files_structure(
                    project_files, project_dir, project_id, branch_id
                )
                
                project_info['files'] = processed_files
                project_info['status'] = 'completed'
                self.stats['projects_processed'] += 1
                
                files_downloaded = sum(1 for f in processed_files if f.get('download_successful', False))
                print(f"  ✓ Project completed: {files_downloaded}/{len(processed_files)} files downloaded")
                
            except Exception as e:
                print(f"  ✗ Error processing project files: {e}")
                project_info['status'] = f'error - {e}'
            
            backup_summary['projects'].append(project_info)
            
            # Small delay between projects
            time.sleep(0.5)
        
        # Update final statistics
        backup_summary['statistics'] = self.stats.copy()
        return backup_summary
    def display_backup_summary(self, backup_summary: Dict[str, Any]):
        """
        Display the backup results in a readable format
        
        Args:
            backup_summary: Backup summary dictionary
        """
        print("\n" + "=" * 80)
        print("BACKUP COMPLETE - SUMMARY")
        print("=" * 80)
        
        stats = backup_summary.get('statistics', {})
        projects = backup_summary.get('projects', [])
        
        print(f"Backup location: {backup_summary.get('backup_location', 'Unknown')}")
        print(f"Backup completed: {backup_summary.get('backup_timestamp', 'Unknown')}")
        print()
        
        # Overall statistics
        print("Overall Statistics:")
        print(f"  Projects processed: {stats.get('projects_processed', 0)}")
        print(f"  Files found: {stats.get('files_found', 0)}")
        print(f"  Files downloaded: {stats.get('files_downloaded', 0)}")
        print(f"  Files skipped (already exist): {stats.get('files_skipped', 0)}")
        print(f"  Download errors: {stats.get('download_errors', 0)}")
        
        total_mb = stats.get('total_bytes_downloaded', 0) / (1024 * 1024)
        print(f"  Total data downloaded: {total_mb:.2f} MB")
        
        # Project breakdown
        print("\nProject Breakdown:")
        for project in projects:
            project_name = project.get('name', 'Unknown')
            status = project.get('status', 'Unknown')
            files = project.get('files', [])
            downloaded = sum(1 for f in files if f.get('download_successful', False))
            total = len(files)
            
            print(f"  📁 {project_name}: {status}")
            if total > 0:
                print(f"    Files: {downloaded}/{total} downloaded")
        
        # Success rate
        if stats.get('files_found', 0) > 0:
            success_rate = (stats.get('files_downloaded', 0) / stats.get('files_found', 1)) * 100
            print(f"\nSuccess rate: {success_rate:.1f}%")
    
    def save_backup_log(self, backup_summary: Dict[str, Any], filename: str = None):
        """
        Save backup summary to a JSON log file
        
        Args:
            backup_summary: Backup summary dictionary
            filename: Output filename (optional)
        """
        if filename is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f"bild_backup_log_{timestamp}.json"
        
        log_path = self.backup_location / filename
        
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                json.dump(backup_summary, f, indent=2, ensure_ascii=False)
            print(f"\n✓ Backup log saved to: {log_path}")
        except Exception as e:
            print(f"\n✗ Error saving backup log: {e}")


def display_version_info():
    """
    Display script version and information
    """
    print("=" * 80)
    print("BILD PDM COMPLETE BACKUP TOOL")
    print("=" * 80)
    print(f"Version: {__version__}")
    print(f"Description: {__description__}")
    print(f"Author: {__author__}")
    print(f"Python Version: {sys.version.split()[0]}")
    print("=" * 80)


def load_api_key() -> str:
    """
    Load API key from file in the same directory as the script
    
    Returns:
        API key string
        
    Raises:
        FileNotFoundError: If the API key file doesn't exist
        ValueError: If the API key file is empty or invalid
    """
    script_dir = Path(__file__).parent
    api_key_file = script_dir / "bildAPI_backupKey.txt"
    
    try:
        with open(api_key_file, 'r', encoding='utf-8') as f:
            api_key = f.read().strip()
        
        if not api_key:
            raise ValueError("API key file is empty")
        
        print(f"✓ API key loaded from: {api_key_file}")
        return api_key
        
    except FileNotFoundError:
        print(f"✗ API key file not found: {api_key_file}")
        print("Please create a file named 'bildAPI_backupKey.txt' in the same directory as this script.")
        print("The file should contain only your Bild API key (Bearer token).")
        raise
    except Exception as e:
        print(f"✗ Error reading API key file: {e}")
        raise


def main():
    """
    Main function to run the Bild API backup
    """
    # Configuration
    BACKUP_LOCATION = "./bild_backup"  # Replace with your desired backup location
    
    # For production, use: "https://api.getbild.com"
    BASE_URL = "https://sandbox-api.getbild.com"
    
    print("=" * 80)
    print("BILD PDM COMPLETE BACKUP TOOL")
    print("=" * 80)
    
    # Load API key from file
    try:
        API_TOKEN = load_api_key()
    except (FileNotFoundError, ValueError) as e:
        print(f"Cannot proceed without API key: {e}")
        sys.exit(1)
    
    print(f"API Endpoint: {BASE_URL}")
    print(f"Backup Location: {os.path.abspath(BACKUP_LOCATION)}")
    print(f"Start Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Confirm backup location
    backup_path = Path(BACKUP_LOCATION).resolve()
    if backup_path.exists() and any(backup_path.iterdir()):
        print(f"\nWARNING: Backup directory already exists and contains files.")
        print(f"Location: {backup_path}")
        response = input("Continue with backup? (y/N): ").lower().strip()
        if response not in ['y', 'yes']:
            print("Backup cancelled.")
            return
    
    # Initialize the client
    client = BildAPIClient(API_TOKEN, BACKUP_LOCATION, BASE_URL)
    
    try:
        # Start the backup process
        backup_summary = client.backup_all_projects()
        
        # Display results
        client.display_backup_summary(backup_summary)
        
        # Save backup log
        client.save_backup_log(backup_summary)
        
        print(f"\n✓ Backup process completed!")
        print(f"✓ All files saved to: {client.backup_location}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Backup interrupted by user")
        print("Partial backup may be available in the backup directory.")
    except Exception as e:
        print(f"\n✗ Unexpected error during backup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()